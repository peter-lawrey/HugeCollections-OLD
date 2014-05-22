/*
 * Copyright 2014 Higher Frequency Trading
 * <p/>
 * http://www.higherfrequencytrading.com
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.collections.map.replicators;

import net.openhft.collections.ReplicatedSharedHashMap;
import net.openhft.lang.thread.NamedThreadFactory;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.Executors.newSingleThreadExecutor;

/**
 * Used with a {@see net.openhft.collections.ReplicatedSharedHashMap} to send data between the maps using a socket connection
 * <p/>
 * {@see net.openhft.collections.OutSocketReplicator}
 *
 * @author Rob Austin.
 */
public class ClientTcpSocketReplicator implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(ClientTcpSocketReplicator.class.getName());
    private final AtomicReference<SocketChannel> socketChannelRef = new AtomicReference<SocketChannel>();
    private final ExecutorService executorService;

    public ClientTcpSocketReplicator(@NotNull final ClientPort clientPort,
                                     @NotNull final SocketChannelEntryReader socketChannelEntryReader,
                                     @NotNull final SocketChannelEntryWriter socketChannelEntryWriter,
                                     @NotNull final ReplicatedSharedHashMap map) {

        executorService = newSingleThreadExecutor(new NamedThreadFactory("InSocketReplicator-" + map.getIdentifier(), true));
        executorService.execute(new Runnable() {

            @Override
            public void run() {
                try {

                    SocketChannel socketChannel = null;

                    for (; ; ) {
                        try {
                            socketChannel = SocketChannel.open(new InetSocketAddress(clientPort.host, clientPort.port));
                            if (LOG.isDebugEnabled()) {
                                LOG.info("successfully connected to " + clientPort + ", local-id=" + map.getIdentifier());
                            }
                            break;
                        } catch (ConnectException e) {
                            if (socketChannel != null)
                                socketChannel.close();
                            // todo add better back-off logic
                            Thread.sleep(100);
                        }
                    }

                    socketChannelRef.set(socketChannel);
                    socketChannel.socket().setReceiveBufferSize(8 * 1024);


                    socketChannelEntryWriter.sendIdentifier(socketChannel, map.getIdentifier());
                    final byte remoteIdentifier = socketChannelEntryReader.readIdentifier(socketChannel);

                    socketChannelEntryWriter.sendTimestamp(socketChannel, map.getLastModificationTime(remoteIdentifier));
                    final long remoteTimestamp = socketChannelEntryReader.readTimeStamp(socketChannel);

                    final ReplicatedSharedHashMap.ModificationIterator remoteModificationIterator = map.acquireModificationIterator(remoteIdentifier);
                    remoteModificationIterator.dirtyEntries(remoteTimestamp);

                    if (remoteIdentifier == map.getIdentifier())
                        throw new IllegalStateException("Non unique identifiers id=" + map.getIdentifier());

                    if (LOG.isDebugEnabled())
                        LOG.debug("client-connection id=" + map.getIdentifier() + ", remoteIdentifier=" + remoteIdentifier);

                    // we start this connection in blocking mode ( to do the bootstrapping ) , then move it to non-blocking
                    socketChannel.configureBlocking(false);
                    final Selector selector = Selector.open();
                    socketChannel.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE);

                    // process any writer.remaining(), this can occur because reading socket for the bootstrap,
                    // may read more than just 9 writer
                    socketChannelEntryReader.readAll(socketChannel);

                    while (socketChannel.isOpen()) {
                        // this may block for a long time, upon return the
                        // selected set contains keys of the ready channels
                        final int n = selector.select();

                        if (n == 0) {
                            continue;    // nothing to do
                        }

                        // get an iterator over the set of selected keys
                        final Iterator it = selector.selectedKeys().iterator();

                        // look at each key in the selected set
                        while (it.hasNext()) {

                            SelectionKey key = (SelectionKey) it.next();

                            // remove key from selected set, it's been handled
                            it.remove();

                            try {

                                if (!key.isValid())
                                    continue;

                                // is there data to read on this channel?
                                if (key.isReadable())
                                    socketChannelEntryReader.readAll(socketChannel);

                                if (key.isWritable())
                                    socketChannelEntryWriter.writeAll(socketChannel, remoteModificationIterator);

                            } catch (Exception e) {

                                //  if (!isClosed.get()) {
                                if (socketChannel.isOpen())
                                    LOG.info("", e);
                                // Close channel and nudge selector
                                try {
                                    key.channel().close();
                                } catch (IOException ex) {
                                    // do nothing
                                }
                            }
                        }
                    }


                } catch (Exception e) {
                    // we wont log exceptions that occur due ot the socket being closed
                    final SocketChannel socketChannel = socketChannelRef.get();
                    if (socketChannel != null && socketChannel.isOpen()) {
                        LOG.error("", e);
                    }
                }
            }

        });
    }

    @Override
    public void close() throws IOException {
        executorService.shutdownNow();
        final SocketChannel socketChannel = socketChannelRef.get();
        if (socketChannel != null)
            socketChannel.close();
    }

    public static class ClientPort {
        final String host;
        final int port;

        public ClientPort(int port, String host) {
            this.host = host;
            this.port = port;
        }

        @Override
        public String toString() {
            return "host=" + host +
                    ", port=" + port;

        }
    }

}

