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

package net.openhft.collections;

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
class TcpClientSocketReplicator implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(TcpClientSocketReplicator.class.getName());
    private final AtomicReference<SocketChannel> socketChannelRef = new AtomicReference<SocketChannel>();
    private final ExecutorService executorService;

    TcpClientSocketReplicator(@NotNull final InetSocketAddress inetSocketAddress,
                              @NotNull final TcpSocketChannelEntryReader tcpSocketChannelEntryReader,
                              @NotNull final TcpSocketChannelEntryWriter tcpSocketChannelEntryWriter,
                              @NotNull final ReplicatedSharedHashMap map) {

        executorService = newSingleThreadExecutor(new NamedThreadFactory("InSocketReplicator-" + map.identifier(), true));
        executorService.execute(new Runnable() {

            @Override
            public void run() {
                try {

                    SocketChannel socketChannel = null;

                    for (; ; ) {
                        try {
                            socketChannel = SocketChannel.open(inetSocketAddress);
                            if (LOG.isDebugEnabled()) {
                                LOG.info("successfully connected to " + inetSocketAddress + ", local-id=" + map.identifier());
                            }
                            break;
                        } catch (ConnectException e) {
                            Thread.sleep(100);
                        }
                    }

                    socketChannelRef.set(socketChannel);

                    socketChannel.socket().setReceiveBufferSize(0x100000); // 1Mb
                    socketChannel.socket().setSendBufferSize(0x100000); // 1Mb

                    tcpSocketChannelEntryWriter.sendIdentifier(socketChannel, map.identifier());
                    final byte remoteIdentifier = tcpSocketChannelEntryReader.readIdentifier(socketChannel);

                    tcpSocketChannelEntryWriter.sendTimestamp(socketChannel, map.lastModificationTime(remoteIdentifier));
                    final long remoteTimestamp = tcpSocketChannelEntryReader.readTimeStamp(socketChannel);

                    final ReplicatedSharedHashMap.ModificationIterator remoteModificationIterator = map.acquireModificationIterator(remoteIdentifier);
                    remoteModificationIterator.dirtyEntries(remoteTimestamp);

                    if (remoteIdentifier == map.identifier())
                        throw new IllegalStateException("Non unique identifiers id=" + map.identifier());

                    if (LOG.isDebugEnabled())
                        LOG.debug("client-connection id=" + map.identifier() + ", remoteIdentifier=" + remoteIdentifier);

                    // we start this connection in blocking mode ( to do the bootstrapping ) , then move it to non-blocking
                    socketChannel.configureBlocking(false);
                    final Selector selector = Selector.open();
                    socketChannel.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE);

                    // process any writer.remaining(), this can occur because reading socket for the bootstrap,
                    // may read more than just 9 writer
                    tcpSocketChannelEntryReader.readAll(socketChannel);

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
                                    tcpSocketChannelEntryReader.readAll(socketChannel);

                                if (key.isWritable())
                                    tcpSocketChannelEntryWriter.writeAll(socketChannel, remoteModificationIterator);

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

        final SocketChannel socketChannel = socketChannelRef.getAndSet(null);
        if (socketChannel != null) {
            socketChannel.socket().close();
            socketChannel.close();
        }
        executorService.shutdownNow();
    }


}

