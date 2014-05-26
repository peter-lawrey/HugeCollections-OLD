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
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static java.nio.channels.SelectionKey.*;
import static java.nio.channels.SelectionKey.OP_READ;
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
    private static final int BUFFER_SIZE = 0x100000; // 1Mb

    private final AtomicReference<SocketChannel> socketChannelRef = new AtomicReference<SocketChannel>();
    private final ExecutorService executorService;

    TcpClientSocketReplicator(@NotNull final InetSocketAddress endpoint,
                              @NotNull final TcpSocketChannelEntryReader entryReader,
                              @NotNull final TcpSocketChannelEntryWriter entryWriter,
                              @NotNull final ReplicatedSharedHashMap map) {

        executorService = newSingleThreadExecutor(
                new NamedThreadFactory("InSocketReplicator-" + map.identifier(), true));
        executorService.execute(new Runnable() {

            @Override
            public void run() {
                try {

                    SocketChannel socketChannel = null;

                    for (; ; ) {
                        try {
                            socketChannel = SocketChannel.open(endpoint);
                            if (LOG.isDebugEnabled()) {
                                LOG.info("successfully connected to {}, local-id={}",
                                        endpoint, map.identifier());
                            }
                            break;
                        } catch (ConnectException e) {
                            Thread.sleep(100);
                        }
                    }

                    socketChannelRef.set(socketChannel);

                    socketChannel.socket().setReceiveBufferSize(BUFFER_SIZE);
                    socketChannel.socket().setSendBufferSize(BUFFER_SIZE);

                    entryWriter.sendIdentifier(socketChannel, map.identifier());
                    final byte remoteIdentifier = entryReader.readIdentifier(socketChannel);

                    entryWriter.sendTimestamp(socketChannel, map.lastModificationTime(remoteIdentifier));
                    final long remoteTimestamp = entryReader.readTimeStamp(socketChannel);

                    final ReplicatedSharedHashMap.ModificationIterator remoteModificationIterator =
                            map.acquireModificationIterator(remoteIdentifier);
                    remoteModificationIterator.dirtyEntries(remoteTimestamp);

                    if (remoteIdentifier == map.identifier())
                        throw new IllegalStateException("Non unique identifiers id=" + map.identifier());

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("client-connection id={}, remoteIdentifier={}",
                                map.identifier(), remoteIdentifier);
                    }

                    // we start this connection in blocking mode ( to do the bootstrapping ),
                    // then move it to non-blocking
                    socketChannel.configureBlocking(false);
                    final Selector selector = Selector.open();
                    socketChannel.register(selector, OP_READ | OP_WRITE);

                    // process any writer.remaining(), this can occur because reading socket for the bootstrap,
                    // may read more than just 9 writer
                    entryReader.readAll(socketChannel);

                    while (socketChannel.isOpen()) {
                        final int nSelectedKeys = selector.select();
                        if (nSelectedKeys == 0) {
                            continue;    // nothing to do
                        }

                        Set<SelectionKey> selectedKeys = selector.selectedKeys();
                        for (SelectionKey key : selectedKeys) {
                            try {
                                if (!key.isValid())
                                    continue;

                                // is there data to read on this channel?
                                if (key.isReadable())
                                    entryReader.readAll(socketChannel);

                                if (key.isWritable())
                                    entryWriter.writeAll(socketChannel, remoteModificationIterator);

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
                        selectedKeys.clear();
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
            try {
                socketChannel.socket().close();
            } finally {
                socketChannel.close();
            }
        }
        executorService.shutdownNow();
    }


}

