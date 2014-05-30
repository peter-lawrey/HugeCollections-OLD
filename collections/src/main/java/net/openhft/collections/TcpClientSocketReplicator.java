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
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static java.nio.channels.SelectionKey.*;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static net.openhft.collections.TcpServerSocketReplicator.Attached;

/**
 * Used with a {@see net.openhft.collections.ReplicatedSharedHashMap} to send data between the maps using a
 * socket connection
 *
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
                                        process(endpoint, map, entryWriter, entryReader);
                                    }
                                }

        );
    }

    private void process(InetSocketAddress endpoint,
                         ReplicatedSharedHashMap map,
                         TcpSocketChannelEntryWriter entryWriter,
                         TcpSocketChannelEntryReader entryReader) {
        Selector selector = null;
        try {
            selector = Selector.open();

            connect(endpoint, map, selector);

            for (; ; ) {
                final int nSelectedKeys = selector.select(100);
                if (nSelectedKeys == 0) {
                    continue;    // nothing to do
                }

                Set<SelectionKey> selectedKeys = selector.selectedKeys();

                for (SelectionKey key : selectedKeys) {
                    try {
                        if (!key.isValid())
                            continue;


                        if (key.isConnectable()) {

                            final SocketChannel socketChannel = (SocketChannel) key.channel();

                            while (!socketChannel.finishConnect()) {
                                Thread.sleep(100);
                            }
                            // see http://www.exampledepot.8waytrips.com/egs/java.nio/NbClient.html
                            socketChannel.register(selector, OP_READ | OP_WRITE);

                            if (LOG.isDebugEnabled()) {
                                LOG.info("successfully connected to  local-id=" + map
                                        .identifier());
                            }

                            entryWriter.sendIdentifier(socketChannel, map.identifier());

                            final byte remoteIdentifier = entryReader.readIdentifier(socketChannel);

                            entryWriter.sendTimestamp(socketChannel,
                                    map.lastModificationTime(remoteIdentifier));
                            final long remoteTimestamp = entryReader.readTimeStamp(socketChannel);

                            ReplicatedSharedHashMap.ModificationIterator remoteModificationIterator = map.acquireModificationIterator(remoteIdentifier);
                            remoteModificationIterator.dirtyEntries(remoteTimestamp);

                            if (remoteIdentifier == map.identifier())
                                throw new IllegalStateException("Non unique identifiers id=" + map.identifier());

                            if (LOG.isDebugEnabled())
                                LOG.debug("client-connection id=" + map.identifier() + ", remoteIdentifier=" + remoteIdentifier);

                            final Attached attached = new Attached();
                            attached.entryReader = entryReader;
                            attached.entryWriter = entryWriter;
                            attached.remoteModificationIterator =
                                    remoteModificationIterator;

                            socketChannel.register(selector,
                                    OP_READ | OP_WRITE, attached);

                            // process any writer.remaining(), this can occur because reading socket for the bootstrap,
                            // may read more than just 9 writer
                            entryReader.readAll(socketChannel);
                        }

                        // is there data to read on this channel?
                        if (key.isReadable()) {

                            final SocketChannel socketChannel0 = (SocketChannel) key.channel();
                            final Attached a = (Attached) key.attachment();
                            a.entryReader.readAll(socketChannel0);
                        }
                        if (key.isWritable()) {
                            final SocketChannel socketChannel0 = (SocketChannel) key.channel();
                            final Attached a = (Attached) key.attachment();
                            a.entryWriter.writeAll(socketChannel0, a.remoteModificationIterator);
                        }
                    } catch (Exception e) {

                        //  if (!isClosed.get()) {
                        //   if (socketChannel.isOpen())
                        //     LOG.info("", e);
                        // Close channel and nudge selector
                        try {
                            key.channel().close();
                        } catch (IOException ex) {
                            // do nothing

                        }
                    }
                    selectedKeys.clear();
                }
            }
        } catch (Exception e) {
            LOG.error("", e);
        } finally {
            if (selector != null)
                try {
                    selector.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            close(socketChannelRef.getAndSet(null));
        }
    }

    private void connect(InetSocketAddress endpoint,
                         ReplicatedSharedHashMap map,
                         Selector selector) throws Exception {
        SocketChannel socketChannel;

        for (; ; ) {

            socketChannel = SocketChannel.open();
            socketChannel.configureBlocking(false);
            socketChannel.socket().setKeepAlive(true);
            socketChannel.socket().setReuseAddress(false);
            socketChannel.socket().setSoLinger(false, 0);
            socketChannel.socket().setSoTimeout(0);
            socketChannel.socket().setTcpNoDelay(true);

            try {

                // todo not sure why we have to have this here,
                // but if we don't add it'll fail !
                Thread.sleep(100);

                socketChannel.connect(endpoint);
                socketChannelRef.set(socketChannel);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("successfully connected to {}, local-id={}",
                            endpoint, map.identifier());
                }

                break;
            } catch (InterruptedException e) {
                throw e;
            } catch (Exception e) {
                socketChannel.close();
                LOG.error("", e);
                throw e;
            }

        }

        socketChannel.register(selector, OP_CONNECT);
    }

    @Override
    public void close() throws IOException {
        close(socketChannelRef.getAndSet(null));

        executorService.shutdownNow();
    }

    private static void close(SocketChannel socketChannel) {
        if (socketChannel != null) {
            try {
                try {
                    socketChannel.socket().close();
                } catch (IOException e) {
                    LOG.error("", e);
                }
            } finally {
                try {
                    socketChannel.close();
                } catch (IOException e) {
                    LOG.error("", e);
                }
            }
        }

    }


}

