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
import java.net.ServerSocket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import static java.nio.channels.SelectionKey.*;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static net.openhft.collections.ReplicatedSharedHashMap.EntryExternalizable;
import static net.openhft.collections.ReplicatedSharedHashMap.ModificationIterator;

/**
 * Used with a {@link ReplicatedSharedHashMap} to send data between the
 * maps using nio, non blocking, server socket connection.
 *
 * @see TcpClientSocketReplicator
 *
 * @author Rob Austin.
 */
class TcpServerSocketReplicator implements Closeable {

    private static final Logger LOG =
            LoggerFactory.getLogger(TcpServerSocketReplicator.class.getName());

    private final ReplicatedSharedHashMap map;
    private final InetSocketAddress address;
    private final ServerSocketChannel serverChannel;
    private final TcpSocketChannelEntryWriter entryWriter;
    private final byte localIdentifier;
    private final EntryExternalizable externalizable;
    private final int serializedEntrySize;

    private final short packetSize;
    private final ExecutorService executorService;
    private CountDownLatch latch = new CountDownLatch(1);
    private final Selector selector;

    public TcpServerSocketReplicator(@NotNull final ReplicatedSharedHashMap map,
                                     @NotNull final EntryExternalizable externalizable,
                                     final int port,
                                     @NotNull final TcpSocketChannelEntryWriter entryWriter,
                                     final short packetSize,
                                     final int serializedEntrySize) throws IOException {

        this.externalizable = externalizable;
        this.map = map;
        address = new InetSocketAddress(port);
        this.serverChannel = ServerSocketChannel.open();
        selector = Selector.open();
        this.localIdentifier = map.identifier();
        this.entryWriter = entryWriter;
        this.serializedEntrySize = serializedEntrySize;
        this.packetSize = packetSize;

        // out bound
        executorService = newSingleThreadExecutor(
                new NamedThreadFactory("TcpServerSocketReplicator-" + localIdentifier, true));

        executorService.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    process();
                } catch (Exception e) {
                    LOG.error("", e);
                }
            }

        });
    }

    @Override
    public void close() throws IOException {
        serverChannel.close();
        try {
            latch.await();
        } catch (InterruptedException e) {
            LOG.error("", e);
        }
        executorService.shutdownNow();
    }

    /**
     * binds to the server socket and process data This method will block until interrupted
     */
    private void process() throws IOException, InterruptedException {

        LOG.debug("Listening on port {}", address.getPort());

        final ServerSocket serverSocket = serverChannel.socket();
        try {
            serverSocket.setReuseAddress(true);
            serverSocket.bind(address);

            serverChannel.configureBlocking(false);
            serverChannel.register(selector, OP_ACCEPT);

            while (serverChannel.isOpen()) {
                final int nSelectedKeys = selector.select(100);
                if (nSelectedKeys == 0) {
                    continue;    // nothing to do
                }

                Set<SelectionKey> selectedKeys = selector.selectedKeys();
                for (SelectionKey key : selectedKeys) {
                    if (key.isAcceptable()) {

                        final ServerSocketChannel server = (ServerSocketChannel) key.channel();
                        final SocketChannel channel = server.accept();
                        channel.configureBlocking(false);
                        channel.socket().setKeepAlive(true);
                        channel.socket().setSoTimeout(100);

                        final TcpSocketChannelEntryReader entryReader =
                                new TcpSocketChannelEntryReader(serializedEntrySize, externalizable, packetSize);

                        final byte remoteIdentifier = entryReader.readIdentifier(channel);
                        entryWriter.sendIdentifier(channel, localIdentifier);

                        final long remoteTimestamp = entryReader.readTimeStamp(channel);

                        final ModificationIterator remoteModificationIterator =
                                map.acquireModificationIterator(remoteIdentifier);
                        remoteModificationIterator.dirtyEntries(remoteTimestamp);

                        // register it with the selector and store the ModificationIterator for this key
                        final Attached attached = new Attached(entryReader, remoteModificationIterator);
                        channel.register(selector, OP_WRITE | OP_READ, attached);

                        if (remoteIdentifier == map.identifier())
                            throw new IllegalStateException("Non unique identifiers id=" + map.identifier());

                        entryWriter.sendTimestamp(channel, map.lastModificationTime(remoteIdentifier));

                        if (LOG.isDebugEnabled()) {
                            // Pre-check prevents autoboxing of identifiers, i. e. garbage creation.
                            // Subtle gain, but.
                            LOG.debug("server-connection id={}, remoteIdentifier={}",
                                    map.identifier(), remoteIdentifier);
                        }

                        // process any writer.remaining(), this can occur because reading socket for the bootstrap,
                        // may read more than just 9 writer
                        entryReader.readAll(channel);
                    }
                    try {

                        if (key.isWritable()) {
                            final SocketChannel socketChannel = (SocketChannel) key.channel();
                            final Attached attachment = (Attached) key.attachment();
                            entryWriter.writeAll(socketChannel, attachment.remoteModificationIterator);
                        }

                        if (key.isReadable()) {
                            final SocketChannel socketChannel = (SocketChannel) key.channel();
                            final Attached attachment = (Attached) key.attachment();
                            attachment.entryReader.readAll(socketChannel);
                        }

                    } catch (Exception e) {

                        if (serverChannel.isOpen())
                            LOG.error("", e);

                        // Close channel and nudge selector
                        try {
                            key.channel().close();
                        } catch (IOException ex) {
                            // do nothing
                        }
                        return;
                    }
                }
                selectedKeys.clear();
            }
        } finally {

            try {
                serverChannel.socket().close();
            } finally {
                serverChannel.close();
            }
            selector.close();
            latch.countDown();
        }
    }

    private static class Attached {

        final TcpSocketChannelEntryReader entryReader;
        final ModificationIterator remoteModificationIterator;

        private Attached(TcpSocketChannelEntryReader entryReader,
                         ModificationIterator remoteModificationIterator) {
            this.entryReader = entryReader;
            this.remoteModificationIterator = remoteModificationIterator;
        }
    }

}


