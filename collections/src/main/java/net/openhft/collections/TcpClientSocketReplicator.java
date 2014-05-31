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
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static java.nio.channels.SelectionKey.*;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

/**
 * Used with a {@see net.openhft.collections.ReplicatedSharedHashMap} to send data between the maps using a
 * socket connection
 *
 * {@see net.openhft.collections.OutSocketReplicator}
 *
 * @author Rob Austin.
 */
class TcpClientSocketReplicator extends AbstractTCPReplicator implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(TcpClientSocketReplicator.class.getName());
    private static final int BUFFER_SIZE = 0x100000; // 1Mb

    private final AtomicReference<SocketChannel> socketChannelRef = new AtomicReference<SocketChannel>();
    private final ExecutorService executorService;
    final CopyOnWriteArraySet<SocketChannel> socketChannels = new CopyOnWriteArraySet<SocketChannel>();
    final Selector selector;

    TcpClientSocketReplicator(@NotNull final ReplicatedSharedHashMap map,
                              final short packetSize,
                              final int serializedEntrySize,
                              @NotNull final ReplicatedSharedHashMap.EntryExternalizable externalizable,
                              @NotNull final Set<InetSocketAddress> endpoint) throws IOException {

        executorService = newSingleThreadExecutor(
                new NamedThreadFactory("InSocketReplicator-" + map.identifier(), true));
        selector = Selector.open();

        executorService.execute(new Runnable() {
                                    @Override
                                    public void run() {
                                        try {
                                            process(map, packetSize,
                                                    serializedEntrySize, externalizable, endpoint);
                                        } catch (IOException e) {
                                            LOG.error("", e);
                                        }
                                    }
                                }
        );
    }

    /**
     * Registers the SocketChannel with the selector
     *
     * @param newSockets
     * @return
     * @throws ClosedChannelException
     */
    void register(ConcurrentLinkedQueue<SocketChannel> newSockets) throws ClosedChannelException {
        for (SocketChannel sc = newSockets.poll(); sc != null; sc = newSockets.poll()) {
            sc.register(selector, OP_CONNECT);
        }
    }


    private void process(ReplicatedSharedHashMap map,
                         final short packetSize, final int serializedEntrySize,
                         final ReplicatedSharedHashMap.EntryExternalizable externalizable,
                         final Set<InetSocketAddress> endpoints) throws IOException {

        final byte identifier = map.identifier();

        try {

            final ConcurrentLinkedQueue<SocketChannel> newSockets = asyncConnect(identifier, endpoints);


            for (; ; ) {

                register(newSockets);

                if (!selector.isOpen())
                    return;

                final int nSelectedKeys = selector.select(100);
                if (nSelectedKeys == 0) {
                    continue;    // nothing to do
                }

                final Set<SelectionKey> selectedKeys = selector.selectedKeys();

                for (final SelectionKey key : selectedKeys) {
                    try {
                        if (!key.isValid())
                            continue;

                        if (key.isConnectable()) {
                            onConnect(map, packetSize, serializedEntrySize, externalizable, key);
                        }

                        if (key.isReadable()) {
                            onRead(map, key);
                        }

                        if (key.isWritable()) {
                            onWrite(key);
                        }


                    } catch (Exception e) {

                        //  if (!isClosed.get()) {
                        //   if (socketChannel.isOpen())
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
            LOG.error("", e);
        } finally {
            if (selector != null)
                try {
                    selector.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            close();
        }
    }

    /**
     * @param identifier
     * @param endpoints
     * @return a queue containing the SocketChannel as they become connected
     */
    private ConcurrentLinkedQueue<SocketChannel> asyncConnect(final byte identifier, final Set<InetSocketAddress> endpoints) {

        final ConcurrentLinkedQueue<SocketChannel> result = new ConcurrentLinkedQueue<SocketChannel>();

        for (final InetSocketAddress endpoint : endpoints) {

            final Runnable target = new Runnable() {

                public void run() {
                    try {
                        SocketChannel socketChannel = blockingConnect(endpoint, identifier);
                        socketChannels.add(socketChannel);
                        result.add(socketChannel);
                    } catch (InterruptedException e) {
                        // do nothing
                    } catch (IOException e) {
                        LOG.error("", e);
                    }
                }

                /**
                 * blocks until connected
                 *
                 * @param endpoint   the endpoint to connect
                 * @param identifier used for logging only
                 * @throws IOException if we are not successful at connection
                 */
                private SocketChannel blockingConnect(final InetSocketAddress endpoint,
                                                      final byte identifier) throws InterruptedException, IOException {

                    boolean success = false;

                    for (; ; ) {

                        final SocketChannel socketChannel = SocketChannel.open();

                        try {

                            socketChannel.configureBlocking(false);
                            socketChannel.socket().setKeepAlive(true);
                            socketChannel.socket().setReuseAddress(false);
                            socketChannel.socket().setSoLinger(false, 0);
                            socketChannel.socket().setSoTimeout(0);
                            socketChannel.socket().setTcpNoDelay(true);

                            // todo not sure why we have to have this here,
                            // but if we don't add it'll fail, no sure why ?
                            Thread.sleep(500);

                            socketChannel.connect(endpoint);
                            socketChannelRef.set(socketChannel);

                            if (LOG.isDebugEnabled())
                                LOG.debug("successfully connected to {}, local-id={}", endpoint, identifier);
                            success = true;
                            return socketChannel;

                        } catch (IOException e) {
                            throw e;
                        } catch (InterruptedException e) {
                            throw e;
                        } finally {
                            if (!success)
                                try {
                                    socketChannel.close();
                                } catch (IOException e) {
                                    LOG.error("", e);
                                }
                        }

                    }

                }

            };

            final Thread thread = new Thread(target);
            thread.setName("connector-" + endpoint);
            thread.setDaemon(true);
            thread.start();
        }
        return result;
    }

    private void onConnect(ReplicatedSharedHashMap map,
                           short packetSize,
                           int serializedEntrySize,
                           ReplicatedSharedHashMap.EntryExternalizable externalizable,
                           SelectionKey key) throws IOException, InterruptedException {
        final SocketChannel channel = (SocketChannel) key.channel();

        while (!channel.finishConnect()) {
            Thread.sleep(100);
        }

        channel.configureBlocking(false);
        channel.socket().setKeepAlive(true);
        channel.socket().setSoTimeout(100);
        channel.socket().setSoLinger(false, 0);

        final Attached attached = new Attached();

        attached.entryReader = new TcpSocketChannelEntryReader(serializedEntrySize,
                externalizable, packetSize);

        attached.entryWriter = new TcpSocketChannelEntryWriter(serializedEntrySize,
                externalizable, packetSize);

        channel.register(selector, OP_WRITE | OP_READ, attached);

        // register it with the selector and store the ModificationIterator for this key
        final byte identifier1 = map.identifier();
        attached.entryWriter.identifierToBuffer(identifier1);
    }


    @Override
    public void close() throws IOException {

        for (SocketChannel socketChannel : this.socketChannels)
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

        socketChannels.clear();
        selector.close();
        executorService.shutdownNow();
    }


}

