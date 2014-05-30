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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArraySet;
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
    final CopyOnWriteArraySet<SocketChannel> socketChannels = new CopyOnWriteArraySet<SocketChannel>();
    final Selector selector;

    TcpClientSocketReplicator(@NotNull final InetSocketAddress endpoint,
                              @NotNull final ReplicatedSharedHashMap map,
                              final short packetSize,
                              final int serializedEntrySize,
                              final ReplicatedSharedHashMap.EntryExternalizable externalizable) throws IOException {

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

    private void process(ReplicatedSharedHashMap map,
                         final short packetSize, final int serializedEntrySize,
                         final ReplicatedSharedHashMap.EntryExternalizable externalizable,
                         final InetSocketAddress... endpoints) throws IOException {


        final ConcurrentLinkedQueue<SocketChannel> newSockets = new ConcurrentLinkedQueue<SocketChannel>();

        final byte identifier = map.identifier();

        try {

            for (final InetSocketAddress endpoint : endpoints) {

                final Runnable target = new Runnable() {

                    public void run() {
                        try {
                            SocketChannel socketChannel = blockingConnect(endpoint, identifier);
                            socketChannels.add(socketChannel);
                            newSockets.add(socketChannel);
                        } catch (InterruptedException e) {
                            // do nothing
                        } catch (IOException e) {
                            LOG.error("", e);
                        }
                    }

                };

                final Thread thread = new Thread(target);
                thread.setName("connector-" + endpoint);
                thread.setDaemon(true);
                thread.start();
            }

            boolean wasRegistered = false;

            for (; ; ) {


                for (; ; ) {
                    final SocketChannel socketChannel = newSockets.poll();

                    if (socketChannel == null)
                        break;
                    synchronized (selector) {
                        if (selector.isOpen()) {
                            socketChannel.register(selector, OP_CONNECT);
                            wasRegistered = true;
                        } else
                            return;
                    }
                }


                if (!wasRegistered) {
                    Thread.sleep(100);
                    continue;
                }

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

                            final SocketChannel channel = (SocketChannel) key.channel();

                            while (!channel.finishConnect()) {
                                Thread.sleep(100);
                            }


                            channel.configureBlocking(false);
                            channel.socket().setKeepAlive(true);
                            channel.socket().setSoTimeout(100);
                            channel.socket().setSoLinger(false, 0);
                            final Attached attached = new Attached();
                            channel.register(selector, OP_WRITE | OP_READ, attached);

                            attached.entryReader = new TcpSocketChannelEntryReader(serializedEntrySize,
                                    externalizable, packetSize);

                            attached.entryWriter = new TcpSocketChannelEntryWriter(serializedEntrySize,
                                    externalizable, packetSize);


                            // register it with the selector and store the ModificationIterator for this key
                            final byte identifier1 = map.identifier();
                            LOG.info("out=" + identifier1);
                            attached.entryWriter.writeIdentifier(identifier1);


                        }


                        if (key.isReadable()) {

                            final SocketChannel socketChannel = (SocketChannel) key.channel();
                            final Attached a = (Attached) key.attachment();

                            a.entryReader.compact();
                            int len = a.entryReader.read(socketChannel);

                            if (len > 0) {

                                if (!a.handShakingComplete) {
                                    if (a.remoteIdentifier == Byte.MIN_VALUE) {
                                        if (len == 8)
                                            len = 8;
                                        byte remoteIdentifier = a.entryReader.readIdentifier();

                                        if (remoteIdentifier != Byte.MIN_VALUE) {
                                            a.remoteIdentifier = remoteIdentifier;
                                            sendTimeStamp(map, a.entryWriter, a, remoteIdentifier);
                                        }
                                    }


                                    if (a.remoteIdentifier != Byte.MIN_VALUE && a.remoteTimestamp == Long
                                            .MIN_VALUE) {
                                        a.remoteTimestamp = a.entryReader.readTimeStamp();
                                        if (a.remoteTimestamp != Long.MIN_VALUE) {
                                            a.remoteModificationIterator.dirtyEntries(a.remoteTimestamp);
                                            a.handShakingComplete = true;
                                        }
                                    }
                                }


                                if (a.handShakingComplete)
                                    a.entryReader.readAll(socketChannel);
                            }

                        }

                        if (key.isWritable()) {
                            final SocketChannel socketChannel = (SocketChannel) key.channel();
                            final Attached a = (Attached) key.attachment();

                            if (a.remoteModificationIterator != null)
                                a.entryWriter.writeAll(socketChannel, a.remoteModificationIterator);

                            a.entryWriter.sendAll(socketChannel);
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

    private void sendTimeStamp(ReplicatedSharedHashMap map, TcpSocketChannelEntryWriter entryWriter, Attached a, byte remoteIdentifier) throws IOException {
        LOG.info("remoteIdentifier=" + remoteIdentifier);
        if (LOG.isDebugEnabled()) {
            // Pre-check prevents autoboxing of identifiers, i. e. garbage creation.
            // Subtle gain, but.
            LOG.debug("server-connection id={}, remoteIdentifier={}",
                    map.identifier(), remoteIdentifier);
        }

        if (remoteIdentifier == map.identifier())
            throw new IllegalStateException("Non unique identifiers id=" + map.identifier());
        a.remoteModificationIterator =
                map.acquireModificationIterator(remoteIdentifier);

        entryWriter.writeTimestamp(map.lastModificationTime(remoteIdentifier));
    }

    /**
     * blocks until connected
     *
     * @param endpoint   the endpoint to connect
     * @param identifier used for logging only
     * @throws Exception if we are not successful at connection
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
                // but if we don't add it'll fail !
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
                Thread.sleep(200);

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

        synchronized (selector) {
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
        }

        executorService.shutdownNow();
    }


}

