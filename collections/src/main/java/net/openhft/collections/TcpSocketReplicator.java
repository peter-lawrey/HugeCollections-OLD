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
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.nio.channels.*;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;

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
class TcpSocketReplicator extends AbstractTCPReplicator implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(TcpSocketReplicator.class.getName());
    private static final int BUFFER_SIZE = 0x100000; // 1Mb

    private final ExecutorService executorService;
    private final CopyOnWriteArraySet<Closeable> closeables = new CopyOnWriteArraySet<Closeable>();
    private final Selector selector;

    TcpSocketReplicator(@NotNull final ReplicatedSharedHashMap map,
                        final short packetSize,
                        final int serializedEntrySize,
                        @NotNull final ReplicatedSharedHashMap.EntryExternalizable externalizable,
                        @NotNull final Set<? extends SocketAddress> endpoint,
                        final int port) throws IOException {

        executorService = newSingleThreadExecutor(
                new NamedThreadFactory("InSocketReplicator-" + map.identifier(), true));
        selector = Selector.open();

        executorService.execute(new Runnable() {
                                    @Override
                                    public void run() {
                                        try {
                                            process(map, packetSize, serializedEntrySize,
                                                    externalizable, endpoint, port);
                                        } catch (IOException e) {
                                            if (selector.isOpen())
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
    void register(@NotNull final Queue<SelectableChannel> newSockets) throws ClosedChannelException {
        for (SelectableChannel sc = newSockets.poll(); sc != null; sc = newSockets.poll()) {
            if (sc instanceof ServerSocketChannel)
                sc.register(selector, OP_ACCEPT);
            else
                sc.register(selector, OP_CONNECT);
        }
    }


    private void process(@NotNull final ReplicatedSharedHashMap map,
                         final short packetSize, final int serializedEntrySize,
                         @NotNull final ReplicatedSharedHashMap.EntryExternalizable externalizable,
                         @NotNull final Set<? extends SocketAddress> endpoints, final int port) throws IOException {

        final InetSocketAddress serverEndpoint = port == -1 ? null : new InetSocketAddress(port);
        final byte identifier = map.identifier();

        try {

            final Queue<SelectableChannel> newSockets = asyncConnect(identifier, serverEndpoint, endpoints);

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


                        if (key.isAcceptable()) {
                            doAccept(key, serializedEntrySize, externalizable, packetSize, map.identifier());
                        }

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

        } catch (ClosedSelectorException e) {
            if (LOG.isDebugEnabled())
                LOG.debug("", e);
        } catch (Exception e) {
            LOG.error("", e);
        } finally {
            if (selector != null)
                try {
                    selector.close();
                } catch (IOException e) {
                    if (LOG.isDebugEnabled())
                        LOG.debug("", e);
                }

            close();
        }
    }


    /**
     * used to connect both client and server sockets
     *
     * @param identifier
     * @param clientEndpoints a queue containing the SocketChannel as they become connected
     * @return
     */
    private ConcurrentLinkedQueue<SelectableChannel> asyncConnect(
            final byte identifier,
            final @Nullable SocketAddress serverEndpoint,
            final @NotNull Set<? extends SocketAddress> clientEndpoints) {

        final ConcurrentLinkedQueue<SelectableChannel> result = new ConcurrentLinkedQueue<SelectableChannel>();

        abstract class AbstractConnector implements Runnable {

            private final SocketAddress address;

            abstract SelectableChannel connect(final SocketAddress address, final byte identifier)
                    throws IOException, InterruptedException;

            AbstractConnector(@NotNull final SocketAddress address) {
                this.address = address;
            }

            public void run() {
                try {

                    final SelectableChannel socketChannel = connect(this.address, identifier);
                    closeables.add(socketChannel);
                    result.add(socketChannel);
                } catch (InterruptedException e) {
                    // do nothing
                } catch (IOException e) {
                    LOG.error("", e);
                }

            }

        }

        class ServerConnector extends AbstractConnector {

            ServerConnector(@NotNull final SocketAddress address) {
                super(address);
            }

            SelectableChannel connect(@NotNull final SocketAddress address, final byte identifier) throws
                    IOException {

                ServerSocketChannel serverChannel = ServerSocketChannel.open();
                final ServerSocket serverSocket = serverChannel.socket();
                closeables.add(serverSocket);
                closeables.add(serverChannel);
                serverSocket.setReuseAddress(true);
                serverSocket.bind(address);
                serverChannel.socket().setReceiveBufferSize(BUFFER_SIZE);
                serverChannel.configureBlocking(false);
                return serverChannel;
            }

        }

        class ClientConnector extends AbstractConnector {

            ClientConnector(SocketAddress address) {
                super(address);
            }

            /**
             * blocks until connected
             *
             * @param endpoint   the endpoint to connect
             * @param identifier used for logging only
             * @throws IOException if we are not successful at connection
             */
            SelectableChannel connect(final SocketAddress endpoint,
                                      final byte identifier)
                    throws IOException, InterruptedException {

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
                        socketChannel.socket().setReceiveBufferSize(BUFFER_SIZE);
                        closeables.add(socketChannel.socket());

                        // todo not sure why we have to have this here,
                        // but if we don't add it'll fail, no sure why ?
                        Thread.sleep(500);

                        socketChannel.connect(endpoint);
                        closeables.add(socketChannel);

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

                                try {
                                    socketChannel.socket().close();
                                } catch (Exception e) {
                                    LOG.error("", e);
                                }

                                socketChannel.close();
                            } catch (IOException e) {
                                LOG.error("", e);
                            }
                    }

                }

            }
        }

        if (serverEndpoint != null) {
            final Thread thread = new Thread(new ServerConnector(serverEndpoint));
            thread.setName("server-tcp-connector-" + serverEndpoint);
            thread.setDaemon(true);
            thread.start();
        }

        for (final SocketAddress endpoint : clientEndpoints) {
            final Thread thread = new Thread(new ClientConnector(endpoint));
            thread.setName("client-tcp-connector-" + endpoint);
            thread.setDaemon(true);
            thread.start();
        }

        return result;
    }

    private void onConnect(@NotNull final ReplicatedSharedHashMap map,
                           short packetSize,
                           int serializedEntrySize,
                           @NotNull final ReplicatedSharedHashMap.EntryExternalizable externalizable,
                           @NotNull final SelectionKey key) throws IOException, InterruptedException {
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

        for (Closeable closeable : this.closeables) {

            try {
                closeable.close();
            } catch (IOException e) {
                LOG.error("", e);
            }

        }

        closeables.clear();
        executorService.shutdownNow();
        selector.close();
    }


    private void doAccept(SelectionKey key,
                          final int serializedEntrySize,
                          final ReplicatedSharedHashMap.EntryExternalizable externalizable,
                          final short packetSize,
                          final int localIdentifier) throws IOException {
        final ServerSocketChannel server = (ServerSocketChannel) key.channel();
        final SocketChannel channel = server.accept();
        channel.configureBlocking(false);
        channel.socket().setReuseAddress(true);
        channel.socket().setKeepAlive(true);
        channel.socket().setSoTimeout(100);
        channel.socket().setSoLinger(false, 0);
        final Attached attached = new Attached();
        channel.register(selector, OP_WRITE | OP_READ, attached);

        attached.entryReader = new TcpSocketChannelEntryReader(serializedEntrySize,
                externalizable, packetSize);

        attached.entryWriter = new TcpSocketChannelEntryWriter(serializedEntrySize,
                externalizable, packetSize);

        attached.entryWriter.identifierToBuffer(localIdentifier);
    }

}

