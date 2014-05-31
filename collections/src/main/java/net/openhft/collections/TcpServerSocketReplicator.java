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

/**
 * Used with a {@link ReplicatedSharedHashMap} to send data between the maps using nio, non blocking, server
 * socket connection.
 *
 * @author Rob Austin.
 * @see TcpClientSocketReplicator
 */
class TcpServerSocketReplicator extends AbstractTCPReplicator implements Closeable {

    private static final Logger LOG =
            LoggerFactory.getLogger(TcpServerSocketReplicator.class.getName());

    private final ReplicatedSharedHashMap map;
    private final InetSocketAddress address;
    private final ServerSocketChannel serverChannel;

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
                                     final short packetSize,
                                     final int serializedEntrySize) throws IOException {

        this.externalizable = externalizable;
        this.map = map;
        address = new InetSocketAddress(port);
        this.serverChannel = ServerSocketChannel.open();
        selector = Selector.open();
        this.localIdentifier = map.identifier();

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

        if (LOG.isDebugEnabled())
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

                    try {

                        if (key.isAcceptable()) {
                            doAccept(key);
                        }

                        if (key.isReadable()) {
                            onRead(map, key);
                        }

                        if (key.isWritable()) {
                            onWrite(key);
                        }

                    } catch (Exception e) {

                        if (serverChannel.isOpen())
                            LOG.error("", e);

                        // Close channel and nudge selector
                        try {
                            key.channel().close();
                        } catch (IOException ex) {
                            // ignore the exception, likely due to a client disconnect
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

    private void doAccept(SelectionKey key) throws IOException {
        final ServerSocketChannel server = (ServerSocketChannel) key.channel();
        final SocketChannel channel = server.accept();
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

        attached.entryWriter.identifierToBuffer(localIdentifier);
    }


}


