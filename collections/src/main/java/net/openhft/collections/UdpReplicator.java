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

import net.openhft.lang.io.ByteBufferBytes;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import static java.nio.channels.SelectionKey.OP_READ;
import static java.nio.channels.SelectionKey.OP_WRITE;
import static net.openhft.collections.ReplicatedSharedHashMap.ModificationIterator;
import static net.openhft.collections.ReplicatedSharedHashMap.ModificationNotifier;
import static net.openhft.collections.TcpReplicator.EntryCallback;


/**
 * Configuration (builder) class for TCP replication feature of {@link SharedHashMap}.
 *
 * @see SharedHashMapBuilder#udpReplication
 *
 *
 * The UdpReplicator attempts to read the data ( but it does not enforce or grantee delivery ), typically, you
 * should use the UdpReplicator if you have a large number of nodes, and you wish to receive the data before
 * it becomes available on TCP/IP. In order to not miss data. The UdpReplicator should be used in conjunction
 * with the TCP Replicator.
 */
class UdpReplicator extends AbstractChannelReplicator implements ModificationNotifier, Closeable {

    private static final Logger LOG =
            LoggerFactory.getLogger(UdpReplicator.class.getName());

    private final UdpSocketChannelEntryWriter writer;
    private final UdpSocketChannelEntryReader reader;

    private ModificationIterator modificationIterator;
    private Throttler throttler;

    @NotNull
    private final UdpReplicatorBuilder udpReplicatorBuilder;

    private final ServerConnector serverConnector;
    private final Queue<Runnable> pendingRegistrations;

    private SelectableChannel writeChannel;
    private volatile boolean shouldEnableOpWrite;


    @Override

    public void close() {
        writeChannel = null;
        super.close();
    }

    /**
     * @param map
     * @param udpReplicatorBuilder
     * @param serializedEntrySize
     * @param externalizable
     * @throws IOException
     */
    UdpReplicator(@NotNull final ReplicatedSharedHashMap map,
                  @NotNull final UdpReplicatorBuilder udpReplicatorBuilder,
                  int serializedEntrySize,
                  @NotNull final ReplicatedSharedHashMap.EntryExternalizable externalizable) throws
            IOException {
        super("UdpReplicator-" + map.identifier());

        this.udpReplicatorBuilder = udpReplicatorBuilder;

        this.writer = new UdpReplicator.UdpSocketChannelEntryWriter(serializedEntrySize, externalizable);
        this.reader = new UdpReplicator.UdpSocketChannelEntryReader(serializedEntrySize, externalizable);

        // throttling is calculated at bytes in a period, minus the size of on entry
        if (udpReplicatorBuilder.throttle() > 0)
            throttler = new Throttler(selector, 100,
                    serializedEntrySize, udpReplicatorBuilder.throttle());


        final InetSocketAddress address = new InetSocketAddress(udpReplicatorBuilder.broadcastAddress(),
                udpReplicatorBuilder.port());
        pendingRegistrations = new ConcurrentLinkedQueue<Runnable>();
        final Details connectionDetails = new Details(address, map.identifier());
        serverConnector = new ServerConnector(connectionDetails);

        this.executorService.execute(new Runnable() {
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


    /**
     * binds to the server socket and process data This method will block until interrupted
     */
    private void process() throws Exception {

        connectClient(udpReplicatorBuilder.port()).register(selector, OP_READ);
        serverConnector.connectLater();

        while (selector.isOpen()) {

            if (!pendingRegistrations.isEmpty())
                register(this.pendingRegistrations);

            // this may block for a long time, upon return the
            // selected set contains keys of the ready channels
            final int n = selector.select(100);

            if (shouldEnableOpWrite)
                enableWrites();

            if (throttler != null)
                throttler.checkThrottleInterval();

            if (n == 0) {
                continue;    // nothing to do
            }

            final Set<SelectionKey> selectionKeys = selector.selectedKeys();
            for (final SelectionKey key : selectionKeys) {

                try {

                    if (key.isReadable()) {
                        final DatagramChannel socketChannel = (DatagramChannel) key.channel();
                        reader.readAll(socketChannel);
                    }

                    if (key.isWritable()) {
                        final DatagramChannel socketChannel = (DatagramChannel) key.channel();
                        try {
                            int len = writer.writeAll(socketChannel, modificationIterator);
                            if (throttler != null)
                                throttler.contemplateUnregisterWriteSocket(len);
                        } catch (NotYetConnectedException e) {
                            if (LOG.isDebugEnabled())
                                LOG.debug("", e);
                            serverConnector.connectLater();
                        } catch (IOException e) {
                            if (LOG.isDebugEnabled())
                                LOG.debug("", e);
                            serverConnector.connectLater();
                        }
                    }

                } catch (Exception e) {

                    LOG.error("", e);

                    // Close channel and nudge selector
                    try {
                        key.channel().close();
                        if (throttler != null)
                            throttler.remove(key.channel());
                        closeables.remove(key.channel());
                    } catch (IOException ex) {
                        // do nothing
                    }
                }
            }

            selectionKeys.clear();
        }


    }


    private DatagramChannel connectClient(final int port) throws IOException {
        final DatagramChannel client = DatagramChannel.open();

        final InetSocketAddress hostAddress = new InetSocketAddress(port);
        client.configureBlocking(false);
        synchronized (closeables) {
            client.bind(hostAddress);

            if (LOG.isDebugEnabled())
                LOG.debug("Listening on port " + port);
            closeables.add(client);
        }
        return client;
    }

    /**
     * called whenever there is a change to the modification iterator
     */
    @Override
    public void onChange() {
        // the write have to be enabled on the same thread as the selector
        shouldEnableOpWrite = true;
        selector.wakeup();
    }

    private void enableWrites() {
        try {
            final SelectionKey selectionKey = writeChannel.keyFor(this.selector);
            selectionKey.interestOps(selectionKey.interestOps() | OP_WRITE);
        } catch (Exception e) {
            LOG.error("", e);
        }
    }

    private void disableWrites() {
        try {
            final SelectionKey selectionKey = writeChannel.keyFor(this.selector);
            selectionKey.interestOps(selectionKey.interestOps() & ~OP_WRITE);
        } catch (Exception e) {
            LOG.error("", e);
        }
    }

    private class UdpSocketChannelEntryWriter {

        private final ByteBuffer out;
        private final ByteBufferBytes in;
        private final EntryCallback entryCallback;

        UdpSocketChannelEntryWriter(final int serializedEntrySize,
                                    @NotNull final ReplicatedSharedHashMap.EntryExternalizable externalizable) {

            // we make the buffer twice as large just to give ourselves headroom
            out = ByteBuffer.allocateDirect(serializedEntrySize * 2);
            in = new ByteBufferBytes(out);
            entryCallback = new EntryCallback(externalizable, in);

        }

        /**
         * writes all the entries that have changed, to the tcp socket
         *
         * @param socketChannel
         * @param modificationIterator
         * @throws InterruptedException
         * @throws java.io.IOException
         */

        /**
         * update that are throttled are rejected.
         *
         * @param socketChannel        the socketChannel that we will write to
         * @param modificationIterator modificationIterator that relates to this channel
         * @throws InterruptedException
         * @throws IOException
         */
        int writeAll(@NotNull final DatagramChannel socketChannel,
                     @NotNull final ModificationIterator modificationIterator) throws InterruptedException, IOException {

            out.clear();
            in.clear();
            in.skip(2);

            final boolean wasDataRead = modificationIterator.nextEntry(entryCallback);

            if (!wasDataRead) {
                disableWrites();
                return 0;
            }

            // we'll write the size inverted at the start
            in.writeShort(0, ~(in.readUnsignedShort(2)));
            out.limit((int) in.position());

            return socketChannel.write(out);

        }
    }

    public static final int SIZE_OF_SHORT = 2;
    public static final int SIZE_OF_UNSIGNED_SHORT = 2;

    private static class UdpSocketChannelEntryReader {

        private final ReplicatedSharedHashMap.EntryExternalizable externalizable;
        private final ByteBuffer in;
        private final ByteBufferBytes out;

        /**
         * @param serializedEntrySize the maximum size of an entry include the meta data
         * @param externalizable      supports reading and writing serialize entries
         */
        UdpSocketChannelEntryReader(final int serializedEntrySize,
                                    @NotNull final ReplicatedSharedHashMap.EntryExternalizable externalizable) {
            // we make the buffer twice as large just to give ourselves headroom
            in = ByteBuffer.allocateDirect(serializedEntrySize * 2);
            this.externalizable = externalizable;
            out = new ByteBufferBytes(in);
            out.limit(0);
            in.clear();
        }

        /**
         * reads entries from the socket till it is empty
         *
         * @param socketChannel the socketChannel that we will read from
         * @throws IOException
         * @throws InterruptedException
         */
        void readAll(@NotNull final DatagramChannel socketChannel) throws IOException, InterruptedException {

            out.clear();
            in.clear();

            socketChannel.receive(in);

            final int bytesRead = in.position();

            if (bytesRead < SIZE_OF_SHORT + SIZE_OF_UNSIGNED_SHORT)
                return;

            out.limit(in.position());

            final short invertedSize = out.readShort();
            final int size = out.readUnsignedShort();

            // check the the first 4 bytes are the inverted len followed by the len
            // we do this to check that this is a valid start of entry, otherwise we throw it away
            if (((short) ~size) != invertedSize)
                return;

            if (out.remaining() != size)
                return;

            externalizable.readExternalEntry(out);
        }

    }


    private class ServerConnector extends TcpReplicator.AbstractConnector {

        private final Details details;

        private ServerConnector(Details connectionDetails) {
            super("UDP-Connector", closeables);
            this.details = connectionDetails;
        }

        SelectableChannel doConnect() throws
                IOException, InterruptedException {
            final DatagramChannel server = DatagramChannel.open();

            // Create a non-blocking socket channel
            server.socket().setBroadcast(true);
            server.configureBlocking(false);

            // Kick off connection establishment
            try {
                synchronized (UdpReplicator.this.closeables) {
                    server.connect(details.address());
                    UdpReplicator.this.closeables.add(server);
                }
            } catch (IOException e) {
                connectLater();
                return null;
            }
            server.setOption(StandardSocketOptions.SO_REUSEADDR, true)
                    .setOption(StandardSocketOptions.IP_MULTICAST_LOOP, false)
                    .setOption(StandardSocketOptions.SO_BROADCAST, true)
                    .setOption(StandardSocketOptions.SO_REUSEADDR, true);

            // the registration has be be run on the same thread as the selector
            pendingRegistrations.add(new Runnable() {
                @Override
                public void run() {

                    try {
                        server.register(selector, OP_WRITE);
                        writeChannel = server;
                        if (throttler != null)
                            throttler.add(server);
                    } catch (ClosedChannelException e) {
                        LOG.error("", e);
                    }

                }
            });

            return server;
        }


    }

    public void setModificationIterator(ModificationIterator modificationIterator) {
        this.modificationIterator = modificationIterator;
    }


}





