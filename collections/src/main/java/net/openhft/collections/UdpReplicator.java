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
import net.openhft.lang.thread.NamedThreadFactory;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static java.nio.channels.SelectionKey.OP_READ;
import static java.nio.channels.SelectionKey.OP_WRITE;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static net.openhft.collections.ReplicatedSharedHashMap.ModificationIterator;
import static net.openhft.collections.TcpReplicator.AbstractConnector.Details;
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
class UdpReplicator implements Closeable {

    private static final Logger LOG =
            LoggerFactory.getLogger(UdpReplicator.class.getName());

    private final UdpSocketChannelEntryWriter writer;
    private final byte localIdentifier;

    private final ExecutorService executorService;
    private final Set<Closeable> closeables = new CopyOnWriteArraySet<Closeable>();

    private final ModificationIterator udpModificationIterator;
    private final UdpSocketChannelEntryReader reader;
    @NotNull
    private final UdpReplicatorBuilder udpReplicatorBuilder;
    private final int throttleInterval = 100;
    final Selector selector = Selector.open();
    private long maxBytesInInterval;
    private static final int BITS_IN_A_BYTE = 8;

    private Throttler throttler;
    private final ServerConnector serverConnector;
    private final Queue<SelectableChannel> pendingRegistrations;

    /**
     * @param map
     * @param udpReplicatorBuilder
     * @param udpModificationIterator
     * @param entrySize
     * @param externalizable
     * @throws IOException
     */
    UdpReplicator(@NotNull final ReplicatedSharedHashMap map,
                  @NotNull final UdpReplicatorBuilder udpReplicatorBuilder,
                  @NotNull final ModificationIterator udpModificationIterator, int entrySize,
                  @NotNull final ReplicatedSharedHashMap.EntryExternalizable externalizable) throws
            IOException {

        this.udpReplicatorBuilder = udpReplicatorBuilder;
        this.localIdentifier = map.identifier();
        this.udpModificationIterator = udpModificationIterator;

        this.writer = new UdpReplicator.UdpSocketChannelEntryWriter(entrySize, externalizable);
        this.reader = new UdpReplicator.UdpSocketChannelEntryReader(entrySize, externalizable);

        // throttling is calculated at bytes in a period, minus the size of on entry
        this.maxBytesInInterval = (TimeUnit.SECONDS.toMillis(udpReplicatorBuilder.throttle()) *
                throttleInterval * BITS_IN_A_BYTE) - entrySize;

        final InetSocketAddress address = new InetSocketAddress(udpReplicatorBuilder.broadcastAddress(), udpReplicatorBuilder.port());
        pendingRegistrations = new ConcurrentLinkedQueue<SelectableChannel>();
        Details connectionDetails = new Details(address, pendingRegistrations, closeables, 0, localIdentifier);
        serverConnector = new ServerConnector(connectionDetails);

        executorService = newSingleThreadExecutor(new NamedThreadFactory("UdpReplicator-" + localIdentifier, true));
        executorService.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    process();
                } catch (Exception e) {
                    LOG.error("", e);
                    close();

                }
            }

        });
    }


    @Override
    public void close() {
        executorService.shutdownNow();

        synchronized (closeables) {
            for (Closeable closeable : closeables) {
                try {
                    closeable.close();
                } catch (IOException e) {
                    LOG.error("", e);
                }
            }
        }
    }

    /**
     * binds to the server socket and process data This method will block until interrupted
     *
     * @throws Exception
     */
    private void process() throws Exception {


        connectClient(udpReplicatorBuilder.port()).register(selector, OP_READ);
        serverConnector.asyncConnect();


        for (; ; ) {
            // this may block for a long time, upon return the
            // selected set contains keys of the ready channels
            final int n = selector.select(100);

            register(this.pendingRegistrations);

            if (throttler != null)
                throttler.checkThrottleInterval();

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

                    if (key.isReadable()) {
                        final DatagramChannel socketChannel = (DatagramChannel) key.channel();
                        reader.readAll(socketChannel);
                    }

                    if (key.isWritable()) {
                        final DatagramChannel socketChannel = (DatagramChannel) key.channel();
                        try {
                            int len = writer.writeAll(socketChannel, udpModificationIterator);
                            throttler.checkUnregisterSelector(len);
                        } catch (NotYetConnectedException e) {
                            if (LOG.isDebugEnabled())
                                LOG.debug("", e);
                            serverConnector.reconnect(socketChannel);
                        } catch (IOException e) {
                            if (LOG.isDebugEnabled())
                                LOG.debug("", e);
                            serverConnector.reconnect(socketChannel);
                        }
                    }

                } catch (Exception e) {

                    LOG.error("", e);

                    // Close channel and nudge selector
                    try {
                        key.channel().close();
                    } catch (IOException ex) {
                        // do nothing
                    }
                }

            }
        }


    }


    /**
     * Registers the SocketChannel with the selector
     *
     * @param selectableChannels the SelectableChannel to register
     * @throws ClosedChannelException
     */
    private void register(@NotNull final Queue<SelectableChannel> selectableChannels) throws ClosedChannelException {
        for (SelectableChannel sc = selectableChannels.poll(); sc != null; sc = selectableChannels.poll()) {
            if (sc instanceof DatagramChannel) {
                sc.register(selector, OP_WRITE);

                if (throttleInterval > 0)
                    throttler = new Throttler((DatagramChannel) sc, selector, throttleInterval,
                            maxBytesInInterval, serverConnector);
            } else
                sc.register(selector, OP_READ);
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


    private static class UdpSocketChannelEntryWriter {

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

            if (!wasDataRead)
                return 0;

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

    /**
     * throttles 'writes' to ensure the network is not swamped, this is achieved by periodically
     * de-registering the write selector during periods of high volume.
     */
    private static class Throttler {

        private long lastTime = System.currentTimeMillis();
        private final DatagramChannel server;
        private long byteWritten;
        private Selector selector;
        private final int throttleInterval;
        private final long maxBytesInInterval;
        private final ServerConnector serverConnector;

        Throttler(DatagramChannel server,
                  Selector selector,
                  int throttleInterval,
                  long maxBytesInInterval, ServerConnector serverConnector) {
            this.server = server;
            this.selector = selector;
            this.throttleInterval = throttleInterval;
            this.maxBytesInInterval = maxBytesInInterval;
            this.serverConnector = serverConnector;
        }

        /**
         * re register the 'write' on the selector if the throttleInterval has passed
         *
         * @throws ClosedChannelException
         */
        public void checkThrottleInterval() throws ClosedChannelException {
            final long time = System.currentTimeMillis();

            if (lastTime + throttleInterval >= time)
                return;

            lastTime = time;
            byteWritten = 0;

            try {
                server.register(selector, OP_WRITE);
            } catch (IOException e) {
                if (LOG.isDebugEnabled())
                    LOG.debug("", e);
                serverConnector.reconnect(server);
            }
        }


        /**
         * checks the number of bytes written in the interval, so see if we should de-register the 'write' on
         * the selector, If it has it deRegisters the selector
         *
         * @param len the number of bytes just written
         * @throws ClosedChannelException
         */
        public void checkUnregisterSelector(int len) throws ClosedChannelException {
            byteWritten += len;
            if (byteWritten > maxBytesInInterval) {

                server.register(selector, 0);

                if (LOG.isDebugEnabled())
                    LOG.debug("Throttling UDP writes");
            }
        }
    }


    private class ServerConnector extends TcpReplicator.AbstractConnector {

        private final Details details;

        private ServerConnector(Details connectionDetails) {
            super(connectionDetails);
            this.details = connectionDetails;
        }

        SelectableChannel connect() throws
                IOException, InterruptedException {
            final DatagramChannel server = DatagramChannel.open();

            // Create a non-blocking socket channel
            server.socket().setBroadcast(true);
            server.configureBlocking(false);

            // Kick off connection establishment
            try {
                synchronized (details.closeables) {
                    server.connect(details.address);
                    details.closeables.add(server);
                }
            } catch (IOException e) {
                details.reconnectionInterval = 100;
                reconnect(server);
                return null;
            }
            server.setOption(StandardSocketOptions.SO_REUSEADDR, true)
                    .setOption(StandardSocketOptions.IP_MULTICAST_LOOP, false)
                    .setOption(StandardSocketOptions.SO_BROADCAST, true)
                    .setOption(StandardSocketOptions.SO_REUSEADDR, true);


            return server;
        }

        private void asyncConnect() {
            final Thread thread = new Thread(new ServerConnector(details));
            thread.setName("server-tcp-connector-" + details.address);
            thread.setDaemon(true);
            thread.start();
        }

        private void reconnect(DatagramChannel socketChannel) {
            try {
                socketChannel.close();
            } catch (IOException e1) {
                LOG.error("", e1);
            }

            serverConnector.asyncConnect();
        }

    }


}





