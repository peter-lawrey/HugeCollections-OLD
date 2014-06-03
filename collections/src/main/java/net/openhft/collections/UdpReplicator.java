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
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static net.openhft.collections.ReplicatedSharedHashMap.ModificationIterator;


/**
 * The UdpReplicator attempts to read the data ( but it does not enforce or grantee delivery ), typically, you
 * should use the UdpReplicator if you have a large number of nodes, and you wish to receive the data before
 * it becomes available on TCP/IP. In order to not miss data. The UdpReplicator should be used in conjunction
 * the with the TCP Replicator.
 */
class UdpReplicator implements Closeable {

    private static final Logger LOG =
            LoggerFactory.getLogger(UdpReplicator.class.getName());

    private final int port;

    private final UdpSocketChannelEntryWriter socketChannelEntryWriter;
    private final byte localIdentifier;

    private final ExecutorService executorService;
    private final Set<Closeable> closeables = new CopyOnWriteArraySet<Closeable>();

    private final ModificationIterator udpModificationIterator;
    private final UdpSocketChannelEntryReader socketChannelEntryReader;

    UdpReplicator(@NotNull final ReplicatedSharedHashMap map,
                  final int port,
                  @NotNull final UdpSocketChannelEntryWriter socketChannelEntryWriter,
                  @NotNull final UdpSocketChannelEntryReader socketChannelEntryReader,
                  @NotNull final ModificationIterator udpModificationIterator) throws IOException {
        this.port = port;

        this.localIdentifier = map.identifier();
        this.socketChannelEntryWriter = socketChannelEntryWriter;
        this.socketChannelEntryReader = socketChannelEntryReader;
        this.udpModificationIterator = udpModificationIterator;

        executorService = newSingleThreadExecutor(new NamedThreadFactory("UdpReplicator-" + localIdentifier, true));

        executorService.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    process();
                } catch (Exception e) {
                    close();

                }
            }

        });
    }


    @Override
    public void close() {
        executorService.shutdownNow();

        for (Closeable closeable : closeables) {
            try {
                closeable.close();
            } catch (IOException e) {
                LOG.error("", e);
            }
        }
    }

    /**
     * binds to the server socket and process data This method will block until interrupted
     *
     * @throws Exception
     */
    private void process() throws Exception {


        final Selector selector = Selector.open();

        connectClient(port).register(selector, SelectionKey.OP_READ);
        connectServer("255.255.255.255", port).register(selector, SelectionKey.OP_WRITE);


        for (; ; ) {
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

                    if (key.isReadable()) {
                        final DatagramChannel socketChannel = (DatagramChannel) key.channel();
                        socketChannelEntryReader.readAll(socketChannel);
                    }

                    if (key.isWritable()) {
                        final DatagramChannel socketChannel = (DatagramChannel) key.channel();
                        socketChannelEntryWriter.writeAll(socketChannel, udpModificationIterator);
                    }


                } catch (Exception e) {

                    if (key.channel().isOpen())
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
     * @param hostname //224.0.0.0 through 239.255.255.255
     * @param port
     * @throws IOException
     */
    private DatagramChannel connectServer(final String hostname, final int port) throws IOException {

        final DatagramChannel server = DatagramChannel.open();
        final InetSocketAddress hostAddress = new InetSocketAddress(hostname, port);

        // Create a non-blocking socket channel
        server.socket().setBroadcast(true);
        server.configureBlocking(false);

        // Kick off connection establishment
        server.connect(hostAddress);
        server.setOption(StandardSocketOptions.SO_REUSEADDR, true)
                .setOption(StandardSocketOptions.IP_MULTICAST_LOOP, true)
                .setOption(StandardSocketOptions.SO_BROADCAST, true)
                .setOption(StandardSocketOptions.SO_REUSEADDR, true);

        closeables.add(server);
        return server;
    }

    private DatagramChannel connectClient(final int port) throws IOException {
        final DatagramChannel client = DatagramChannel.open();

        final InetSocketAddress hostAddress = new InetSocketAddress(port);
        client.configureBlocking(false);

        client.bind(hostAddress);

        if (LOG.isDebugEnabled())
            LOG.debug("Listening on port " + port);

        closeables.add(client);
        return client;
    }


    static class UdpSocketChannelEntryWriter {

        private final ByteBuffer out;
        private final ByteBufferBytes in;
        private final TcpSocketReplicator.EntryCallback entryCallback;

        public UdpSocketChannelEntryWriter(final int serializedEntrySize,
                                           @NotNull final ReplicatedSharedHashMap.EntryExternalizable externalizable) {
            // we make the buffer twice as large just to give ourselves headroom
            out = ByteBuffer.allocateDirect(serializedEntrySize * 2);
            in = new ByteBufferBytes(out);
            entryCallback = new TcpSocketReplicator.EntryCallback(externalizable, in);
        }

        /**
         * writes all the entries that have changed, to the tcp socket
         *
         * @param socketChannel
         * @param modificationIterator
         * @throws InterruptedException
         * @throws java.io.IOException
         */
        void writeAll(@NotNull final DatagramChannel socketChannel,
                      @NotNull final ModificationIterator modificationIterator) throws InterruptedException, IOException {

            out.clear();
            in.clear();
            in.skip(2);

            final boolean wasDataRead = modificationIterator.nextEntry(entryCallback);

            if (!wasDataRead)
                return;

            // we'll write the size inverted at the start
            in.writeShort(0, ~(in.readUnsignedShort(2)));
            out.limit((int) in.position());

            try {
                socketChannel.write(out);
            } catch (IOException e) {
                if (LOG.isDebugEnabled())
                    LOG.debug("", e);
            }
        }


    }

    public static final int SIZE_OF_SHORT = 2;
    public static final int SIZE_OF_UNSIGNED_SHORT = 2;

    static class UdpSocketChannelEntryReader {

        private final ReplicatedSharedHashMap.EntryExternalizable externalizable;
        private final ByteBuffer in;
        private final ByteBufferBytes out;

        /**
         * @param serializedEntrySize the maximum size of an entry include the meta data
         * @param externalizable      supports reading and writing serialize entries
         */
        public UdpSocketChannelEntryReader(final int serializedEntrySize,
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
         * @param socketChannel
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


}





