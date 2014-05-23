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
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static net.openhft.collections.ReplicatedSharedHashMap.ModificationIterator;


class UdpReplicator implements Closeable {

    private static final Logger LOG =
            LoggerFactory.getLogger(UdpReplicator.class.getName());

    private final int port;
    private final DatagramChannel datagramChannel;
    private final UdpSocketChannelEntryWriter socketChannelEntryWriter;
    private final byte localIdentifier;

    private final ExecutorService executorService;
    private final ModificationIterator udpModificationIterator;
    private final UdpSocketChannelEntryReader socketChannelEntryReader;

    UdpReplicator(@NotNull final ReplicatedSharedHashMap map,
                  final int port,
                  @NotNull final UdpSocketChannelEntryWriter socketChannelEntryWriter,
                  @NotNull final UdpSocketChannelEntryReader socketChannelEntryReader,
                  @NotNull final ModificationIterator udpModificationIterator) throws IOException {
        this.port = port;
        this.datagramChannel = DatagramChannel.open();
        this.localIdentifier = map.getIdentifier();
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
                    LOG.error("", e);
                    try {
                        datagramChannel.close();
                    } catch (IOException e1) {
                        LOG.error("", e1);
                    }

                }
            }

        });
    }


    @Override
    public void close() throws IOException {
        executorService.shutdownNow();

        if (datagramChannel != null)
            datagramChannel.close();
    }

    /**
     * binds to the server socket and process data
     * This method will block until interrupted
     *
     * @throws Exception
     */
    private void process() throws Exception {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Listening on port " + port);
        }
        final Selector selector = Selector.open();
        InetSocketAddress hostAddress = new InetSocketAddress("255.255.255.255", port);


        // Create a non-blocking socket channel
        datagramChannel.socket().setBroadcast(true);
       // datagramChannel.socket().bind(hostAddress);
        datagramChannel.configureBlocking(false);

        // Kick off connection establishment
        datagramChannel.connect(hostAddress);


        this.datagramChannel.register(selector, SelectionKey.OP_WRITE | SelectionKey.OP_READ);

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

                    if (this.datagramChannel.isOpen())
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


    static class UdpSocketChannelEntryWriter {

        private final ByteBuffer out;
        private final ByteBufferBytes in;
        private final TcpSocketChannelEntryWriter.EntryCallback entryCallback;

        public UdpSocketChannelEntryWriter(final int serializedEntrySize,
                                           @NotNull final ReplicatedSharedHashMap.EntryExternalizable externalizable) {
            // we make the buffer twice as large just to give ourselves headroom
            out = ByteBuffer.allocateDirect(serializedEntrySize * 2);
            in = new ByteBufferBytes(out);
            entryCallback = new TcpSocketChannelEntryWriter.EntryCallback(externalizable, in);
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
            socketChannel.write(out);

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

            final int bytesRead = socketChannel.read(in);

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





