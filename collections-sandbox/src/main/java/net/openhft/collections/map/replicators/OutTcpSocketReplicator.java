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

package net.openhft.collections.map.replicators;

import net.openhft.collections.ReplicatedSharedHashMap;
import net.openhft.collections.VanillaSharedReplicatedHashMap;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.NativeBytes;
import net.openhft.lang.thread.NamedThreadFactory;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static net.openhft.collections.ReplicatedSharedHashMap.EntryExternalizable;
import static net.openhft.collections.ReplicatedSharedHashMap.ModificationIterator;

/**
 * Used with a {@see net.openhft.collections.ReplicatedSharedHashMap} to send data between the maps using a socket connection
 * <p/>
 * {@see net.openhft.collections.InSocketReplicator}
 *
 * @author Rob Austin.
 */
public class OutTcpSocketReplicator extends AbstractQueueReplicator implements Closeable {

    private static final Logger LOG =
            Logger.getLogger(VanillaSharedReplicatedHashMap.class.getName());
    @NotNull
    private final ReplicatedSharedHashMap map;

    private final int port;
    private final byte localIdentifier;

    @NotNull
    private AtomicBoolean isWritingEntry = new AtomicBoolean(true);

    private final int adjustedEntrySize;
    private final ReplicatedSharedHashMap.EntryCallback entryCallback;
    private final ByteBufferBytes headerBytesBuffer;
    private final ByteBuffer headerBB;
    private final ServerSocketChannel serverChannel;
    private final EntryReader entryReader;

    public OutTcpSocketReplicator(@NotNull final ReplicatedSharedHashMap map,
                                  final byte localIdentifier,
                                  final int entrySize,
                                  @NotNull final EntryExternalizable externalizable,
                                  int packetSizeInBytes,
                                  int port, final ServerSocketChannel serverChannel) {


        //todo HCOLL-71 fix the 128 padding
        super(entrySize + 128, toMaxNumberOfEntriesPerChunk(packetSizeInBytes, entrySize));

        this.entryReader = new EntryReader(entrySize + 128, externalizable);
        this.localIdentifier = localIdentifier;
        this.map = map;
        this.port = port;
        this.serverChannel = serverChannel;

        //todo HCOLL-71 fix the 128 padding
        this.adjustedEntrySize = entrySize + 128;
        this.entryCallback = new EntryCallback(externalizable);

        this.headerBB = ByteBuffer.allocateDirect(9);
        this.headerBytesBuffer = new ByteBufferBytes(headerBB);

        // out bound
        newSingleThreadExecutor(new NamedThreadFactory("OutSocketReplicator-" + localIdentifier, true)).execute(new Runnable() {

            @Override
            public void run() {
                try {
                    process();
                } catch (Exception e) {
                    LOG.log(Level.SEVERE, "", e);
                }
            }

        });
    }


    /**
     * writes at the entries that have changed to the socket
     * <p/>
     * A blocking call to process and read the next entry
     *
     * @param socketChannel
     * @param modificationIterator
     * @return
     * @throws InterruptedException
     * @throws IOException
     */
    private void writeAllEntries(@NotNull final SocketChannel socketChannel,
                                 final ModificationIterator modificationIterator) throws InterruptedException, IOException {
        //todo if buffer.position() ==0 it would make sense to call a blocking version of modificationIterator.nextEntry(entryCallback);
        for (; ; ) {

            final boolean wasDataRead = modificationIterator.nextEntry(entryCallback);

            if (wasDataRead) {
                isWritingEntry.set(false);
            } else if (buffer.position() == 0) {
                isWritingEntry.set(false);
                return;
            }

            if (buffer.remaining() > adjustedEntrySize && (wasDataRead || buffer.position() == 0))
                continue;

            buffer.flip();

            final ByteBuffer byteBuffer = buffer.buffer();
            byteBuffer.limit((int) buffer.limit());
            byteBuffer.position((int) buffer.position());

            socketChannel.write(byteBuffer);

            // clear the buffer for reuse, we can store a maximum of MAX_NUMBER_OF_ENTRIES_PER_CHUNK in this buffer
            buffer.clear();
            byteBuffer.clear();

            // we've filled up one buffer lets give another channel a chance to send data
            return;
        }

    }

    /**
     * @return true indicates that all the data has been processed at the time it was called
     */
    public boolean isEmpty() {
        final boolean b = isWritingEntry.get();
        return !b && buffer.position() == 0;
    }


    private static short toMaxNumberOfEntriesPerChunk(final double packetSizeInBytes, final double entrySize) {

        //todo HCOLL-71 fix the 128 padding
        final double entrySize0 = entrySize + 128;

        final double maxNumberOfEntriesPerChunkD = packetSizeInBytes / entrySize0;
        final int maxNumberOfEntriesPerChunk0 = (int) maxNumberOfEntriesPerChunkD;

        return (short) ((maxNumberOfEntriesPerChunkD != (double) ((int) maxNumberOfEntriesPerChunkD)) ?
                maxNumberOfEntriesPerChunk0 :
                maxNumberOfEntriesPerChunk0 + 1);
    }

    class EntryCallback implements VanillaSharedReplicatedHashMap.EntryCallback {

        private final EntryExternalizable externalizable;

        EntryCallback(@NotNull final EntryExternalizable externalizable) {
            this.externalizable = externalizable;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean onEntry(NativeBytes entry) {
            return OutTcpSocketReplicator.this.onEntry(entry, externalizable);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onBeforeEntry() {
            isWritingEntry.set(true);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onAfterEntry() {
        }
    }

    @Override
    public void close() throws IOException {
        if (serverChannel != null)
            serverChannel.close();
    }


    /**
     * binds to the server socket and process data
     * This method will block until interrupted
     *
     * @throws Exception
     */
    public void process()
            throws Exception {

        System.out.println("Listening on port " + port);

        // allocate an unbound process socket channel

        // Get the associated ServerSocket to bind it with
        ServerSocket serverSocket = serverChannel.socket();

        // create a new Selector for use below
        Selector selector = Selector.open();

        serverSocket.setReuseAddress(true);

        // set the port the process channel will listen to
        serverSocket.bind(new InetSocketAddress(port));

        // set non-blocking mode for the listening socket
        serverChannel.configureBlocking(false);

        // register the ServerSocketChannel with the Selector
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        while (true) {
            // this may block for a long time, upon return the
            // selected set contains keys of the ready channels
            int n = selector.select();

            if (n == 0) {
                continue;    // nothing to do
            }

            // get an iterator over the set of selected keys
            Iterator it = selector.selectedKeys().iterator();

            // look at each key in the selected set
            while (it.hasNext()) {
                SelectionKey key = (SelectionKey) it.next();

                // Is a new connection coming in?
                if (key.isAcceptable()) {
                    final ServerSocketChannel server = (ServerSocketChannel) key.channel();
                    final SocketChannel channel = server.accept();

                    // set the new channel non-blocking
                    channel.configureBlocking(false);

                    // read  remoteIdentifier and time stamp
                    headerBB.clear();
                    headerBytesBuffer.clear();

                    // read from the channel the timestamp and identifier
                    while (headerBB.remaining() > 0) {
                        channel.read(headerBB);
                    }

                    headerBytesBuffer.limit(headerBB.position());
                    final byte remoteIdentifier = headerBytesBuffer.readByte();

                    final ModificationIterator remoteModificationIterator = map.getModificationIterator(remoteIdentifier);

                    // todo HCOLL-77 : map replication : back fill missed updates on startup
                    remoteModificationIterator.dirtyAllEntriesNewerThan(headerBytesBuffer.readStopBit());

                    // register it with the selector and store the ModificationIterator for this key

                    channel.register(selector, SelectionKey.OP_WRITE | SelectionKey.OP_WRITE, remoteModificationIterator);

                    // notify remote map to start to receive data for {@code localIdentifier}
                    // entryReader.sendWelcomeMessage(channel, map.lastModification(), localIdentifier);
                }
                try {

                    if (key.isWritable()) {
                        final SocketChannel socketChannel = (SocketChannel) key.channel();
                        writeAllEntries(socketChannel, (ModificationIterator) key.attachment());
                    }

                    if (key.isReadable()) {
                        final SocketChannel socketChannel = (SocketChannel) key.channel();
                        entryReader.readEntriesTillSocketEmpty(socketChannel);
                    }

                } catch (Exception e) {

                    LOG.log(Level.INFO, "closing channel", e);

                    // Close channel and nudge selector
                    try {
                        key.channel().close();
                    } catch (IOException ex) {
                        // do nothing
                    }
                }
                // remove key from selected set, it's been handled
                it.remove();
            }
        }
    }


}
