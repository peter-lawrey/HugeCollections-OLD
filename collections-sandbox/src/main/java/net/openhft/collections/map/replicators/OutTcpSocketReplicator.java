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

import net.openhft.chronicle.sandbox.queue.locators.shared.remote.channel.provider.SocketChannelProvider;
import net.openhft.collections.ReplicatedSharedHashMap;
import net.openhft.collections.VanillaSharedReplicatedHashMap;
import net.openhft.lang.io.NativeBytes;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import static net.openhft.collections.ReplicatedSharedHashMap.EntryExternalizable;

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
    private final ReplicatedSharedHashMap.ModificationIterator modificationIterator;
    private AtomicBoolean isWritingEntry = new AtomicBoolean(true);
    private SocketChannelProvider socketChannelProvider;
    private final int adjustedEntrySize;
    private final ReplicatedSharedHashMap.EntryCallback entryCallback;

    public OutTcpSocketReplicator(@NotNull final ReplicatedSharedHashMap.ModificationIterator modificationIterator,
                                  final byte localIdentifier,
                                  final int entrySize,
                                  @NotNull final EntryExternalizable externalizable,
                                  @NotNull final SocketChannelProvider socketChannelProvider,
                                  int packetSizeInBytes) {

        super(entrySize + 128, toMaxNumberOfEntriesPerChunk(packetSizeInBytes, entrySize));

        this.adjustedEntrySize = entrySize + 128;
        this.modificationIterator = modificationIterator;
        this.socketChannelProvider = socketChannelProvider;
        this.entryCallback = new EntryCallback(externalizable);

        // out bound
        Executors.newSingleThreadExecutor(new ThreadFactory() {

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "OutSocketReplicator-" + localIdentifier);
            }

        }).execute(new Runnable() {

            @Override
            public void run() {
                // this is used in nextEntry() below, its what could be described as callback method
                for (; ; ) {
                    try {
                        final SocketChannel socketChannel = socketChannelProvider.getSocketChannel();

                        for (; ; ) {
                            readNextEntry(socketChannel);
                        }

                    } catch (Exception e) {
                        LOG.log(Level.SEVERE, "", e);
                    }
                }
            }
        });
    }


    /**
     * reads an entry from the socket
     * <p/>
     * A blocking call to process and read the next entry
     *
     * @param socketChannel
     * @return
     * @throws InterruptedException
     * @throws IOException
     */
    private void readNextEntry(@NotNull final SocketChannel socketChannel) throws InterruptedException, IOException {
        //todo if buffer.position() ==0 it would make sense to call a blocking version of modificationIterator.nextEntry(entryCallback);

        // this is not a blocking call
        final boolean wasDataRead = modificationIterator.nextEntry(entryCallback);

        if (wasDataRead) {
            isWritingEntry.set(false);
        } else if (buffer.position() == 0) {
            isWritingEntry.set(false);
            Thread.sleep(1);
            return;
        }

        if (buffer.remaining() > adjustedEntrySize && (wasDataRead || buffer.position() == 0))
            return;

        buffer.flip();

        final ByteBuffer byteBuffer = buffer.buffer();
        byteBuffer.limit((int) buffer.limit());
        byteBuffer.position((int) buffer.position());

        socketChannel.write(byteBuffer);

        // clear the buffer for reuse, we can store a maximum of MAX_NUMBER_OF_ENTRIES_PER_CHUNK in this buffer
        buffer.clear();
        byteBuffer.clear();

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
        socketChannelProvider.close();
    }

}
