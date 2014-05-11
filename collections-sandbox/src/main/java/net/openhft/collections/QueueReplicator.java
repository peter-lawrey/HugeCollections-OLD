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
import net.openhft.lang.io.NativeBytes;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.lang.System.arraycopy;

/**
 * This class replicates data from one ReplicatedShareHashMap to another using a queue
 * it was originally written to test the logic in the {@code VanillaSharedReplicatedHashMap}
 *
 * @author Rob Austin.
 */

public class QueueReplicator<K, V> {


    public static final short MAX_NUMBER_OF_ENTRIES_PER_CHUNK = 10;

    private double localIdentifier;

    private AtomicBoolean isWritingEntry = new AtomicBoolean(true);
    private AtomicBoolean isReadingEntry = new AtomicBoolean(true);


    private static final Logger LOG =
            Logger.getLogger(VanillaSharedReplicatedHashMap.class.getName());
    private ByteBufferBytes buffer;


    public QueueReplicator(@NotNull final ReplicatedSharedHashMap<Integer, CharSequence> replicatedMap,
                           @NotNull final ReplicatedSharedHashMap.ModificationIterator modificationIterator,
                           @NotNull final BlockingQueue<byte[]> input,
                           @NotNull final BlockingQueue<byte[]> output,
                           @NotNull final Alignment alignment,
                           final int entrySize, byte localIdentifier) {



        this.localIdentifier = localIdentifier;

        //todo HCOLL-71 fix the 128 padding
        final int entrySize0 = entrySize + 128;

        // in bound

        Executors.newSingleThreadExecutor(new ThreadFactory() {

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "reader-map" + replicatedMap.getIdentifier());
            }

        }).execute(new Runnable() {

            @Override
            public void run() {

                // this is used in nextEntry() below, its what could be described as callback method

                try {

                    for (; ; ) {

                        byte[] item = null;
                        try {


                            for (; ; ) {
                                isReadingEntry.set(true);
                                item = input.poll();
                                if (item == null) {
                                    isReadingEntry.set(false);
                                    Thread.sleep(1);
                                } else {
                                    break;
                                }

                            }

                            final ByteBufferBytes bufferBytes = new ByteBufferBytes(ByteBuffer.wrap(item));

                            while (bufferBytes.remaining() > 0) {

                                final long entrySize = bufferBytes.readStopBit();

                                final long position = bufferBytes.position();
                                final long limit = bufferBytes.limit();

                                bufferBytes.limit(position + entrySize);
                                replicatedMap.onUpdate(bufferBytes);

                                bufferBytes.position(position);
                                bufferBytes.limit(limit);

                                // skip onto the next entry
                                bufferBytes.skip(entrySize);

                            }
                            isReadingEntry.set(false);

                        } catch (InterruptedException e1) {
                            LOG.log(Level.SEVERE, "", e1);
                        }

                    }
                } catch (Exception e) {
                    LOG.log(Level.SEVERE, "", e);
                }

            }

        });

        // out bound
        Executors.newSingleThreadExecutor().execute(new Runnable() {


            @Override
            public void run() {

                buffer = new ByteBufferBytes(ByteBuffer.allocate(entrySize0 * MAX_NUMBER_OF_ENTRIES_PER_CHUNK));

                // this is used in nextEntry() below, its what could be described as callback method
                final VanillaSharedReplicatedHashMap.EntryCallback entryCallback =
                        new VanillaSharedReplicatedHashMap.EntryCallback() {


                            /**
                             * {@inheritDoc}
                             */
                            @Override
                            public boolean onEntry(NativeBytes entry) {

                                long keyLen = entry.readStopBit();
                                entry.skip(keyLen);

                                //  timestamp, readLong is faster than skip(8)
                                entry.readLong();

                                // we have to check the id again, as it may have changes since we last walked the bit-set
                                // this use case can occur when a remote node update this entry.

                                if (entry.readByte() != QueueReplicator.this.localIdentifier)
                                    return false;

                                final boolean isDeleted = entry.readBoolean();
                                long valueLen = isDeleted ? 0 : entry.readStopBit();

                                // set the limit on the entry to the length ( in bytes ) of our entry
                                final long position = entry.position();

                                // write the entry size
                                final long length = position + valueLen;
                                buffer.writeStopBit(length);

                                // we are going to write the first part of the entry
                                buffer.write(entry.position(0).limit(position));

                                if (isDeleted)
                                    return true;

                                // skipping the alignment, as alignment wont work when we send the data over the wire.
                                entry.position(position);
                                alignment.alignPositionAddr(entry);
                                entry.limit(entry.position() + valueLen);

                                // writes the value into the buffer
                                buffer.write(entry);

                                return true;
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


                        };

                try {
                    for (; ; ) {

                        final boolean wasDataRead = modificationIterator.nextEntry(entryCallback);

                        if (wasDataRead) {

                            isWritingEntry.set(false);
                        } else if (buffer.position() == 0) {
                            isWritingEntry.set(false);
                            continue;
                        }

                        if (buffer.remaining() <= entrySize0 && ((wasDataRead || buffer.position() == 0)))
                            continue;

                        // we are going to create an byte[] so that the buffer can be copied into this.
                        final byte[] source = buffer.buffer().array();
                        final int length = (int) buffer.position();
                        final byte[] dest = new byte[length];

                        arraycopy(source, 0, dest, 0, length);

                        try {
                            output.put(dest);
                        } catch (InterruptedException e1) {
                            LOG.log(Level.SEVERE, "", e1);
                            break;
                        }

                        // clear the buffer for reuse, we can store a maximum of MAX_NUMBER_OF_ENTRIES_PER_CHUNK in this buffer
                        buffer.clear();

                    }
                } catch (Exception e) {
                    LOG.log(Level.SEVERE, "", e);
                }
            }


        });


    }


    /**
     * @return true indicates that all the data has been processed ( it lock free so can not be relied upon )
     */
    public boolean isEmpty() {
        final boolean b = isWritingEntry.get() || isReadingEntry.get();
        return !b && buffer.position() == 0;
    }

}

