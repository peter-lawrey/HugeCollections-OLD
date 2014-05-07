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
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Rob Austin.
 */

public class QueueReplicator<K, V> {


    public static final short MAX_NUMBER_OF_ENTRIES_PER_CHUNK = 10;
    private final ReplicatedSharedHashMap<Integer, CharSequence> replicatedMap;
    private double localIdentifier;

    private AtomicBoolean isWritingEntry = new AtomicBoolean(true);
    private AtomicBoolean isReadingEntry = new AtomicBoolean(true);
    private volatile short count;

    public QueueReplicator(@NotNull final ReplicatedSharedHashMap<Integer, CharSequence> replicatedMap,
                           @NotNull final ReplicatedSharedHashMap.ModificationIterator modificationIterator,
                           @NotNull final BlockingQueue<byte[]> input,
                           @NotNull final BlockingQueue<byte[]> output,
                           @NotNull final Executor e,
                           @NotNull final Alignment alignment,
                           final int _entrySize, byte localIdentifier) {


        this.replicatedMap = replicatedMap;
        this.localIdentifier = localIdentifier;
        final int entrySize = _entrySize + 128;
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

                            //todo remove this
                            int numberOfEntries = bufferBytes.readShort();

                            for (int i = 1; i <= numberOfEntries; i++) {

                                final long entrySize = bufferBytes.readStopBit();

                                // process that entry
                                final ByteBufferBytes slice = bufferBytes.createSlice(0, entrySize);

                                replicatedMap.onUpdate(slice);


                                // skip onto the next entry
                                bufferBytes.skip(entrySize);

                            }
                            isReadingEntry.set(false);

                        } catch (InterruptedException e1) {
                            e1.printStackTrace();
                        }

                    }
                } catch (Exception e) {

                    e.printStackTrace();
                }

            }

        });

        // out bound
        Executors.newSingleThreadExecutor(new ThreadFactory() {

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "writer-map" + replicatedMap.getIdentifier());
            }

        }).execute(new Runnable() {


            @Override
            public void run() {

                final ByteBufferBytes buffer = new ByteBufferBytes(ByteBuffer.allocate(entrySize * MAX_NUMBER_OF_ENTRIES_PER_CHUNK));

                // this is used in nextEntry() below, its what could be described as callback method
                final VanillaSharedReplicatedHashMap.EntryCallback entryCallback =
                        new VanillaSharedReplicatedHashMap.EntryCallback() {


                            /**
                             *
                             * @param entry the entry you will receive
                             * @return false if this entry should be ignored because the {@code identifier} is not from one of our changes, WARNING even though we check the {@code identifier} in the ModificationIterator the entry may have been updated.
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

                            @Override
                            public void onAfterEntry() {
                                isWritingEntry.set(false);
                            }

                            @Override
                            public void onBeforeEntry() {
                                isWritingEntry.set(true);
                            }
                        };

                try {
                    for (; ; ) {

                        isWritingEntry.set(false);
                        final boolean wasDataRead = modificationIterator.nextEntry(entryCallback);

                        if (wasDataRead)
                            count++;
                        else if (count == 0)
                            continue;
                     /*   try {
                     //       Thread.sleep(1);
                            continue;
                        } catch (InterruptedException e1) {
                            e1.printStackTrace();
                        }*/


                        if (count == MAX_NUMBER_OF_ENTRIES_PER_CHUNK || (!wasDataRead && count > 0)) {

                            // we are going to create an byte[] so that the buffer can be copied into this.
                            final byte[] source = buffer.buffer().array();
                            final int length = (int) buffer.position();
                            final byte[] dest = new byte[length + 2];

                            // lets write out the number of entries in this chunk

                            // todo remove the number of records in the chuck as its not worth having
                            dest[0] = (byte) (count & 0xff);
                            dest[1] = (byte) ((count >> 8) & 0xff);

                            System.arraycopy(source, 0, dest, 2, length);
                            try {
                                output.put(dest);
                            } catch (InterruptedException e1) {
                                //
                            }


                            // clear the buffer for reuse, we can store a maximum of MAX_NUMBER_OF_ENTRIES_PER_CHUNK in this buffer
                            buffer.clear();
                            count = 0;

                        } else {
                            // Thread.yield();
                        }

                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }


        });


    }


    public boolean isEmpty() {

        final byte identifier = replicatedMap.getIdentifier();
        final boolean b = isWritingEntry.get() || isReadingEntry.get();
       /* if (b == true && identifier == 1) {
            int i = 1;
        } else {
            int i = 2;
        }*/

        return !b && count == 0;
    }

}
