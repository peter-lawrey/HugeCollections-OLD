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
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;

/**
 * @author Rob Austin.
 */

public class QueueBasedReplicator<K, V> {

    public static final int STOPBIT = 1;
    public static final short MAX_NUMBER_OF_ENTRIES_PER_CHUNK = 10;


    public QueueBasedReplicator(@NotNull final ReplicatedSharedHashMap<Integer, CharSequence> replicatedMap,
                                @NotNull final SegmentModificationIterator segmentModificationIterator,
                                @NotNull final BlockingQueue<byte[]> input,
                                @NotNull final Queue<byte[]> output,
                                @NotNull final Executor e,
                                @NotNull final Alignment alignment,
                                final int entrySize) {


        // in bound
        e.execute(new Runnable() {

            short count;

            @Override
            public void run() {

                // this is used in nextEntry() below, its what could be described as callback method


                for (; ; ) {

                    final byte[] item;
                    try {
                        item = input.take();

                        final ByteBufferBytes bufferBytes = new ByteBufferBytes(ByteBuffer.wrap(item));
                        int numberOfEntries = bufferBytes.readShort();


                        for (int i = 1; i <= numberOfEntries; i++) {

                            final long entrySize = bufferBytes.readStopBit();

                            // process that entry
                            final ByteBufferBytes slice = bufferBytes.createSlice(0, entrySize);


                            replicatedMap.onUpdate(slice);

                            // skip onto the next entry
                            bufferBytes.skip(entrySize);
                        }

                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }

                }

            }

        });

        // out bound
        e.execute(new Runnable() {

            short count = 0;

            @Override
            public void run() {

                final ByteBufferBytes buffer = new ByteBufferBytes(ByteBuffer.allocate(entrySize * MAX_NUMBER_OF_ENTRIES_PER_CHUNK));

                // this is used in nextEntry() below, its what could be described as callback method
                final SegmentModificationIterator.EntryProcessor entryProcessor = new SegmentModificationIterator.EntryProcessor() {

                    @Override
                    public void onEntry(NativeBytes entry) {

                        long keyLen = entry.readStopBit();
                        entry.skip(keyLen);

                        // timestamp + and id, but not the deleted flag
                        entry.skip(9);

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
                            return;

                        // what we are doing here is skipping the alignment, as this wont work when we send the data over the wire.
                        entry.position(position);
                        alignment.alignPositionAddr(entry);
                        entry.limit(entry.position() + valueLen);

                        buffer.write(entry);

                    }
                };

                for (; ; ) {

                    final boolean wasDataRead = segmentModificationIterator.nextEntry(entryProcessor);

                    if (wasDataRead)
                        count++;


                    if (count == MAX_NUMBER_OF_ENTRIES_PER_CHUNK || (!wasDataRead && count > 0)) {

                        // we are going to create an byte[] so that the buffer can be copied into this.
                        final byte[] source = buffer.buffer().array();
                        final int length = (int) buffer.position();
                        final byte[] dest = new byte[length + 2];

                        // lets write out the number of entries in this chunk
                        dest[0] = (byte) (count & 0xff);
                        dest[1] = (byte) ((count >> 8) & 0xff);

                        System.arraycopy(source, 0, dest, 2, length);
                        output.add(dest);

                        // clear the buffer for reuse, we can store a maximum of MAX_NUMBER_OF_ENTRIES_PER_CHUNK in this buffer
                        buffer.clear();

                        // if we are not reading the MAX_NUMBER_OF_ENTRIES_PER_CHUNK then we'll sleep before doing the next one
                        if (count != MAX_NUMBER_OF_ENTRIES_PER_CHUNK) {
                            try {
                                Thread.sleep(1);
                            } catch (InterruptedException e1) {
                                //
                            }
                        }

                        count = 0;

                    }

                }


            }

        });


    }


}
