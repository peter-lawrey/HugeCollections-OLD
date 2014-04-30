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
                                @NotNull final Alignment alignment, int entrySize) {


        final ByteBufferBytes buffer = new ByteBufferBytes(ByteBuffer.allocate(entrySize * MAX_NUMBER_OF_ENTRIES_PER_CHUNK));


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

                            long entrySize = bufferBytes.readStopBit();
                            final ByteBufferBytes slice = bufferBytes.createSlice(bufferBytes.position(), entrySize);
                            replicatedMap.onUpdate(slice);
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

                // this is used in nextEntry() below, its what could be described as callback method
                final SegmentModificationIterator.EntryProcessor entryProcessor = new SegmentModificationIterator.EntryProcessor() {

                    @Override
                    public void onEntry(NativeBytes entry) {

                        long keyLen = entry.readStopBit();
                        entry.skip(keyLen);

                        // timestamp + and id
                        entry.skip(9);

                        final boolean isDeleted = entry.readBoolean();
                        final long size;

                        if (!isDeleted) {
                            long valueLen = alignment.alignSize((int) entry.readStopBit());
                            alignment.alignPositionAddr(entry);
                            size = entry.position() + valueLen;

                        } else
                            size = entry.position();

                        entry.limit(size);
                        entry.position(0);

                        // write the entry size
                        buffer.writeStopBit(size);
                        buffer.write(entry);

                    }
                };

                for (; ; ) {

                    final boolean wasDataRead = segmentModificationIterator.nextEntry(entryProcessor);

                    if (wasDataRead)
                        count++;


                    if (count == MAX_NUMBER_OF_ENTRIES_PER_CHUNK || (!wasDataRead && count > 0)) {

                        final byte[] source = buffer.buffer().array();
                        final int length = (int) buffer.position();
                        final byte[] dest = new byte[(int) length + 2];

                        // lets write out the number of entries in this chunk
                        dest[0] = (byte) (count & 0xff);
                        dest[1] = (byte) ((count >> 8) & 0xff);


                        System.arraycopy(source, 0, dest, 2, length);
                        output.add(dest);

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
