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

import net.openhft.chronicle.sandbox.queue.locators.shared.remote.channel.provider.SocketChannelProvider;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.NativeBytes;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Used with a {@see net.openhft.collections.ReplicatedSharedHashMap} to send data between the maps
 *
 * @author Rob Austin.
 */
public class OutSocketReplicator {

    @NotNull
    final ReplicatedSharedHashMap.ModificationIterator modificationIterator;


    public static final short MAX_NUMBER_OF_ENTRIES_PER_CHUNK = 10;
    private volatile short count;

    private AtomicBoolean isWritingEntry = new AtomicBoolean(true);


    public OutSocketReplicator(@NotNull final ReplicatedSharedHashMap.ModificationIterator modificationIterator,
                               final byte localIdentifier, final int entrySize,
                               @NotNull final Alignment alignment,
                               @NotNull final SocketChannelProvider socketChannelProvider) {
        this.modificationIterator = modificationIterator;


        //todo HCOLL-71 fix the 128 padding
        final int entrySize0 = entrySize + 128;

        // out bound
        Executors.newSingleThreadExecutor(new ThreadFactory() {

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "OutSocketReplicator-" + localIdentifier);
            }

        }).execute(new Runnable() {


            @Override
            public void run() {
                try {

                    final ByteBufferBytes buffer = new ByteBufferBytes(ByteBuffer.allocate(entrySize0 * MAX_NUMBER_OF_ENTRIES_PER_CHUNK));

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
                                    // this case can occur when a remote node update this entry.

                                    if (entry.readByte() != localIdentifier)
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


                    for (; ; ) {

                        // this is a blocking call
                        final boolean wasDataRead = modificationIterator.nextEntry(entryCallback);

                        if (wasDataRead) {
                            count++;
                            isWritingEntry.set(false);
                        } else if (count == 0) {
                            isWritingEntry.set(false);
                            continue;
                        }

                        if (count != MAX_NUMBER_OF_ENTRIES_PER_CHUNK && ((wasDataRead || count <= 0)))
                            continue;

                        buffer.flip();

                        final SocketChannel socketChannel = socketChannelProvider.getSocketChannel();
                        socketChannel.write(buffer.buffer());

                        // clear the buffer for reuse, we can store a maximum of MAX_NUMBER_OF_ENTRIES_PER_CHUNK in this buffer
                        buffer.clear();
                        count = 0;


                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }


        });


    }

    /**
     * @return true indicates that all the data has been processed at the time it was called
     */
    public boolean isEmpty() {
        final boolean b = isWritingEntry.get();
        return !b && count == 0;
    }


}
