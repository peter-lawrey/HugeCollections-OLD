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
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * @author Rob Austin.
 */
public class SocketChannelEntryWriter {

    private final int entryMaxSize;
    private final ByteBuffer byteBuffer;
    private final ByteBufferBytes buffer;
    private final ReplicatedSharedHashMap.EntryExternalizable externalizable;
    private final EntryCallback entryCallback = new EntryCallback();

    public SocketChannelEntryWriter(final int entryMaxSize,
                                    final short maxNumberOfEntriesPerChunk,
                                    @NotNull final ReplicatedSharedHashMap.EntryExternalizable externalizable) {
        this.entryMaxSize = entryMaxSize;
        byteBuffer = ByteBuffer.allocateDirect(entryMaxSize * maxNumberOfEntriesPerChunk);
        buffer = new ByteBufferBytes(byteBuffer);
        this.externalizable = externalizable;
    }


    /**
     * writes all the entries that have changed, to the tcp socket
     *
     * @param socketChannel
     * @param modificationIterator
     * @throws InterruptedException
     * @throws java.io.IOException
     */
    void writeAll(@NotNull final SocketChannel socketChannel,
                  final ReplicatedSharedHashMap.ModificationIterator modificationIterator) throws InterruptedException, IOException {


        //todo if buffer.position() ==0 it would make sense to call a blocking version of modificationIterator.nextEntry(entryCallback);
        for (; ; ) {

            final boolean wasDataRead = modificationIterator.nextEntry(entryCallback);

            if (!wasDataRead && buffer.position() == 0)
                return;

            if (buffer.remaining() > entryMaxSize && (wasDataRead || buffer.position() == 0))
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
     * {@inheritDoc}
     */
    class EntryCallback implements VanillaSharedReplicatedHashMap.EntryCallback {

        /**
         * {@inheritDoc}
         */
        public boolean onEntry(final NativeBytes entry) {

            buffer.skip(2);
            final long start = (int) buffer.position();
            externalizable.writeExternalEntry(entry, buffer);

            if (buffer.position() - start == 0) {
                buffer.position(buffer.position() - 2);
                return false;
            }

            // write the length of the entry, just before the start, so when we read it back
            // we read the length of the entry first and hence know how many preceding bytes to read
            final int entrySize = (int) (buffer.position() - start);
            buffer.writeUnsignedShort(start - 2L, entrySize);

            return true;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onAfterEntry() {

        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onBeforeEntry() {

        }
    }


}
