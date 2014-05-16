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
public class EntryWriter {


    private final int entrySize;
    final ByteBuffer byteBuffer;
    final ByteBufferBytes buffer;

    private final EntryCallback entryCallback;

    public EntryWriter(int entrySize, short maxNumberOfEntriesPerChunk, final ReplicatedSharedHashMap.EntryExternalizable externalizable) {
        this.entrySize = entrySize;
        byteBuffer = ByteBuffer.allocateDirect(entrySize * maxNumberOfEntriesPerChunk);
        //     this.entryBuffer = new ByteBufferBytes(ByteBuffer.allocateDirect(entrySize));
        buffer = new ByteBufferBytes(byteBuffer);
        this.entryCallback = new EntryCallback(externalizable);
    }


    /**
     * Called whenever a put() or remove() has occurred to a replicating map
     * <p/>
     *
     * @param entry the entry you will receive, this does not have to be locked, as locking is already provided from
     *              the caller.
     * @return false if this entry should be ignored because the {@code identifier} is not from
     * one of our changes, WARNING even though we check the {@code identifier} in the
     * ModificationIterator the entry may have been updated.
     */
    boolean onEntry(final NativeBytes entry, final ReplicatedSharedHashMap.EntryExternalizable externalizable) {


        buffer.skip(2);
        long start = (int) buffer.position();
        externalizable.writeExternalEntry(entry, buffer);

        if (buffer.position() - start == 0) {
            buffer.position(buffer.position() - 2);
            return false;
        }


        // write the len, just before the start
        buffer.writeUnsignedShort(start - 2L, (int) (buffer.position() - start));

        return true;
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

            if (wasDataRead) {
                //  isWritingEntry.set(false);
            } else if (buffer.position() == 0) {
                //  isWritingEntry.set(false);
                return;
            }

            if (buffer.remaining() > entrySize && (wasDataRead || buffer.position() == 0))
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


    class EntryCallback implements VanillaSharedReplicatedHashMap.EntryCallback {

        private final ReplicatedSharedHashMap.EntryExternalizable externalizable;

        EntryCallback(@NotNull final ReplicatedSharedHashMap.EntryExternalizable externalizable) {
            this.externalizable = externalizable;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean onEntry(NativeBytes entry) {
            return EntryWriter.this.onEntry(entry, externalizable);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onBeforeEntry() {
            //  isWritingEntry.set(true);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onAfterEntry() {
        }
    }

}
