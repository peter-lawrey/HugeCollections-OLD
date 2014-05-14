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
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.NativeBytes;

import java.nio.ByteBuffer;

/**
 * @author Rob Austin.
 */
abstract class AbstractQueueReplicator {


    final ByteBuffer byteBuffer;
    final ByteBufferBytes buffer;
    private final ByteBufferBytes entryBuffer;

    public AbstractQueueReplicator(int entrySize, short maxNumberOfEntriesPerChunk) {
        byteBuffer = ByteBuffer.allocateDirect(entrySize * maxNumberOfEntriesPerChunk);
        buffer = new ByteBufferBytes(byteBuffer);
        entryBuffer = new ByteBufferBytes(ByteBuffer.allocateDirect(entrySize));
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

}
