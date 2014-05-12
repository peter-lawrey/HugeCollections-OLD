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


    public AbstractQueueReplicator(int entrySize, short maxNumberOfEntriesPerChunk) {
        byteBuffer = ByteBuffer.allocateDirect(entrySize * maxNumberOfEntriesPerChunk);
        buffer = new ByteBufferBytes(byteBuffer);
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

        final long limit = buffer.limit();
        final long entryStart = entry.position();

        final long length = externalizable.entryLength(entry);
        if (length == 0)
            return false;

        // we are now going to write the entry len
        buffer.writeStopBit(length);
        final long end = buffer.position() + length;
        buffer.limit(end);

        entry.position(entryStart);

        // and now the entry
        externalizable.writeExternalEntry(entry, buffer);

        buffer.limit(limit);
        buffer.position(end);

        return true;
    }

}
