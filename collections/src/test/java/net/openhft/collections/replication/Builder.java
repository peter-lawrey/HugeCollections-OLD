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

package net.openhft.collections.replication;


import net.openhft.collections.VanillaSharedReplicatedHashMap;
import net.openhft.collections.VanillaSharedReplicatedHashMapBuilder;
import net.openhft.lang.values.IntValue;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * @author Rob Austin.
 */
public class Builder {

    // added to ensure uniqueness
    static int count;

    public static File getPersistenceFile() {
        String TMP = System.getProperty("java.io.tmpdir");
        File file = new File(TMP + "/shm-test" + System.nanoTime() + (count++));
        file.delete();
        file.deleteOnExit();
        return file;
    }


    static VanillaSharedReplicatedHashMap<Integer, CharSequence> newShmIntString(
            int size, final ArrayBlockingQueue<byte[]> input,
            final ArrayBlockingQueue<byte[]> output, final byte localIdentifier, byte externalIdentifier) throws IOException {

        final VanillaSharedReplicatedHashMapBuilder builder =
                new VanillaSharedReplicatedHashMapBuilder()
                        .entries(size)
                        .identifier(localIdentifier);

        final VanillaSharedReplicatedHashMap<Integer, CharSequence> result =
                builder.create(getPersistenceFile(), Integer.class, CharSequence.class);

        new QueueReplicator(result.acquireModificationIterator(externalIdentifier),
                input, output, builder.entrySize(), result);

        return result;

    }

    interface MapProvider<T> {
        T getMap();

        boolean isQueueEmpty();
    }

    static MapProvider<VanillaSharedReplicatedHashMap<Integer, Integer>> newShmIntInt(
            int size, final ArrayBlockingQueue<byte[]> input,
            final ArrayBlockingQueue<byte[]> output, final byte localIdentifier, byte externalIdentifier) throws IOException {

        final VanillaSharedReplicatedHashMapBuilder builder =
                new VanillaSharedReplicatedHashMapBuilder()
                        .entries(size)
                        .entrySize(24)
                        .actualSegments(2)
                        .identifier(localIdentifier);

        final VanillaSharedReplicatedHashMap<Integer, Integer> result =
                builder.create(getPersistenceFile(), Integer.class, Integer.class);

        final QueueReplicator q = new QueueReplicator(result.acquireModificationIterator(externalIdentifier),
                input, output, builder.entrySize(), result);

        return new MapProvider<VanillaSharedReplicatedHashMap<Integer, Integer>>() {

            @Override
            public VanillaSharedReplicatedHashMap<Integer, Integer> getMap() {
                return result;
            }

            @Override
            public boolean isQueueEmpty() {
                return q.isEmpty();
            }

        };


    }


    static VanillaSharedReplicatedHashMap<IntValue, IntValue> newShmIntValueIntValue(
            int size, final ArrayBlockingQueue<byte[]> input,
            final ArrayBlockingQueue<byte[]> output, final byte localIdentifier, byte externalIdentifier) throws IOException {

        final VanillaSharedReplicatedHashMapBuilder builder =
                new VanillaSharedReplicatedHashMapBuilder()
                        .entries(size)
                        .entrySize(24)
                        .actualSegments(2)
                        .identifier(localIdentifier);

        final VanillaSharedReplicatedHashMap<IntValue, IntValue> result =
                builder.create(getPersistenceFile(), IntValue.class, IntValue.class);


        new QueueReplicator(result.acquireModificationIterator(externalIdentifier),
                input, output, builder.entrySize(), result);
        return result;

    }


    static VanillaSharedReplicatedHashMap<CharSequence, CharSequence> newShmStringString(
            int size, final ArrayBlockingQueue<byte[]> input,
            final ArrayBlockingQueue<byte[]> output,
            final byte localIdentifier,
            byte externalIdentifier) throws IOException {

        final VanillaSharedReplicatedHashMapBuilder builder =
                new VanillaSharedReplicatedHashMapBuilder()
                        .entries(size)
                        .identifier(localIdentifier);

        final VanillaSharedReplicatedHashMap<CharSequence, CharSequence> result =
                builder.create(getPersistenceFile(), CharSequence.class, CharSequence.class);


        new QueueReplicator(result.acquireModificationIterator(externalIdentifier),
                input, output, builder.entrySize(), result);

        return result;

    }
}
