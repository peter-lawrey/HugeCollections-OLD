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

package net.openhft.chronicle.sandbox.map.shared;

import net.openhft.collections.*;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;

/**
 * @author Rob Austin.
 */
public class ReplicationTest {

    private static File getPersistenceFile() {
        String TMP = System.getProperty("java.io.tmpdir");
        File file = new File(TMP + "/shm-test" + System.nanoTime());
        file.delete();
        file.deleteOnExit();
        return file;
    }


    @Test
    public void test() throws IOException, InterruptedException {


        final ArrayBlockingQueue<byte[]> map1ToMap2 = new ArrayBlockingQueue<byte[]>(100);
        final ArrayBlockingQueue<byte[]> map2ToMap1 = new ArrayBlockingQueue<byte[]>(100);

        final SharedHashMap<Integer, CharSequence> map1 = newShmIntString(10, new SegmentModificationIterator(), map1ToMap2, map2ToMap1);
        final SharedHashMap<Integer, CharSequence> map2 = newShmIntString(10, new SegmentModificationIterator(), map2ToMap1, map1ToMap2);

        map1.put(1, "EXAMPLE");
        map1.remove(1, "EXAMPLE");

        map2.put(2, "EXAMPLE-2");

        // allow time for the recompilation to resolve
        Thread.sleep(10);


        assertEquals(map1, map2);
        System.out.print(map1);

    }

    @Test
    public void testSoakTestWithRandomData() throws IOException, InterruptedException {

        final ArrayBlockingQueue<byte[]> map1ToMap2 = new ArrayBlockingQueue<byte[]>(100);
        final ArrayBlockingQueue<byte[]> map2ToMap1 = new ArrayBlockingQueue<byte[]>(100);

        final SharedHashMap<Integer, Integer> map1 = newShmIntInt(10, new SegmentModificationIterator(), map1ToMap2, map2ToMap1);
        final SharedHashMap<Integer, Integer> map2 = newShmIntInt(10, new SegmentModificationIterator(), map2ToMap1, map1ToMap2);

        for (int i = 1; i < 100000; i++) {

            final ConcurrentMap map = (Math.random() > 0.5) ? map1 : map2;

            switch ((int) (Math.random() * 2)) {
                case 0:
                    map.put((int) (Math.random() * 25), (int) (Math.random() * 25));
                    break;
                case 1:
                    map.remove((Integer) ((int) (Math.random() * 25)));
                    break;


            }
        }


        // allow time for the recompilation to resolve
        Thread.sleep(100);


        assertEquals(map1, map2);
        System.out.print(map1);

    }


    SharedHashMap<Integer, CharSequence> newShmIntString(int size, final SegmentModificationIterator segmentModificationIterator, final ArrayBlockingQueue<byte[]> input, final ArrayBlockingQueue<byte[]> output) throws IOException {

        final VanillaSharedReplicatedHashMapBuilder builder = new VanillaSharedReplicatedHashMapBuilder()
                .entries(size)
                .eventListener(segmentModificationIterator);

        final VanillaSharedReplicatedHashMap<Integer, CharSequence> result = builder.create(getPersistenceFile(), Integer.class, CharSequence.class);

        segmentModificationIterator.setSegmentInfoProvider(result);

        final Executor e = Executors.newFixedThreadPool(2);

        new QueueBasedReplicator(result, segmentModificationIterator, input, output, e, builder.alignment(), builder.entrySize());

        return result;

    }

    SharedHashMap<Integer, Integer> newShmIntInt(int size, final SegmentModificationIterator segmentModificationIterator, final ArrayBlockingQueue<byte[]> input, final ArrayBlockingQueue<byte[]> output) throws IOException {

        final VanillaSharedReplicatedHashMapBuilder builder = new VanillaSharedReplicatedHashMapBuilder()
                .entries(size)
                .eventListener(segmentModificationIterator);

        final VanillaSharedReplicatedHashMap<Integer, Integer> result = builder.create(getPersistenceFile(), Integer.class, Integer.class);

        segmentModificationIterator.setSegmentInfoProvider(result);

        final Executor e = Executors.newFixedThreadPool(2);

        new QueueBasedReplicator(result, segmentModificationIterator, input, output, e, builder.alignment(), builder.entrySize());

        return result;

    }


}
