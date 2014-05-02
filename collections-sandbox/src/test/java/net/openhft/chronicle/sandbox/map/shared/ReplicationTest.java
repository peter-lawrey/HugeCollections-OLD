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

import net.openhft.collections.SegmentModificationIterator;
import net.openhft.collections.SharedHashMap;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Rob Austin.
 */
public class ReplicationTest {


    @Test
    public void test() throws IOException, InterruptedException {


        final ArrayBlockingQueue<byte[]> map1ToMap2 = new ArrayBlockingQueue<byte[]>(100);
        final ArrayBlockingQueue<byte[]> map2ToMap1 = new ArrayBlockingQueue<byte[]>(100);

        final SharedHashMap<Integer, CharSequence> map1 = Builder.newShmIntString(10, new SegmentModificationIterator((byte) 1), map1ToMap2, map2ToMap1, (byte) 1);
        final SharedHashMap<Integer, CharSequence> map2 = Builder.newShmIntString(10, new SegmentModificationIterator((byte) 2), map2ToMap1, map1ToMap2, (byte) 2);

        map1.put(1, "EXAMPLE");


        // allow time for the recompilation to resolve
        Thread.sleep(10);

        assertEquals(map1, map2);
        assertTrue(!map2.isEmpty());
        System.out.print(map1);

    }

    @Test
    public void testSoakTestWithRandomData() throws IOException, InterruptedException {

        final ArrayBlockingQueue<byte[]> map1ToMap2 = new ArrayBlockingQueue<byte[]>(100);
        final ArrayBlockingQueue<byte[]> map2ToMap1 = new ArrayBlockingQueue<byte[]>(100);

        final SegmentModificationIterator segmentModificationIterator1 = new SegmentModificationIterator((byte) 1);
        final SharedHashMap<Integer, Integer> map1 = Builder.newShmIntInt(10, segmentModificationIterator1, map1ToMap2, map2ToMap1, (byte) 1);

        final SegmentModificationIterator segmentModificationIterator2 = new SegmentModificationIterator((byte) 2);
        final SharedHashMap<Integer, Integer> map2 = Builder.newShmIntInt(10, segmentModificationIterator2, map2ToMap1, map1ToMap2, (byte) 2);

        for (int i = 1; i < 1000000; i++) {

            final ConcurrentMap<Integer, Integer> map = (Math.random() > 0.5) ? map1 : map2;

            switch ((int) (Math.random() * 2)) {
                case 0:
                    map.put((int) (Math.random() * 100), (int) (Math.random() * 25));
                    break;
                case 1:
                    map.remove((int) (Math.random() * 78));
                    break;
            }
        }

        // allow time for the recompilation to resolve
        // we will check 10 times that there all the work queues are empty
        int i = 0;
        for (; i < 10; i++) {
            if (!map1ToMap2.isEmpty() || !map2ToMap1.isEmpty() || segmentModificationIterator1.hasNext() || segmentModificationIterator2.hasNext()) {
                i = 0;
            }
            Thread.sleep(1);
        }

        assertEquals(map1, map2);

    }


}
