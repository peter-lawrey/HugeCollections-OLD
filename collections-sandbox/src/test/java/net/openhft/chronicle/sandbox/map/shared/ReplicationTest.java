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

import java.io.IOException;
import java.util.Random;
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

        final SharedHashMap<Integer, CharSequence> map1 =
                Builder.newShmIntString(10, map1ToMap2, map2ToMap1, (byte) 1);
        final SharedHashMap<Integer, CharSequence> map2 =
                Builder.newShmIntString(10, map2ToMap1, map1ToMap2, (byte) 2);

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


        final ReplicatedSharedHashMap<Integer, Integer> map1 =
                Builder.newShmIntInt(20000, map2ToMap1, map1ToMap2, (byte) 1);
        ReplicatedSharedHashMap.ModificationIterator segmentModificationIterator1 =
                map1.getModificationIterator();

        final ReplicatedSharedHashMap<Integer, Integer> map2 =
                Builder.newShmIntInt(20000, map1ToMap2, map2ToMap1, (byte) 2);
        ReplicatedSharedHashMap.ModificationIterator segmentModificationIterator2 =
                map2.getModificationIterator();

        for (int j = 1; j < 1000; j++) {
            Random rnd = new Random(j);
              for (int i = 1; i < 1000; i++) {

                  final int select = rnd.nextInt(2);
                  final ConcurrentMap<Integer, Integer> map = select > 0 ? map1 : map2;

                switch (rnd.nextInt(2)) {
                    case 0:
                        map.put(rnd.nextInt(101) /* + select * 100 */, i);
                        break;
                    case 1:
//                         map.remove(rnd.nextInt(8) /*+ select * 100 */);
                        break;
            }
            }

            // allow time for the recompilation to resolve
            // we will check 10 times that there all the work queues are empty
            int i = 0;
            for (; i < 10; i++) {
                if (!map2ToMap1.isEmpty() || !map1ToMap2.isEmpty() ||
                        segmentModificationIterator1.hasNext() ||
                        segmentModificationIterator2.hasNext()) {
                    i = 0;
                }
                Thread.sleep(1);
            }


            assertEquals("j=" + j, map1, map2);
        }

    }


}
