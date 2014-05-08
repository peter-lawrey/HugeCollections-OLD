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

import net.openhft.collections.ReplicatedSharedHashMap;
import net.openhft.collections.TimeProvider;
import net.openhft.collections.VanillaSharedReplicatedHashMap;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;

import static org.junit.Assert.assertEquals;

/**
 * @author Rob Austin.
 */
public class MultiMapTimeBaseReplicationTest {

    private ReplicatedSharedHashMap.ModificationIterator segmentModificationIterator2;
    private ReplicatedSharedHashMap.ModificationIterator segmentModificationIterator1;
    private Builder.MapProvider<VanillaSharedReplicatedHashMap<Integer, Integer>> mapP1;
    private Builder.MapProvider<VanillaSharedReplicatedHashMap<Integer, Integer>> mapP2;
    private VanillaSharedReplicatedHashMap<Integer, Integer> map2;
    private VanillaSharedReplicatedHashMap<Integer, Integer> map1;
    private ArrayBlockingQueue<byte[]> map1ToMap2;
    private ArrayBlockingQueue<byte[]> map2ToMap1;

    @Before
    public void setup() throws IOException {
        final TimeProvider timeProvider = Mockito.mock(TimeProvider.class);


        map1ToMap2 = new ArrayBlockingQueue<byte[]>(10000);
        map2ToMap1 = new ArrayBlockingQueue<byte[]>(10000);


        mapP1 = Builder.newShmIntInt(20000, map2ToMap1, map1ToMap2, (byte) 1);
        map1 = mapP1.getMap();
        segmentModificationIterator1 = map1.getModificationIterator();

        mapP2 = Builder.newShmIntInt(20000, map1ToMap2, map2ToMap1, (byte) 2);
        map2 = mapP2.getMap();

        segmentModificationIterator2 = map2.getModificationIterator();

        Mockito.when(timeProvider.currentTimeMillis()).thenReturn((long) 1);
    }


    @Test
    public void testPut2Remove2Remove2Put1() throws IOException, InterruptedException {


        map2.put(1, 1, (byte) 2, 1399459457425L);
        map2.remove(1, null, (byte) 2, 1399459457425L);
        map2.remove(1, null, (byte) 2, 1399459457426L);
        waitTillFinished();
        map1.put(1, 895, (byte) 1, 1399459457426L);


        // we will check 10 times that there all the work queues are empty
        waitTillFinished();


        assertEquals(new TreeMap(map1), new TreeMap(map2));


    }


    @Test
    public void testPut1Remove2Remove2() throws IOException, InterruptedException {

        map1.put(1, 895, (byte) 1, 1399459457425L);
        waitTillFinished();
        map2.remove(1, null, (byte) 2, 1399459457425L);
        map2.remove(1, null, (byte) 2, 1399459457426L);


        // we will check 10 times that there all the work queues are empty
        waitTillFinished();
        assertEquals(new TreeMap(map1), new TreeMap(map2));

    }


    @Test
    public void testRemove1put2() throws IOException, InterruptedException {

        map2.remove(1, null, (byte) 2, 1399459457425L);
        waitTillFinished();
        map1.put(1, 895, (byte) 1, 1399459457425L);

        // we will check 10 times that there all the work queues are empty
        waitTillFinished();


        assertEquals(new TreeMap(map1), new TreeMap(map2));
    }


    @Test
    public void testRemove1put2Flip() throws IOException, InterruptedException {

        map1.remove(1, null, (byte) 1, 1399459457425L);
        waitTillFinished();
        map2.put(1, 895, (byte) 2, 1399459457425L);

        // we will check 10 times that there all the work queues are empty
        waitTillFinished();


        assertEquals(new TreeMap(map1), new TreeMap(map2));
    }


    @Test
    public void testPut1Put1CrazyTimes() throws IOException, InterruptedException {

        map1.put(1, 894, (byte) 1, 1399459457425L);
        waitTillFinished();
        map2.put(1, 895, (byte) 2, 0L);

        // we will check 10 times that there all the work queues are empty
        waitTillFinished();


        assertEquals(new TreeMap(map1), new TreeMap(map2));
    }


    @Test
    public void testPut1Put1SameTimes() throws IOException, InterruptedException {

        map1.put(1, 10, (byte) 1, 0L);
        waitTillFinished();
        map2.put(1, 20, (byte) 2, 0L);

        // we will check 10 times that there all the work queues are empty
        waitTillFinished();


        assertEquals(new TreeMap(map1), new TreeMap(map2));
    }

    @Test
    public void testPut1Put1SameTimesFlip() throws IOException, InterruptedException {

        map2.put(1, 894, (byte) 2, 0L);
        waitTillFinished();
        map1.put(1, 895, (byte) 1, 0L);

        // we will check 10 times that there all the work queues are empty
        waitTillFinished();


        assertEquals(new TreeMap(map1), new TreeMap(map2));
    }

    @Test
    public void testPut1Put1CrazyTimesMapFlip() throws IOException, InterruptedException {

        map2.put(1, 894, (byte) 2, 1399459457425L);
        waitTillFinished();
        map1.put(1, 895, (byte) 1, 0L);

        // we will check 10 times that there all the work queues are empty
        waitTillFinished();


        assertEquals(new TreeMap(map1), new TreeMap(map2));
    }


    @Test
    public void testRemovePut() throws IOException, InterruptedException {


        map2.remove(1, null, (byte) 2, 1399459457425L);
        waitTillFinished();
        map1.put(1, 1, (byte) 1, 1399459457425L);

        waitTillFinished();

        assertEquals(new TreeMap(map1), new TreeMap(map2));

    }


    @Test
    public void testRemovePutFlip() throws IOException, InterruptedException {


        map1.remove(1, null, (byte) 1, 1399459457425L);
        waitTillFinished();
        map2.put(1, 1, (byte) 2, 1399459457425L);
        waitTillFinished();
        // we will check 10 times that there all the work queues are empty

        assertEquals(new TreeMap(map1), new TreeMap(map2));


    }


    @Test
    public void testPutRemove() throws IOException, InterruptedException {


        map2.put(1, 1, (byte) 2, 1399459457425L);
        waitTillFinished();
        map1.remove(1, null, (byte) 1, 1399459457425L);
        waitTillFinished();
        // we will check 10 times that there all the work queues are empty

        assertEquals(new TreeMap(map1), new TreeMap(map2));

    }


    @Test
    public void testPutRemoveFilp() throws IOException, InterruptedException {


        map1.put(1, 1, (byte) 1, 1399459457425L);
        waitTillFinished();
        map2.remove(1, null, (byte) 2, 1399459457425L);
        waitTillFinished();
        // we will check 10 times that there all the work queues are empty

        assertEquals(new TreeMap(map1), new TreeMap(map2));

    }


    private void waitTillFinished() throws InterruptedException {
        int i = 0;
        for (; i < 3; i++) {
            if (!(map2ToMap1.isEmpty() && map2ToMap1.isEmpty() && !segmentModificationIterator1.hasNext() && !segmentModificationIterator2.hasNext() && mapP1.isQueueEmpty() && mapP2.isQueueEmpty())) {
                i = 0;
            }
            Thread.sleep(1);
        }
    }


   // @Ignore
    @Test
    public void testSoakTestWithRandomData() throws IOException, InterruptedException {


        for (int j = 1; j < 200; j++) {
            System.out.println(j);
            Random rnd = new Random(j);
            for (int i = 1; i < 10; i++) {

                final int select = rnd.nextInt(2);
                final ReplicatedSharedHashMap<Integer, Integer> map = select > 0 ? map1 : map2;


                switch (rnd.nextInt(2)) {
                    case 0:
                        map.put(rnd.nextInt(10) /* + select * 100 */, i);
                        break;
                    case 1:
                        map.remove(rnd.nextInt(8) /*+ select * 100 */);
                        break;
                }
            }


            waitTillFinished();

            assertEquals("j=" + j, new TreeMap(map1), new TreeMap(map2));
        }

    }


}
