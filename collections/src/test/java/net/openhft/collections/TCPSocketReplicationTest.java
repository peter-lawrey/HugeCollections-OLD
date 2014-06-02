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

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Random;

import static net.openhft.collections.TCPSocketReplication4WayMapTest.newTcpSocketShmIntString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test  VanillaSharedReplicatedHashMap where the Replicated is over a TCP Socket
 *
 * @author Rob Austin.
 */

public class TCPSocketReplicationTest {


    private SharedHashMap<Integer, CharSequence> map1;
    private SharedHashMap<Integer, CharSequence> map2;

    @Before
    public void setup() throws IOException {
        map1 = newTcpSocketShmIntString((byte) 1, 8076, new InetSocketAddress("localhost", 8077));
        map2 = newTcpSocketShmIntString((byte) 2, 8077);
    }

    @After
    public void tearDown() throws InterruptedException {

        for (final Closeable closeable : new Closeable[]{map1, map2}) {
            try {
                closeable.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    @Test
    public void test3() throws IOException, InterruptedException {

        map1.put(5, "EXAMPLE-2");

        // allow time for the recompilation to resolve
        waitTillEqual(5000);

        assertEquals(map1, map2);
        assertTrue(!map1.isEmpty());
    }


    @Test
    public void test() throws IOException, InterruptedException {

        map1.put(1, "EXAMPLE-1");
        map1.put(2, "EXAMPLE-2");
        map1.put(3, "EXAMPLE-1");

        map2.put(5, "EXAMPLE-2");
        map2.put(6, "EXAMPLE-2");

        map1.remove(2);
        map2.remove(3);
        map1.remove(3);
        map2.put(5, "EXAMPLE-2");

        // allow time for the recompilation to resolve
        waitTillEqual(5000);

        assertEquals(map1, map2);
        assertTrue(!map1.isEmpty());

    }


    @Test
    public void testBufferOverflow() throws IOException, InterruptedException {

        for (int i = 0; i < map1.builder().entries(); i++) {
            map1.put(i, "EXAMPLE-1");
        }

        // allow time for the recompilation to resolve
        waitTillEqual(5000);

        assertEquals(map1, map2);
        assertTrue(!map2.isEmpty());

    }

    /**
     * * waits until map1 and map2 show the same value
     *
     * @param timeOutMs timeout in milliseconds
     * @throws InterruptedException
     */
    private void waitTillEqual(final int timeOutMs) throws InterruptedException {
        int t = 0;
        for (; t < timeOutMs; t++) {
            if (map1.equals(map2))
                break;
            Thread.sleep(1);
        }

    }


    @Test
    public void testSoakTestWithRandomData() throws IOException, InterruptedException {
        final long start = System.currentTimeMillis();
        System.out.print("SoakTesting ");
        for (int j = 1; j < 900 * 1000; j++) {
            if (j % 9000 == 0)
                System.out.print(".");
            Random rnd = new Random(j);
            for (int i = 1; i < 10; i++) {

                final int select = rnd.nextInt(2);
                final SharedHashMap<Integer, CharSequence> map = select > 0 ? map1 : map2;


                switch (rnd.nextInt(2)) {
                    case 0:
                        map.put(rnd.nextInt(1000) /* + select * 100 */, "test");
                        break;
                    case 1:
                        map.remove(rnd.nextInt(1000) /*+ select * 100 */);
                        break;
                }
            }

        }

        waitTillEqual(5000);

        final long time = System.currentTimeMillis() - start;

        //assertTrue("timeTaken="+time, time < 2200);
        System.out.println("\ntime taken millis=" + time);

    }

    @Ignore
    @Test
    public void testObjectAllocationWithYourKit() throws IOException, InterruptedException {


        System.out.print("SoakTesting ");
        for (; ; ) {

            Random rnd = new Random(0);
            for (int i = 1; i < 10; i++) {

                final int select = rnd.nextInt(2);
                final SharedHashMap<Integer, CharSequence> map = select > 0 ? map1 : map2;

                switch (rnd.nextInt(2)) {
                    case 0:
                        map.put(rnd.nextInt(1000) /* + select * 100 */, "test");
                        break;
                    case 1:
                        map.remove(rnd.nextInt(1000) /*+ select * 100 */);
                        break;
                }
            }

        }


    }


}



