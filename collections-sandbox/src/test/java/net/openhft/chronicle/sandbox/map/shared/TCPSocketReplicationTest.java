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

import net.openhft.collections.SharedHashMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.TreeMap;

import static net.openhft.chronicle.sandbox.map.shared.TCPSocketReplication4WayMapTest.newSocketShmIntString;
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
        map1 = newSocketShmIntString((byte) 1, 8076);
        map2 = newSocketShmIntString((byte) 2, 8077, new InetSocketAddress("localhost", 8076));
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

        assertEquals(new TreeMap(map1), new TreeMap(map2));
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

        assertEquals(new TreeMap(map1), new TreeMap(map2));
        assertTrue(!map1.isEmpty());

    }


    @Test
    public void testBufferOverflow() throws IOException, InterruptedException {

        for (int i = 0; i < 1024; i++) {
            map1.put(i, "EXAMPLE-1");
        }

        // allow time for the recompilation to resolve
        waitTillEqual(5000);

        assertEquals(new TreeMap(map1), new TreeMap(map2));
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
            if (new TreeMap(map1).equals(new TreeMap(map2)))
                break;
            Thread.sleep(1);
        }

    }

}



