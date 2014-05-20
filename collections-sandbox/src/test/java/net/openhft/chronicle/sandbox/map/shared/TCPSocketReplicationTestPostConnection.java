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
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.TreeMap;

import static net.openhft.collections.map.replicators.ClientTcpSocketReplicator.ClientPort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test  VanillaSharedReplicatedHashMap where the Replicated is over a TCP Socket
 *
 * @author Rob Austin.
 */

public class TCPSocketReplicationTestPostConnection {


    private SharedHashMap<Integer, CharSequence> map1;
    private SharedHashMap<Integer, CharSequence> map2;


    @Test
    public void testPostConnection() throws IOException, InterruptedException {


        map1 = TCPSocketReplication4WayMapTest.newSocketShmIntString((byte) 1, 8076);
        map1.put(5, "EXAMPLE-2");

        map2 = TCPSocketReplication4WayMapTest.newSocketShmIntString((byte) 2, 8077, new ClientPort(8076, "localhost"));

        // allow time for the recompilation to resolve
        waitTillEqual(500);

        assertEquals(new TreeMap(map1), new TreeMap(map2));
        assertTrue(!map1.isEmpty());

    }


    @After
    public void tearDown() {

        // todo fix close, it not blocking ( in other-words we should wait till everything is closed before running the next test)

        for (final Closeable closeable : new Closeable[]{map1, map2}) {
            try {
                closeable.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
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



