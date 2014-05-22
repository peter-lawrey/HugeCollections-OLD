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

import net.openhft.collections.VanillaSharedReplicatedHashMap;
import net.openhft.collections.VanillaSharedReplicatedHashMapBuilder;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.TreeMap;

import static net.openhft.collections.map.replicators.ClientTcpSocketReplicator.ClientPort;
import static org.junit.Assert.assertEquals;

/**
 * Test  VanillaSharedReplicatedHashMap where the Replicated is over a TCP Socket
 *
 * @author Rob Austin.
 */

public class TCPSocketReplicationBootStrapTests {

    private VanillaSharedReplicatedHashMap<Integer, CharSequence> map1;
    private VanillaSharedReplicatedHashMap<Integer, CharSequence> map2;


    @Test
    @Ignore
    public void testBootstrap() throws IOException, InterruptedException {

        map1 = TCPSocketReplication4WayMapTest.newSocketShmIntString((byte) 1, 8077);
        final VanillaSharedReplicatedHashMap<Integer, CharSequence> map2a = TCPSocketReplication4WayMapTest.newSocketShmIntString((byte) 2, 8076, new ClientPort(8077, "localhost"));
        map2a.put(10, "EXAMPLE-10");  // this will be the last time that map1 go an update from map2

        long lastModificationTime;

        // lets make sure that the message has got to map 1
        do {
            lastModificationTime = map1.getLastModificationTime((byte) 2);
            Thread.yield();
        } while (lastModificationTime == 0);

        final File map2File = map2a.file();
        map2a.close();

        System.out.println("lastModificationTime=" + lastModificationTime);

        {
            // restart map 2 but don't connect it to map one
            final VanillaSharedReplicatedHashMap<Integer, CharSequence> map2b = new VanillaSharedReplicatedHashMapBuilder()
                    .entries(1000)
                    .identifier((byte) 2)
                    .create(map2File, Integer.class, CharSequence.class);
            // add data into it
            map2b.put(11, "ADDED WHEN DISCONNECTED TO MAP1");
            map2b.close();
        }

        // now restart map2a and connect it to map1, map1 should bootstrap the missing entry
        map2 = map2a.builder().create(map2File, Integer.class, CharSequence.class);

        // add data into it
        waitTillEqual(5000);
        assertEquals("ADDED WHEN DISCONNECTED TO MAP1", map1.get(11));

    }

    @After
    public void tearDown() {

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



