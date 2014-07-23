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
import org.junit.Test;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import static java.util.concurrent.TimeUnit.SECONDS;
import static net.openhft.collections.Builder.getPersistenceFile;
import static org.junit.Assert.assertEquals;

/**
 * Test  VanillaSharedReplicatedHashMap where the Replicated is over a TCP Socket
 *
 * @author Rob Austin.
 */

public class TCPSocketReplicationBootStrapTests {

    private VanillaSharedReplicatedHashMap<Integer, CharSequence> map1;
    private SharedHashMap<Integer, CharSequence> map2;

    @Test
    public void testBootstrap() throws IOException, InterruptedException {

        map1 = TCPSocketReplication4WayMapTest.newTcpSocketShmIntString((byte) 1, 8079);

        final TcpReplicatorBuilder tcpReplicatorBuilder =
                new TcpReplicatorBuilder(8076, new InetSocketAddress("localhost", 8079))
                        .heartBeatInterval(1, SECONDS);

        final SharedHashMapBuilder builder = new SharedHashMapBuilder()
                .entries(1000)
                .identifier((byte) 2)
                .tcpReplicatorBuilder(tcpReplicatorBuilder)
                .entries(20000);

        final VanillaSharedReplicatedHashMap<Integer,
                CharSequence> map2a = (VanillaSharedReplicatedHashMap<Integer, CharSequence>) builder.file(getPersistenceFile()).kClass(Integer.class).vClass(CharSequence.class).create();

        map2a.put(10, "EXAMPLE-10");  // this will be the last time that map1 go an update from map2

        long lastModificationTime;

        // lets make sure that the message has got to map 1
        do {
            lastModificationTime = map1.lastModificationTime((byte) 2);
            Thread.yield();
        } while (lastModificationTime == 0);

        final File map2File = map2a.file();
        map2a.close();

        {
            // restart map 2 but don't doConnect it to map one
            final SharedHashMap<Integer, CharSequence> map2b = new SharedHashMapBuilder()
                    .entries(1000)
                    .identifier((byte) 2)
                    .canReplicate(true).file(map2File).kClass(Integer.class).vClass(CharSequence.class).create();
            // add data into it
            map2b.put(11, "ADDED WHEN DISCONNECTED TO MAP1");
            map2b.close();
        }

        // now restart map2a and doConnect it to map1, map1 should bootstrap the missing entry
        map2 = builder.file(map2File).kClass(Integer.class).vClass(CharSequence.class).create();

        // add data into it
        waitTillEqual(5000);
        assertEquals("ADDED WHEN DISCONNECTED TO MAP1", map1.get(11));

    }


    // todo we have to fix this
    @Test

    public void testBootstrapAndHeartbeat() throws IOException, InterruptedException {

        map1 = TCPSocketReplication4WayMapTest.newTcpSocketShmIntString((byte) 1, 8079, new InetSocketAddress("localhost", 8076));

        final TcpReplicatorBuilder tcpReplicatorBuilder =
                new TcpReplicatorBuilder(8076)
                        .heartBeatInterval(1, SECONDS);


        final SharedHashMapBuilder builder = new SharedHashMapBuilder()
                .entries(1000)
                .identifier((byte) 2)
                .tcpReplicatorBuilder(tcpReplicatorBuilder)
                .entries(20000);
        final VanillaSharedReplicatedHashMap<Integer, CharSequence> map2a = (VanillaSharedReplicatedHashMap<Integer, CharSequence>) builder.file(getPersistenceFile()).kClass(Integer.class).vClass(CharSequence.class).create();

        map2a.put(10, "EXAMPLE-10");  // this will be the last time that map1 go an update from map2

        long lastModificationTime;

        // lets make sure that the message has got to map 1
        do {
            lastModificationTime = map1.lastModificationTime((byte) 2);
            Thread.yield();
        } while (lastModificationTime == 0);

        final File map2File = map2a.file();
        map2a.close();

        {
            // restart map 2 but don't doConnect it to map one
            final SharedHashMap<Integer, CharSequence> map2b = new SharedHashMapBuilder()
                    .entries(1000)
                    .identifier((byte) 2)
                    .canReplicate(true).file(map2File).kClass(Integer.class).vClass(CharSequence.class).create();
            // add data into it
            map2b.put(11, "ADDED WHEN DISCONNECTED TO MAP1");
            map2b.close();
        }

        // now restart map2a and doConnect it to map1, map1 should bootstrap the missing entry
        map2 = builder.file(map2File).kClass(Integer.class).vClass(CharSequence.class).create();

        // add data into it
        waitTillEqual(20000);
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
     * waits until map1 and map2 show the same value
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

}



