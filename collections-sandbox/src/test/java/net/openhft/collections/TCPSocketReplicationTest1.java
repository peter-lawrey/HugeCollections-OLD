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

import static net.openhft.collections.Builder.getPersistenceFile;

/**
 * Test  VanillaSharedReplicatedHashMap where the Replicated is over a TCP Socket
 *
 * @author Rob Austin.
 */

public class TCPSocketReplicationTest1 {
    static final int hostId = Integer.getInteger("hostId");
    static final InetSocketAddress[] NO_SERVERS = {};
    public static final String HOST_SERVER = System.getProperty("server", "server");
    static final InetSocketAddress[] ONE_SERVER = {new InetSocketAddress(HOST_SERVER, 8079)};
    public static final String HOST_SERVER2 = System.getProperty("server2", "server2");
    static final InetSocketAddress[] TWO_SERVER = {new InetSocketAddress(HOST_SERVER, 8079), new InetSocketAddress(HOST_SERVER2, 8079)};
    private SharedHashMap<Integer, CharSequence> map1;

    @Before
    public void setup() throws IOException {

        final TcpReplicatorBuilder tcpReplicatorBuilder = new TcpReplicatorBuilder(8079,
                hostId == 0 ? NO_SERVERS : hostId == 1 ? ONE_SERVER : TWO_SERVER)
                .deletedModIteratorFileOnExit(true)
                .throttleBucketIntervalMS(100)
                .heartBeatIntervalMS(1000);


        map1 = new SharedHashMapBuilder()
                .entries(1000)
                .identifier((byte) 2)
                .tcpReplication(tcpReplicatorBuilder)
                .entries(20000)
                .create(getPersistenceFile(), Integer.class, CharSequence.class);
    }

    @After
    public void tearDown() throws InterruptedException {
        for (final Closeable closeable : new Closeable[]{map1}) {
            try {
                closeable.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    @Test
    @Ignore
    public void testContinueToReceive() throws IOException, InterruptedException {
        Thread.sleep(1000);
        long count = 0;
        long start = System.nanoTime();
        StringBuilder sb = new StringBuilder();
        for (int j = 0; j < 1000000; j++) {
            Thread.sleep(1000);
            map1.put(j, "A");
            System.out.println(map1);
        }
        long time = System.nanoTime() - start;
        System.out.printf("Throughput %.2f Mputs/s%n", count * 1e3 / time);
        Thread.sleep(1000);
    }
}



