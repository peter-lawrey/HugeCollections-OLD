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

import static net.openhft.collections.Builder.getPersistenceFile;
import static net.openhft.collections.UdpReplicatorBuilder.Unit.MEGA_BITS;

/**
 * Test  VanillaSharedReplicatedHashMap where the Replicated is over a TCP Socket
 *
 * @author Rob Austin.
 */

public class UDPSocketReplicationTest {

    static SharedHashMap<Integer, CharSequence> newUdpSocketShmIntString(
            final int identifier,
            final int udpPort) throws IOException {

        final UdpReplicatorBuilder udpReplicatorBuilder = new UdpReplicatorBuilder(udpPort, MEGA_BITS.toBits(50));
        return new SharedHashMapBuilder()
                .entries(1000)
                .identifier((byte) identifier)
                .udpReplication(udpReplicatorBuilder)
                .create(getPersistenceFile(), Integer.class, CharSequence.class);
    }

    //  private SharedHashMap<Integer, CharSequence> map1;
    private SharedHashMap<Integer, CharSequence> map2;

    @Before
    public void setup() throws IOException {
        //      map1 = newUdpSocketShmIntString(1, 1234);
        map2 = newUdpSocketShmIntString(1, 1234);
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @After
    public void tearDown() throws InterruptedException {

        for (final Closeable closeable : new Closeable[]{map2}) {
            try {
                closeable.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    @Test
    @Ignore
    public void testBufferOverflow() throws IOException, InterruptedException {

        for (int i = 0; i < 1024; i++) {
            Thread.sleep(5000);
            map2.put(i*2, "E");
            System.out.println("" + map2);
        }

    }


}



