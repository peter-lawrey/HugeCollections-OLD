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
import java.util.concurrent.TimeUnit;

import static net.openhft.collections.Builder.getPersistenceFile;

/**
 * Test  VanillaSharedReplicatedHashMap where the Replicated is over a TCP Socket
 *
 * @author Rob Austin.
 */

public class TCPSocketReplicationTest2 {

    private SharedHashMap<Integer, CharSequence> map2;

    @Before
    public void setup() throws IOException {

        final TcpReplicatorBuilder tcpReplicatorBuilder = new TcpReplicatorBuilder(8079)
                .heartBeatInterval(10, TimeUnit.SECONDS);

        map2 = new SharedHashMapBuilder()
                .entries(1000)
                .identifier((byte) 2)
                .tcpReplicatorBuilder(tcpReplicatorBuilder)
                .entries(20000).file(getPersistenceFile()).kClass(Integer.class).vClass(CharSequence.class).create();
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
    public void testContinueToPublish() throws IOException, InterruptedException {
        for (; ; ) {
            for (int i = 0; i < 1024; i++) {
            //    Thread.sleep(1000);
                map2.put(1 + (i * 2), "E-1");
                System.out.println(map2);
            }
        }
    }


}



