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

/**
 * Test  VanillaSharedReplicatedHashMap where the Replicated is over a TCP Socket
 *
 * @author Rob Austin.
 */

public class TCPSocketReplicationTest2 {

    private SharedHashMap<Integer, CharSequence> map1;

    @Before
    public void setup() throws IOException {

        final TcpReplicatorBuilder tcpReplicatorBuilder = new TcpReplicatorBuilder(8079)
                .heartBeatInterval(1000);

        map1 = new SharedHashMapBuilder()
                .entries(1000)
                .identifier((byte) 1)
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
    public void testContinueToPublish() throws IOException, InterruptedException {
        for (; ; ) {
            for (int i = 0; i < 1024; i++) {
                map1.put(i * 2, "EXAMPLE-1");
            }
        }
    }


}



