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

import net.openhft.chronicle.sandbox.queue.locators.shared.remote.channel.provider.ClientSocketChannelProvider;
import net.openhft.chronicle.sandbox.queue.locators.shared.remote.channel.provider.ServerSocketChannelProvider;
import net.openhft.collections.*;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Rob Austin.
 */
public class SocketReplicationTest {


    // added to ensure uniqueness
    static int count;
    private SharedHashMap<Integer, CharSequence> map1;
    private SharedHashMap<Integer, CharSequence> map2;

    private static File getPersistenceFile() {
        String TMP = System.getProperty("java.io.tmpdir");
        File file = new File(TMP + "/shm-test" + System.nanoTime() + (count++));
        file.delete();
        file.deleteOnExit();
        return file;
    }

    static VanillaSharedReplicatedHashMap<Integer, CharSequence> newSocketShmIntString(
            final int size,
            final byte identifier, final String remoteHost, final int inPort, final int outPort) throws IOException {

        final VanillaSharedReplicatedHashMapBuilder builder =
                new VanillaSharedReplicatedHashMapBuilder()
                        .entries(size)
                        .identifier(identifier);

        final VanillaSharedReplicatedHashMap<Integer, CharSequence> result =
                builder.create(getPersistenceFile(), Integer.class, CharSequence.class);

        new InSocketReplicator(identifier, builder.entrySize(), new ServerSocketChannelProvider(outPort), result);

        new OutSocketReplicator(result.getModificationIterator(),
                identifier, builder.entrySize(),
                builder.alignment(), new ClientSocketChannelProvider(inPort, remoteHost), 1024);

        return result;

    }

    @Before
    public void setup() throws IOException {
        map1 = newSocketShmIntString(10, (byte) 1, "localhost", 8076, 8077);
        map2 = newSocketShmIntString(10, (byte) 2, "localhost", 8077, 8076);
    }


    @Test
    public void test() throws IOException, InterruptedException {

        map1.put(1, "EXAMPLE-1");
        map1.put(2, "EXAMPLE-1");
        map1.put(3, "EXAMPLE-1");

        map2.put(1, "EXAMPLE-2");
        map2.put(2, "EXAMPLE-2");

        map1.remove(2);
        map2.remove(3);
        map1.remove(3);

        // allow time for the recompilation to resolve
        waitTillEqual(5000);

        assertEquals(new TreeMap(map1), new TreeMap(map2));
        assertTrue(!map2.isEmpty());

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
            if (new TreeMap(map1).equals(new TreeMap(map2)))
                break;
            Thread.sleep(1);
        }


    }


}


