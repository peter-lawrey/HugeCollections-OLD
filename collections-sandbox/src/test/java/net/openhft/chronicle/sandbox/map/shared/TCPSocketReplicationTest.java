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
import net.openhft.chronicle.sandbox.queue.locators.shared.remote.channel.provider.SocketChannelProvider;
import net.openhft.collections.SharedHashMap;
import net.openhft.collections.VanillaSharedReplicatedHashMap;
import net.openhft.collections.VanillaSharedReplicatedHashMapBuilder;
import net.openhft.collections.map.replicators.InTcpSocketReplicator;
import net.openhft.collections.map.replicators.OutTcpSocketReplicator;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.TreeMap;

import static net.openhft.chronicle.sandbox.map.shared.Builder.getPersistenceFile;
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
    private ServerSocketChannelProvider serverSocketChannelProvider;
    private ClientSocketChannelProvider clientSocketChannelProvider;


    VanillaSharedReplicatedHashMap<Integer, CharSequence> newSocketShmIntString(
            final int size,
            final byte identifier,
            @NotNull final SocketChannelProvider socketChannelProvider,
            @NotNull final SocketChannelProvider clientSocketChannelProvider) throws IOException {

        final VanillaSharedReplicatedHashMapBuilder builder =
                new VanillaSharedReplicatedHashMapBuilder()
                        .entries(size)
                        .identifier(identifier);

        final VanillaSharedReplicatedHashMap<Integer, CharSequence> result =
                builder.create(getPersistenceFile(), Integer.class, CharSequence.class);

        new InTcpSocketReplicator(identifier, builder.entrySize(), socketChannelProvider, result);

        //    VanillaSharedReplicatedHashMap.WireFormat w =     VanillaSharedReplicatedHashMap.result.WireFormat();
        new OutTcpSocketReplicator(
                result.getModificationIterator(),
                identifier,
                builder.entrySize(),
                result,
                clientSocketChannelProvider,
                1024 * 8);

        return result;

    }

    static int i;

    @Before
    public void setup() throws IOException {
        i++;
        serverSocketChannelProvider = new ServerSocketChannelProvider(8076);
        clientSocketChannelProvider = new ClientSocketChannelProvider(8076, "localhost");

        map1 = newSocketShmIntString(10000, (byte) 1, serverSocketChannelProvider, serverSocketChannelProvider);
        map2 = newSocketShmIntString(10000, (byte) 2, clientSocketChannelProvider, clientSocketChannelProvider);
    }

    @After
    public void tearDown() {

        // todo fix close, it not blocking ( in other-words we should wait till everything is closed before running the next test)
        try {
            serverSocketChannelProvider.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            clientSocketChannelProvider.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    @Test
    @Ignore
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


