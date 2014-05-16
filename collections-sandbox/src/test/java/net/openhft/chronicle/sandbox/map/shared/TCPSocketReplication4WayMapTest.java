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
import net.openhft.collections.VanillaSharedReplicatedHashMap;
import net.openhft.collections.VanillaSharedReplicatedHashMapBuilder;
import net.openhft.collections.map.replicators.ClientTcpSocketReplicator;
import net.openhft.collections.map.replicators.ServerTcpSocketReplicator;
import net.openhft.collections.map.replicators.SocketChannelEntryReader;
import net.openhft.collections.map.replicators.SocketChannelEntryWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.channels.ServerSocketChannel;
import java.util.TreeMap;

import static net.openhft.chronicle.sandbox.map.shared.Builder.getPersistenceFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test  VanillaSharedReplicatedHashMap where the Replicated is over a TCP Socket, but this versoin has 4 nodes
 *
 * @author Rob Austin.
 */
public class TCPSocketReplication4WayMapTest {


    private SharedHashMap<Integer, CharSequence> map1;
    private SharedHashMap<Integer, CharSequence> map2;
    private SharedHashMap<Integer, CharSequence> map3;
    private SharedHashMap<Integer, CharSequence> map4;

    private ClientTcpSocketReplicator.ClientPort clientSocketChannelProviderMap1;
    private ClientTcpSocketReplicator.ClientPort clientSocketChannelProviderMap2;
    private ClientTcpSocketReplicator.ClientPort clientSocketChannelProviderMap3;
    private ClientTcpSocketReplicator.ClientPort clientSocketChannelProviderMap4;
    private ServerSocketChannel serverChannel4;
    private ServerSocketChannel serverChannel3;
    private ServerSocketChannel serverChannel2;
    private ServerSocketChannel serverChannel1;

    VanillaSharedReplicatedHashMap<Integer, CharSequence> newSocketShmIntString(
            final int size,
            final byte identifier,
            final int serverPort, final byte[] externalIdentifiers, final ServerSocketChannel serverChannel,
            ClientTcpSocketReplicator.ClientPort... clientSocketChannelProviderMaps) throws IOException {

        final VanillaSharedReplicatedHashMapBuilder builder =
                new VanillaSharedReplicatedHashMapBuilder()
                        .entries(size)
                        .externalIdentifiers(externalIdentifiers)
                        .identifier(identifier);

        final VanillaSharedReplicatedHashMap<Integer, CharSequence> result =
                builder.create(getPersistenceFile(), Integer.class, CharSequence.class);


        final int adjustedEntrySize = builder.entrySize() + 128;
        final short maxNumberOfEntriesPerChunk = ServerTcpSocketReplicator.toMaxNumberOfEntriesPerChunk(1024 * 8, adjustedEntrySize);

        // the server will connect to all the clients, the clients will initiate the connection
        final SocketChannelEntryWriter socketChannelEntryWriter = new SocketChannelEntryWriter(adjustedEntrySize, maxNumberOfEntriesPerChunk, result);

        for (ClientTcpSocketReplicator.ClientPort clientSocketChannelProvider : clientSocketChannelProviderMaps) {
            new ClientTcpSocketReplicator(clientSocketChannelProvider, new SocketChannelEntryReader(builder.entrySize(), result), socketChannelEntryWriter, result);
        }

        new ServerTcpSocketReplicator(
                result,
                adjustedEntrySize,
                result,
                serverPort, serverChannel, socketChannelEntryWriter, maxNumberOfEntriesPerChunk);

        return result;
    }


    @Before
    public void setup() throws IOException {

        clientSocketChannelProviderMap1 = new ClientTcpSocketReplicator.ClientPort(8076, "localhost");
        clientSocketChannelProviderMap2 = new ClientTcpSocketReplicator.ClientPort(8077, "localhost");
        clientSocketChannelProviderMap3 = new ClientTcpSocketReplicator.ClientPort(8078, "localhost");
        clientSocketChannelProviderMap4 = new ClientTcpSocketReplicator.ClientPort(8079, "localhost");


        serverChannel1 = ServerSocketChannel.open();
        serverChannel2 = ServerSocketChannel.open();
        serverChannel3 = ServerSocketChannel.open();
        serverChannel4 = ServerSocketChannel.open();

        map1 = newSocketShmIntString(10000, (byte) 1, 8076, new byte[]{(byte) 2, (byte) 3, (byte) 4}, serverChannel1, clientSocketChannelProviderMap2, clientSocketChannelProviderMap3, clientSocketChannelProviderMap4);
        map2 = newSocketShmIntString(10000, (byte) 2, 8077, new byte[]{(byte) 1, (byte) 3, (byte) 4}, serverChannel2, clientSocketChannelProviderMap1, clientSocketChannelProviderMap3, clientSocketChannelProviderMap4);
        map3 = newSocketShmIntString(10000, (byte) 3, 8078, new byte[]{(byte) 1, (byte) 2, (byte) 4}, serverChannel3, clientSocketChannelProviderMap1, clientSocketChannelProviderMap2, clientSocketChannelProviderMap4);
        map4 = newSocketShmIntString(10000, (byte) 4, 8079, new byte[]{(byte) 1, (byte) 2, (byte) 3}, serverChannel4, clientSocketChannelProviderMap1, clientSocketChannelProviderMap2, clientSocketChannelProviderMap3);

    }

    @After
    public void tearDown() {

        // todo fix close, it not blocking ( in other-words we should wait till everything is closed before running the next test)

        for (ServerSocketChannel socketChannel : new ServerSocketChannel[]{serverChannel1, serverChannel2, serverChannel3, serverChannel4}) {
            try {
                socketChannel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }


    @Test
    public void test() throws IOException, InterruptedException {

        // allow all the TCP connections to establish
        Thread.sleep(500);

        map1.put(2, "EXAMPLE-1");
        map2.put(4, "EXAMPLE-2");
        map3.put(2, "EXAMPLE-1");
        map3.remove(2);


        // allow time for the recompilation to resolve
        waitTillEqual(50000000);

        assertEquals(new TreeMap(map1), new TreeMap(map2));
        assertEquals(new TreeMap(map1), new TreeMap(map3));
        assertEquals(new TreeMap(map1), new TreeMap(map4));
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
            if (new TreeMap(map1).equals(new TreeMap(map2)) &&
                    new TreeMap(map1).equals(new TreeMap(map3)) &&
                    new TreeMap(map1).equals(new TreeMap(map4)))
                break;
            Thread.sleep(1);
        }

    }

}



