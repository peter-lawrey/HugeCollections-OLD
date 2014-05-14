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

/**
 * Test  VanillaSharedReplicatedHashMap where the Replicated is over a TCP Socket
 *
 * @author Rob Austin.
 */
/*
public class TCPSocketReplicationTest {


    private SharedHashMap<Integer, CharSequence> map1;
    private SharedHashMap<Integer, CharSequence> map2;
    private SocketChannelProvider clientSocketChannelProvider2;
    private SocketChannelProvider clientSocketChannelProvider1;
    private ServerSocketChannel serverChannel1;
    private ServerSocketChannel serverChannel2;


    VanillaSharedReplicatedHashMap<Integer, CharSequence> newSocketShmIntString(
            final int size,
            final byte identifier,
            final int serverPort, final byte[] externalIdentifiers, final ServerSocketChannel serverChannel,
            @NotNull final SocketChannelProvider... clientSocketChannelProviders) throws IOException {

        final VanillaSharedReplicatedHashMapBuilder builder =
                new VanillaSharedReplicatedHashMapBuilder()
                        .entries(size)
                        .externalIdentifiers(externalIdentifiers)
                        .identifier(identifier);

        final VanillaSharedReplicatedHashMap<Integer, CharSequence> result =
                builder.create(getPersistenceFile(), Integer.class, CharSequence.class);

        for (SocketChannelProvider clientSocketChannelProvider : clientSocketChannelProviders) {
            new InTcpSocketReplicator(identifier, builder.entrySize(), clientSocketChannelProvider, result);
        }


        // the server will connect to all the clients, the clients will initiate the connection
        new OutTcpSocketReplicator(
                result,
                identifier,
                builder.entrySize(),
                result,
                1024 * 8,
                serverPort, serverChannel);

        return result;

    }

    static int i;

    @Before
    public void setup() throws IOException {

        clientSocketChannelProvider2 = new ClientSocketChannelProvider(8066, "localhost");
        clientSocketChannelProvider1 = new ClientSocketChannelProvider(8067, "localhost");

        serverChannel1 = ServerSocketChannel.open();
        serverChannel2 = ServerSocketChannel.open();

        map1 = newSocketShmIntString(10000, (byte) 1, 8066, new byte[]{(byte) 2}, serverChannel1, clientSocketChannelProvider1);
        map2 = newSocketShmIntString(10000, (byte) 2, 8067, new byte[]{(byte) 1}, serverChannel2, clientSocketChannelProvider2);
    }

    @After
    public void tearDown() {

        // todo fix close, it not blocking ( in other-words we should wait till everything is closed before running the next test)

        try {
            clientSocketChannelProvider1.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            clientSocketChannelProvider2.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            serverChannel1.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            serverChannel2.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

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


    */
/**
     * waits until map1 and map2 show the same value
     *
     * @param timeOutMs timeout in milliseconds
     * @throws InterruptedException
     *//*

    private void waitTillEqual(final int timeOutMs) throws InterruptedException {
        int t = 0;
        for (; t < timeOutMs; t++) {
            if (new TreeMap(map1).equals(new TreeMap(map2)))
                break;
            Thread.sleep(1);
        }

    }

}
*/


