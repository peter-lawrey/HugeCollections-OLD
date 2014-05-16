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

package net.openhft.collections.map.replicators;

import net.openhft.collections.ReplicatedSharedHashMap;
import net.openhft.lang.thread.NamedThreadFactory;
import org.jetbrains.annotations.NotNull;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.util.concurrent.Executors.newSingleThreadExecutor;

/**
 * Used with a {@see net.openhft.collections.ReplicatedSharedHashMap} to send data between the maps using a socket connection
 * <p/>
 * {@see net.openhft.collections.OutSocketReplicator}
 *
 * @author Rob Austin.
 */
public class ClientTcpSocketReplicator {

    private static final Logger LOGGER = Logger.getLogger(ClientTcpSocketReplicator.class.getName());

    public static class ClientPort {
        final String host;
        final int port;

        public ClientPort(int port, String host) {
            this.host = host;
            this.port = port;
        }

        @Override
        public String toString() {
            return "host=" + host +
                    ", port=" + port;

        }
    }

    /**
     * todo doc
     *
     * @param entryReader
     * @param socketChannelEntryWriter
     * @param map
     */
    public ClientTcpSocketReplicator(@NotNull final ClientPort clientPort,
                                     @NotNull final SocketChannelEntryReader entryReader,
                                     @NotNull final SocketChannelEntryWriter socketChannelEntryWriter,
                                     @NotNull final ReplicatedSharedHashMap map) {

        newSingleThreadExecutor(new NamedThreadFactory("InSocketReplicator-" + map.getIdentifier(), true)).execute(new Runnable() {

            @Override
            public void run() {
                try {

                    SocketChannel socketChannel;

                    for (; ; ) {
                        try {

                            socketChannel = SocketChannel.open(new InetSocketAddress(clientPort.host, clientPort.port));
                            LOGGER.info("successfully connected to " + clientPort);
                            socketChannel.socket().setReceiveBufferSize(8 * 1024);
                            break;

                        } catch (ConnectException e) {

                            // todo add better back off logic
                            Thread.sleep(100);
                        }
                    }

                    socketChannelEntryWriter.sendWelcomeMessage(socketChannel, map.lastModification(), map.getIdentifier());
          //          final SocketChannelEntryReader.WelcomeMessage welcomeMessage = entryReader.readWelcomeMessage(socketChannel);

                    for (; ; ) {
                        entryReader.readAll(socketChannel);
                    }

                } catch (Exception e) {
                    LOGGER.log(Level.SEVERE, "", e);
                }
            }

        });
    }


}
