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
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
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

    private static final Logger LOG = Logger.getLogger(ClientTcpSocketReplicator.class.getName());

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


    public ClientTcpSocketReplicator(@NotNull final ClientPort clientPort,
                                     @NotNull final SocketChannelEntryReader socketChannelEntryReader,
                                     @NotNull final SocketChannelEntryWriter socketChannelEntryWriter,
                                     @NotNull final ReplicatedSharedHashMap map) {


        newSingleThreadExecutor(new NamedThreadFactory("InSocketReplicator-", true)).execute(new Runnable() {

            @Override
            public void run() {
                try {

                    SocketChannel socketChannel;

                    for (; ; ) {
                        try {
                            socketChannel = SocketChannel.open(new InetSocketAddress(clientPort.host, clientPort.port));
                            LOG.info("successfully connected to " + clientPort);


                            socketChannel.socket().setReceiveBufferSize(8 * 1024);
                            break;
                        } catch (ConnectException e) {

                            // todo add better backoff logic
                            Thread.sleep(100);
                        }
                    }


                    socketChannelEntryWriter.sendWelcomeMessage(socketChannel, map.lastModification(), map.getIdentifier());
                    final SocketChannelEntryReader.WelcomeMessage welcomeMessage = socketChannelEntryReader.readWelcomeMessage(socketChannel);

                    final ReplicatedSharedHashMap.ModificationIterator remoteModificationIterator = map.getModificationIterator(welcomeMessage.identifier);
                    remoteModificationIterator.dirtyEntriesFrom(welcomeMessage.timeStamp);

                    // we start this connection in blocking mode ( to do the hand-shacking ) , then move it to non-blocking
                    socketChannel.configureBlocking(false);

                    final Selector selector = Selector.open();
                    socketChannel.register(selector, SelectionKey.OP_READ);

                    while (true) {
                        // this may block for a long time, upon return the
                        // selected set contains keys of the ready channels
                        int n = selector.select();

                        if (n == 0) {
                            continue;    // nothing to do
                        }

                        // get an iterator over the set of selected keys
                        final Iterator it = selector.selectedKeys().iterator();

                        // look at each key in the selected set
                        while (it.hasNext()) {
                            SelectionKey key = (SelectionKey) it.next();

                            // is there data to read on this channel?
                            if (key.isReadable()) {
                                socketChannelEntryReader.readAll(socketChannel);
                            } else if (key.isWritable()) {
                                socketChannelEntryWriter.writeAll(socketChannel, remoteModificationIterator);
                            }

                            // remove key from selected set, it's been handled
                            it.remove();
                        }
                    }


                } catch (Exception e) {
                    LOG.log(Level.SEVERE, "", e);
                }
            }

        });
    }


}
