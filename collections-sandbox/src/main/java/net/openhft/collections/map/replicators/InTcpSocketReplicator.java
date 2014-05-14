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
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.thread.NamedThreadFactory;
import org.jetbrains.annotations.NotNull;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.logging.Logger;

import static java.util.concurrent.Executors.newSingleThreadExecutor;

/**
 * Used with a {@see net.openhft.collections.ReplicatedSharedHashMap} to send data between the maps using a socket connection
 * <p/>
 * {@see net.openhft.collections.OutSocketReplicator}
 *
 * @author Rob Austin.
 */
public class InTcpSocketReplicator {

    private static final Logger LOGGER = Logger.getLogger(InTcpSocketReplicator.class.getName());
    public static final short MAX_NUMBER_OF_ENTRIES_PER_BUFFER = 128;


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
     * @param localIdentifier
     * @param entrySize
     * @param externalizable  provides the ability to read and write entries from the map
     */
    public InTcpSocketReplicator(final byte localIdentifier,
                                 final int entrySize,
                                 @NotNull final InTcpSocketReplicator.ClientPort clientPort,
                                 final ReplicatedSharedHashMap.EntryExternalizable externalizable) {


        //todo HCOLL-71 fix the 128 padding
        final int entrySize0 = entrySize + 128;

        final ByteBuffer byteBuffer = ByteBuffer.allocate(entrySize0 * MAX_NUMBER_OF_ENTRIES_PER_BUFFER);
        final ByteBufferBytes bytes = new ByteBufferBytes(byteBuffer);

        // todo this should not be set to zero but it should be the last messages timestamp
        final long timeStampOfLastMessage = 0;

        newSingleThreadExecutor(new NamedThreadFactory("InSocketReplicator-" + localIdentifier, true)).execute(new Runnable() {

            @Override
            public void run() {
                try {

                    SocketChannel socketChannel;
                    // this is used in nextEntry() below, its what could be described as callback method

                    for (; ; ) {
                        try {
                            socketChannel = SocketChannel.open(new InetSocketAddress(clientPort.host, clientPort.port));
                            LOGGER.info("successfully connected to " + clientPort);


                            socketChannel.socket().setReceiveBufferSize(8 * 1024);

                            break;
                        } catch (ConnectException e) {

                            // todo add better backoff logic
                            Thread.sleep(100);
                        }
                    }

                    //LOG.log(Level.INFO, "successfully connected to host=" + host + ", port=" + port);

                    bytes.clear();
                    byteBuffer.clear();

                    // this is notification message that we will send the remote server to ask for data for our identifier
                    bytes.writeByte(localIdentifier);
                    bytes.writeLong(timeStampOfLastMessage);
                    byteBuffer.limit((int) bytes.position());
                    socketChannel.write(byteBuffer);

                    byteBuffer.clear();
                    bytes.position(0);
                    bytes.limit(0);

                    for (; ; ) {

                        if (byteBuffer.position() > 0 && byteBuffer.remaining() <= entrySize0) {
                            byteBuffer.compact();
                            bytes.position(0);
                        }

                        bytes.limit(byteBuffer.position());

                        while (bytes.remaining() < 8) {
                            final int read = socketChannel.read(byteBuffer);
                            bytes.limit(byteBuffer.position());
                            if (read == 0)
                                Thread.sleep(100);
                        }

                        final long entrySize = bytes.readUnsignedShort();
                        if (entrySize <= 0)
                            throw new IllegalStateException("invalid entrySize=" + entrySize);

                        while (bytes.remaining() < entrySize) {
                            final int read = socketChannel.read(byteBuffer);
                            bytes.limit(byteBuffer.position());
                            if (read == 0)
                                Thread.sleep(100);
                        }

                        final long limit = bytes.position() + entrySize;
                        bytes.limit(limit);
                        externalizable.readExternalEntry(bytes);

                        // skip onto the next entry
                        bytes.position(limit);


                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        });
    }


}
