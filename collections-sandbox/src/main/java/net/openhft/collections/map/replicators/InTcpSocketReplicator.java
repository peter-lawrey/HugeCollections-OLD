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

import net.openhft.chronicle.sandbox.queue.locators.shared.remote.channel.provider.SocketChannelProvider;
import net.openhft.collections.ReplicatedSharedHashMap;
import net.openhft.lang.io.ByteBufferBytes;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ThreadFactory;
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
public class InTcpSocketReplicator implements Closeable {

    private static final Logger LOGGER = Logger.getLogger(InTcpSocketReplicator.class.getName());
    public static final short MAX_NUMBER_OF_ENTRIES_PER_BUFFER = 128;

    @NotNull
    private final SocketChannelProvider socketChannelProvider;

    /**
     * todo doc
     *
     * @param localIdentifier
     * @param entrySize
     * @param socketChannelProvider provides the SocketChannel
     * @param externalizable        provides the ability to read and write entries from the map
     */
    public InTcpSocketReplicator(final byte localIdentifier,
                                 final int entrySize,
                                 @NotNull final SocketChannelProvider socketChannelProvider,
                                 final ReplicatedSharedHashMap.EntryExternalizable externalizable) {

        this.socketChannelProvider = socketChannelProvider;

        //todo HCOLL-71 fix the 128 padding
        final int entrySize0 = entrySize + 128;

        final ByteBuffer byteBuffer = ByteBuffer.allocate(entrySize0 * MAX_NUMBER_OF_ENTRIES_PER_BUFFER);
        final ByteBufferBytes bytes = new ByteBufferBytes(byteBuffer);

        newSingleThreadExecutor(new ThreadFactory() {

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "InSocketReplicator-" + localIdentifier);
            }

        }).execute(new Runnable() {

            @Override
            public void run() {

                // this is used in nextEntry() below, its what could be described as callback method
                SocketChannel socketChannel = null;

                try {

                    socketChannel = socketChannelProvider.getSocketChannel();

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
                                Thread.sleep(1);
                        }

                        final long entrySize = bytes.readStopBit();
                        if (entrySize <= 0)
                            throw new IllegalStateException("invalid entrySize=" + entrySize);

                        while (bytes.remaining() < entrySize) {
                            final int read = socketChannel.read(byteBuffer);
                            bytes.limit(byteBuffer.position());
                            if (read == 0)
                                Thread.sleep(1);
                        }

                        final long limit = bytes.position() + entrySize;
                        bytes.limit(limit);
                        externalizable.readExternalEntry(bytes);

                        // skip onto the next entry
                        bytes.position(limit);
                    }
                } catch (Exception e1) {
                    if (socketChannel != null && socketChannel.isOpen())
                        LOGGER.log(Level.SEVERE, "", e1);
                }

            }

        });
    }

    @Override
    public void close() throws IOException {
        socketChannelProvider.close();
    }
}
