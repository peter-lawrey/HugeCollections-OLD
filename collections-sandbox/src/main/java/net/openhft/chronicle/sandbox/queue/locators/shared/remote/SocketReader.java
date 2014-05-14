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

package net.openhft.chronicle.sandbox.queue.locators.shared.remote;

import net.openhft.chronicle.sandbox.queue.locators.shared.Index;
import net.openhft.chronicle.sandbox.queue.locators.shared.OffsetProvider;
import net.openhft.chronicle.sandbox.queue.locators.shared.remote.channel.provider.SocketChannelProvider;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;

/**
 * Starts a thread and reads of the socket
 */
public class SocketReader implements Runnable {

    public static final int RECEIVE_BUFFER_SIZE = 256 * 1024;
    private static Logger LOG = LoggerFactory.getLogger(SocketReader.class);
    private final Index ringIndex;

    @NotNull
    private final ByteBuffer targetBuffer;

    @NotNull
    private final OffsetProvider offsetProvider;
    @NotNull
    private final SocketChannelProvider socketChannelProvider;
    @NotNull
    private final String name;

    // use one buffer for
    private final ByteBuffer buffer = ByteBuffer.allocateDirect(RECEIVE_BUFFER_SIZE).order(ByteOrder.nativeOrder());
    private final ByteBuffer rbuffer = buffer.slice().order(ByteOrder.nativeOrder());
    private final ByteBuffer wbuffer = buffer.slice().order(ByteOrder.nativeOrder());


    /**
     * @param ringIndex
     * @param targetBuffer          the buffer that supports the offset provider
     * @param offsetProvider        the location into the buffer for an index location
     * @param socketChannelProvider
     * @param name
     */
    public SocketReader(@NotNull final Index ringIndex,
                        @NotNull final ByteBuffer targetBuffer,
                        @NotNull final OffsetProvider offsetProvider,
                        @NotNull final SocketChannelProvider socketChannelProvider,
                        @NotNull String name) {
        this.ringIndex = ringIndex;
        this.offsetProvider = offsetProvider;
        this.socketChannelProvider = socketChannelProvider;
        this.name = name;
        this.targetBuffer = targetBuffer.slice();
    }


    @Override
    public void run() {


        try {
            final SocketChannel socketChannel = socketChannelProvider.getSocketChannel();

            wbuffer.clear();

            for (; ; ) {

                rbuffer.clear();
                rbuffer.limit(0);

                // read an int from the socket
                while (wbuffer.position() < 4) {
                    int len = socketChannel.read(wbuffer);
                    if (len < 0) throw new EOFException();
                }

                rbuffer.limit(wbuffer.position());
                int intValue = rbuffer.getInt();

                // if this int is negative then we are using it to demote and writerLocation change
                if (intValue <= 0) {
                    // the locations have been -'ed
                    final int index = -intValue;
                    ringIndex.setNextLocation(index);
                } else {

                    int endOfMessageOffset = intValue + 4;

                    while (wbuffer.position() < endOfMessageOffset) {
                        int len = socketChannel.read(wbuffer);
                        if (len < 0) throw new EOFException();
                    }

                    // to allow the target buffer to read uo to the end of the message
                    rbuffer.limit(endOfMessageOffset);

                    int offset = offsetProvider.getOffset(ringIndex.getWriterLocation());
                    targetBuffer.position(offset);

                    ///  LOG.info("reading byte=" + ByteUtils.toString(rbuffer));
                    targetBuffer.put(rbuffer);

                }

                wbuffer.flip();
                wbuffer.position(rbuffer.position());
                wbuffer.compact();

            }

        } catch (Exception e) {
            LOG.warn("", e);
        }
    }

    @Override
    public String toString() {
        return "SocketReader{" +
                "name='" + name + '\'' +
                '}';
    }
}
