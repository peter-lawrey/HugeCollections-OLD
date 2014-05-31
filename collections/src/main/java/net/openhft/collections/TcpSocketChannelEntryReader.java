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

import net.openhft.lang.io.ByteBufferBytes;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static net.openhft.collections.ReplicatedSharedHashMap.EntryExternalizable;

/**
 * Reads map entries from a socket, this could be a client or server socket
 *
 * @author Rob Austin.
 */
class TcpSocketChannelEntryReader {

    private static final Logger LOG = LoggerFactory.getLogger(TcpSocketChannelEntryReader.class);

    public static final int SIZE_OF_UNSIGNED_SHORT = 2;
    private final EntryExternalizable externalizable;
    private final int serializedEntrySize;
    private final ByteBuffer in;
    private final ByteBufferBytes out;

    // we use Integer.MIN_VALUE as N/A
    private int sizeOfNextEntry = Integer.MIN_VALUE;

    /**
     * @param serializedEntrySize the maximum size of an entry include the meta data
     * @param externalizable      supports reading and writing serialize entries
     * @param packetSize          the estimated size of a tcp/ip packet
     */
    TcpSocketChannelEntryReader(final int serializedEntrySize,
                                @NotNull final EntryExternalizable externalizable,
                                final short packetSize) {
        this.serializedEntrySize = serializedEntrySize;
        in = ByteBuffer.allocate(packetSize + serializedEntrySize);
        this.externalizable = externalizable;
        out = new ByteBufferBytes(in);
        out.limit(0);
        in.clear();
    }

    /**
     * reads from the socket and writes the contents to the buffer
     *
     * @param socketChannel the  socketChannel to read from
     * @return the number of bytes read
     * @throws IOException
     * @throws InterruptedException
     */
    int readSocketToBuffer(@NotNull final SocketChannel socketChannel) throws IOException, InterruptedException {
        compactBuffer();
        final int len = socketChannel.read(in);
        out.limit(in.position());
        return len;
    }

    /**
     * reads entries from the socket till it is empty
     *
     * @throws IOException
     * @throws InterruptedException
     */
    void entriesFromBuffer() throws IOException, InterruptedException {

        for (; ; ) {

            out.limit(in.position());

            // its set to MIN_VALUE when it should be read again
            if (sizeOfNextEntry == Integer.MIN_VALUE) {
                if (out.remaining() < SIZE_OF_UNSIGNED_SHORT) {
                    return;
                }

                sizeOfNextEntry = out.readUnsignedShort();
            }

            if (sizeOfNextEntry <= 0)
                throw new IllegalStateException("invalid serializedEntrySize=" + sizeOfNextEntry);

            if (out.remaining() < sizeOfNextEntry) {
                return;
            }

            final long nextEntryPos = out.position() + sizeOfNextEntry;
            final long limit = out.limit();
            out.limit(nextEntryPos);
            externalizable.readExternalEntry(out);

            out.limit(limit);
            // skip onto the next entry
            out.position(nextEntryPos);

            // to allow the sizeOfNextEntry to be read the next time around
            sizeOfNextEntry = Integer.MIN_VALUE;
        }

    }

    /**
     * compacts the buffer and updates the {@code in} and  {@code out} accordingly
     */
    private void compactBuffer() {

        // the serializedEntrySize used here may not be the maximum size of the entry in its serialized form
        // however, its only use as an indication that the buffer is becoming full and should be compacted
        // the buffer can be compacted at any time
        if (in.position() == 0 || in.remaining() > serializedEntrySize)
            return;

        in.limit(in.position());
        in.position((int) out.position());

        in.compact();
        out.position(0);
    }


    /**
     * @return -1 if unsuccessful
     */
    byte identifierFromBuffer() {
        return (out.remaining() >= 1) ? out.readByte() : Byte.MIN_VALUE;
    }


    long timeStampFromBuffer() throws IOException {
        return (out.remaining() >= 8) ? out.readLong() : Long.MIN_VALUE;
    }
}
