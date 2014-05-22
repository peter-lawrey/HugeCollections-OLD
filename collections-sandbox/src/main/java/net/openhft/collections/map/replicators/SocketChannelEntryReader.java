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
public class SocketChannelEntryReader {

    private static final Logger LOG = LoggerFactory.getLogger(SocketChannelEntryReader.class);

    public static final int SIZE_OF_UNSIGNED_SHORT = 4;
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
    public SocketChannelEntryReader(final int serializedEntrySize,
                                    @NotNull final EntryExternalizable externalizable,
                                    final short packetSize) {
        this.serializedEntrySize = serializedEntrySize;
        in = ByteBuffer.allocate(packetSize);
        this.externalizable = externalizable;
        out = new ByteBufferBytes(in);
        out.limit(0);
        in.clear();
    }

    /**
     * reads entries from the socket till it is empty
     *
     * @param socketChannel
     * @throws IOException
     * @throws InterruptedException
     */
    void readAll(@NotNull final SocketChannel socketChannel) throws IOException, InterruptedException {

        compact();

        for (; ; ) {

            out.limit(in.position());

            // its set to MIN_VALUE when it should be read again
            if (sizeOfNextEntry == Integer.MIN_VALUE) {
                if (out.remaining() < SIZE_OF_UNSIGNED_SHORT) {
                    socketChannel.read(in);
                    out.limit(in.position());
                    if (out.remaining() < SIZE_OF_UNSIGNED_SHORT)
                        return;
                }

                sizeOfNextEntry = out.readUnsignedShort();
            }

            if (sizeOfNextEntry <= 0)
                throw new IllegalStateException("invalid serializedEntrySize=" + sizeOfNextEntry);

            if (out.remaining() < sizeOfNextEntry) {
                socketChannel.read(in);
                out.limit(in.position());
                if (out.remaining() < sizeOfNextEntry)
                    return;
            }

            final long limit = out.position() + sizeOfNextEntry;
            out.limit(limit);
            externalizable.readExternalEntry(out);

            // skip onto the next entry
            out.position(limit);
            compact();

            // to allow the sizeOfNextEntry to be read the next time around
            sizeOfNextEntry = Integer.MIN_VALUE;
        }

    }

    /**
     * compacts the buffer and updates the {@code in} and  {@code out} accordingly
     */
    private void compact() {

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


    byte readIdentifier(@NotNull final SocketChannel channel) throws IOException {
        // read from the channel the timestamp and identifier
        while (out.remaining() < 1) {
            channel.read(in);
            out.limit(in.position());
        }

        return  out.readByte();

    }


    long readTimeStamp(@NotNull final SocketChannel channel) throws IOException {
        // read from the channel the timestamp and identifier
        while (out.remaining() < 8) {
            channel.read(in);
            out.limit(in.position());
        }

        return  out.readLong();

    }
}
