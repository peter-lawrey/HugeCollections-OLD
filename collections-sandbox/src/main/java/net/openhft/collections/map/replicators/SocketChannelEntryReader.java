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
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * Reads map entries from a socket, this could be a client or server socket
 *
 * @author Rob Austin.
 */
public class SocketChannelEntryReader {

    public static final short MAX_NUMBER_OF_ENTRIES_PER_BUFFER = 128;
    private ReplicatedSharedHashMap.EntryExternalizable externalizable;

    //todo HCOLL-71 fix the 128 padding
    final int entrySize0;
    final ByteBuffer byteBuffer;
    final ByteBufferBytes bytes;

    private long sizeOfNextEntry = Long.MIN_VALUE;

    public SocketChannelEntryReader(int entrySize, ReplicatedSharedHashMap.EntryExternalizable externalizable) {
        this.entrySize0 = entrySize;
        byteBuffer = ByteBuffer.allocate(entrySize0 * MAX_NUMBER_OF_ENTRIES_PER_BUFFER);
        this.externalizable = externalizable;
        bytes = new ByteBufferBytes(byteBuffer);
        bytes.limit(0);

        // read  remoteIdentifier and time stamp
        byteBuffer.clear();


    }

    /**
     * reads entries from the socket till it is empty
     *
     * @param socketChannel
     * @throws IOException
     * @throws InterruptedException
     */
    void readAll(@NotNull final SocketChannel socketChannel) throws IOException, InterruptedException {

        for (; ; ) {
            // its set to MIN_VALUE when it should be read again
            if (sizeOfNextEntry == Long.MIN_VALUE) {
                if (bytes.remaining() < 8) {
                    socketChannel.read(byteBuffer);
                    bytes.limit(byteBuffer.position());
                    if (bytes.remaining() < 8)
                        return;
                }

                sizeOfNextEntry = bytes.readUnsignedShort();
            }

            if (sizeOfNextEntry <= 0)
                throw new IllegalStateException("invalid entrySize=" + sizeOfNextEntry);

            if (bytes.remaining() < sizeOfNextEntry) {
                socketChannel.read(byteBuffer);
                bytes.limit(byteBuffer.position());
                if (bytes.remaining() < sizeOfNextEntry)
                    return;
            }

            final long limit = bytes.position() + sizeOfNextEntry;
            bytes.limit(limit);
            externalizable.readExternalEntry(bytes);

            // skip onto the next entry
            bytes.position(limit);

            if (byteBuffer.position() > 0 && byteBuffer.remaining() <= entrySize0) {
                byteBuffer.compact();
                bytes.position(0);
            }

            bytes.limit(byteBuffer.position());

            // to allow the sizeOfNextEntry to be read the next time around
            sizeOfNextEntry = Long.MIN_VALUE;
        }

    }


    static class Bootstrap {
        public final long timeStamp;
        public final byte identifier;

        Bootstrap(byte identifier, long timeStamp) {
            this.timeStamp = timeStamp;
            this.identifier = identifier;
        }
    }

    Bootstrap readWelcomeMessage(SocketChannel channel) throws IOException {

        // read from the channel the timestamp and identifier
        while (bytes.remaining() < 9) {
            channel.read(byteBuffer);
            bytes.limit(byteBuffer.position());
        }

        return new Bootstrap(bytes.readByte(), bytes.readLong());

    }
}
