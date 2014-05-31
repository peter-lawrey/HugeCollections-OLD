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
import net.openhft.lang.io.NativeBytes;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static net.openhft.collections.ReplicatedSharedHashMap.EntryExternalizable;
import static net.openhft.collections.ReplicatedSharedHashMap.ModificationIterator;

/**
 * @author Rob Austin.
 */
class TcpSocketChannelEntryWriter {

    private static final Logger LOG = LoggerFactory.getLogger(TcpSocketChannelEntryWriter.class);

    private final ByteBuffer out;
    private final ByteBufferBytes in;
    private final EntryCallback entryCallback;
    private final int serializedEntrySize;

    TcpSocketChannelEntryWriter(final int serializedEntrySize,
                                @NotNull final EntryExternalizable externalizable,
                                int packetSize) {
        this.serializedEntrySize = serializedEntrySize;
        out = ByteBuffer.allocateDirect(packetSize + serializedEntrySize);
        in = new ByteBufferBytes(out);
        entryCallback = new EntryCallback(externalizable, in);
    }


    /**
     * writes the timestamp into the buffer
     *
     * @param localIdentifier the current nodes identifier
     * @throws IOException if it failed to send
     */
    void identifierToBuffer(final int localIdentifier) throws IOException {
        in.writeByte(localIdentifier);
    }

    /**
     * sends the identity and timestamp of this node to a remote node
     *
     * @param timeStampOfLastMessage the last timestamp we received a message from that node
     * @throws IOException if it failed to send
     */
    void timestampToBuffer(final long timeStampOfLastMessage) throws IOException {
        in.writeLong(timeStampOfLastMessage);
    }

    /**
     * writes all the entries that have changed, to the tcp socket
     *
     * @param modificationIterator
     * @throws InterruptedException
     * @throws java.io.IOException
     */
    void entriesToBuffer(@NotNull final ModificationIterator modificationIterator) throws InterruptedException, IOException {

        final long start = in.position();

        for (; ; ) {

            final boolean wasDataRead = modificationIterator.nextEntry(entryCallback);

            // if there was no data written to the buffer and we have not written any more data to
            // the buffer, then give up
            if (!wasDataRead && in.position() == start)
                return;

            // if we have space in the buffer to write more data and we just wrote data into the
            // buffer then let try and write some more, else if we failed to just write data
            // {@code wasDataRead} then we will send what we have
            if (in.remaining() > serializedEntrySize && wasDataRead)
                continue;

            // we've filled up one writer lets give another channel a chance to send data
            return;
        }

    }

    /**
     * writes the contents of the buffer to the socket
     *
     * @param socketChannel the socket to publish the buffer to
     * @throws IOException
     */
    public void writeBufferToSocket(SocketChannel socketChannel) throws IOException {
        // if we still have some unwritten writer from last time
        if (in.position() > 0) {

            out.limit((int) in.position());

            final int len = socketChannel.write(out);

            if (LOG.isDebugEnabled())
                LOG.debug("bytes-written=" + len);

            if (out.remaining() == 0) {
                out.clear();
                in.clear();
            } else {
                out.compact();
                in.position(out.position());
                in.limit(in.capacity());
                out.clear();
            }
        }
    }


    /**
     * {@inheritDoc}
     */
    static class EntryCallback extends VanillaSharedReplicatedHashMap.EntryCallback {

        private final EntryExternalizable externalizable;
        private final ByteBufferBytes in;

        EntryCallback(@NotNull final EntryExternalizable externalizable, @NotNull final ByteBufferBytes in) {
            this.externalizable = externalizable;
            this.in = in;
        }

        @Override
        public boolean onEntry(final NativeBytes entry) {

            in.skip(2);
            final long start = (int) in.position();
            externalizable.writeExternalEntry(entry, in);

            if (in.position() - start == 0) {
                in.position(in.position() - 2);
                return false;
            }

            // write the length of the entry, just before the start, so when we read it back
            // we read the length of the entry first and hence know how many preceding writer to read
            final int entrySize = (int) (in.position() - start);
            in.writeUnsignedShort(start - 2L, entrySize);

            return true;
        }
    }


}

