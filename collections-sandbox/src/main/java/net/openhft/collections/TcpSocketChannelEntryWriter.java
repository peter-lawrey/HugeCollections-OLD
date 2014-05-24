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
     * writes all the entries that have changed, to the tcp socket
     *
     * @param socketChannel
     * @param modificationIterator
     * @throws InterruptedException
     * @throws java.io.IOException
     */
    void writeAll(@NotNull final SocketChannel socketChannel,
                  @NotNull final ModificationIterator modificationIterator) throws InterruptedException, IOException {


        // if we still have some unwritten writer from last time
        if (in.position() > 0)
            writeBytes(socketChannel);

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

            writeBytes(socketChannel);

            // we've filled up one writer lets give another channel a chance to send data
            return;
        }

    }

    private void writeBytes(@NotNull final SocketChannel socketChannel) throws IOException {

        out.limit((int) in.position());

        final int write = socketChannel.write(out);

        if (LOG.isDebugEnabled())
            LOG.debug("bytes-written=" + write);

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


    /**
     * sends the identity and timestamp of this node to a remote node
     *
     * @param socketChannel   the socketChannel the message will be sent on
     * @param localIdentifier the current nodes identifier
     * @throws IOException if it failed to send
     */
    void sendIdentifier(@NotNull final SocketChannel socketChannel,
                        final int localIdentifier) throws IOException {
        in.clear();
        out.clear();

        // send a welcome message to the remote server to ask for data for our localIdentifier
        // and any missed messages
        in.writeByte(localIdentifier);
        out.limit((int) in.position());

        while (out.remaining() > 0) {
            socketChannel.write(out);
        }

        out.clear();
        in.clear();
    }

    /**
     * sends the identity and timestamp of this node to a remote node
     *
     * @param socketChannel          the socketChannel the message will be sent on
     * @param timeStampOfLastMessage the last timestamp we received a message from that node
     * @throws IOException if it failed to send
     */
    void sendTimestamp(@NotNull final SocketChannel socketChannel,
                       final long timeStampOfLastMessage) throws IOException {
        in.clear();
        out.clear();

        // send a welcome message to the remote server to ask for data for our localIdentifier
        // and any missed messages
        in.writeLong(timeStampOfLastMessage);
        out.limit((int) in.position());

        while (out.remaining() > 0) {
            socketChannel.write(out);
        }

        out.clear();
        in.clear();
    }
}

