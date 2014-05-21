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

import net.openhft.collections.VanillaSharedReplicatedHashMap;
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
public class SocketChannelEntryWriter {

    private static final Logger LOG = LoggerFactory.getLogger(VanillaSharedReplicatedHashMap.class);

    private final ByteBuffer out;
    private final ByteBufferBytes in;
    private final EntryExternalizable externalizable;
    private final EntryCallback entryCallback = new EntryCallback();
    private final int serializedEntrySize;

    public SocketChannelEntryWriter(final int serializedEntrySize,
                                    @NotNull final EntryExternalizable externalizable,
                                    int packetSize) {
        this.serializedEntrySize = serializedEntrySize;
        out = ByteBuffer.allocateDirect(packetSize + serializedEntrySize);
        in = new ByteBufferBytes(out);
        this.externalizable = externalizable;
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

        final long start = in.position();

        // if we still have some unwritten writer from last time
        if (in.position() > 0)
            writeBytes(socketChannel);

        for (; ; ) {

            final boolean wasDataRead = modificationIterator.nextEntry(entryCallback);

            if (!wasDataRead && in.position() == start)
                return;

            if (in.remaining() > serializedEntrySize && (wasDataRead || in.position() == start))
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
            out.flip();
            in.limit(in.capacity());
            in.position(out.limit());
            out.limit(out.capacity());
        }
    }


    /**
     * {@inheritDoc}
     */
    class EntryCallback implements VanillaSharedReplicatedHashMap.EntryCallback {

        /**
         * {@inheritDoc}
         */
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


        /**
         * {@inheritDoc}
         */
        @Override
        public void onAfterEntry() {

        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onBeforeEntry() {

        }
    }

    /**
     * sends the identity and timestamp of this node to a remote node
     *
     * @param socketChannel          the socketChannel the message will be sent on
     * @param timeStampOfLastMessage the last timestamp we received a message from that node
     * @param localIdentifier        the current nodes identifier
     * @throws IOException if it failed to send
     */
    public void sendBootstrap(@NotNull final SocketChannel socketChannel,
                              final long timeStampOfLastMessage,
                              final int localIdentifier) throws IOException {
        in.clear();
        out.clear();

        // send a welcome message to the remote server to ask for data for our localIdentifier
        // and any missed messages
        in.writeByte(localIdentifier);
        in.writeLong(timeStampOfLastMessage);
        out.limit((int) in.position());

        socketChannel.write(out);

        if (out.remaining() == 0) {
            out.clear();
            in.clear();
        } else {
            out.compact();
            out.flip();
            in.limit(in.capacity());
            out.limit(out.capacity());
        }

    }

}

