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

    private final int entryMaxSize;
    private final ByteBuffer byteBuffer;
    private final ByteBufferBytes bytes;
    private final EntryExternalizable externalizable;
    private final EntryCallback entryCallback = new EntryCallback();

    private static final Logger LOG = LoggerFactory.getLogger(VanillaSharedReplicatedHashMap.class);

    public SocketChannelEntryWriter(final int entryMaxSize,
                                    final short maxNumberOfEntriesPerChunk,
                                    @NotNull final EntryExternalizable externalizable) {
        this.entryMaxSize = entryMaxSize;
        byteBuffer = ByteBuffer.allocateDirect(entryMaxSize * maxNumberOfEntriesPerChunk);
        bytes = new ByteBufferBytes(byteBuffer);
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

        final long start = bytes.position();

        // if we still have some unwritten writer from last time
        if (bytes.position() > 0)
            writeBytes(socketChannel);

        //todo if writer.position() ==0 it would make sense to call a blocking version of modificationIterator.nextEntry(entryCallback);
        for (; ; ) {

            final boolean wasDataRead = modificationIterator.nextEntry(entryCallback);

            if (!wasDataRead && bytes.position() == start)
                return;

            if (bytes.remaining() > entryMaxSize && (wasDataRead || bytes.position() == start))
                continue;

            writeBytes(socketChannel);

            // we've filled up one writer lets give another channel a chance to send data
            return;
        }

    }

    private void writeBytes(@NotNull final SocketChannel socketChannel) throws IOException {

        byteBuffer.limit((int) bytes.position());

        final int write = socketChannel.write(byteBuffer);

        if (LOG.isDebugEnabled())
            LOG.debug("bytes-written=" + write);

        if (byteBuffer.remaining() == 0) {
            byteBuffer.clear();
            bytes.clear();
        } else {
            byteBuffer.compact();
            byteBuffer.flip();
            bytes.limit(bytes.capacity());
            bytes.position(byteBuffer.limit());
            byteBuffer.limit(byteBuffer.capacity());
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

            bytes.skip(2);
            final long start = (int) bytes.position();
            externalizable.writeExternalEntry(entry, bytes);

            if (bytes.position() - start == 0) {
                bytes.position(bytes.position() - 2);
                return false;
            }

            // write the length of the entry, just before the start, so when we read it back
            // we read the length of the entry first and hence know how many preceding writer to read
            final int entrySize = (int) (bytes.position() - start);
            bytes.writeUnsignedShort(start - 2L, entrySize);

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

    public void sendBootstrap(@NotNull final SocketChannel socketChannel,
                              final long timeStampOfLastMessage,
                              final int localIdentifier) throws IOException {
        bytes.clear();
        byteBuffer.clear();

        // send a welcome message to the remote server to ask for data for our localIdentifier
        // and any missed messages
        bytes.writeByte(localIdentifier);
        bytes.writeLong(timeStampOfLastMessage);
        byteBuffer.limit((int) bytes.position());

        socketChannel.write(byteBuffer);

        if (byteBuffer.remaining() == 0) {
            byteBuffer.clear();
            bytes.clear();
        } else {
            byteBuffer.compact();
            byteBuffer.flip();
            bytes.limit(bytes.capacity());
            byteBuffer.limit(byteBuffer.capacity());
        }

    }

}

