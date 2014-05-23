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
import static net.openhft.collections.ReplicatedSharedHashMap.ModificationIterator;
import static net.openhft.collections.map.replicators.TcpSocketChannelEntryWriter.EntryCallback;

/**
 * @author Rob Austin.
 */
public class UdpSocketChannelEntryWriter {

    private static final Logger LOG = LoggerFactory.getLogger(UdpSocketChannelEntryWriter.class);

    private final ByteBuffer out;
    private final ByteBufferBytes in;
    private final EntryCallback entryCallback;

    public UdpSocketChannelEntryWriter(final int serializedEntrySize,
                                       @NotNull final EntryExternalizable externalizable,
                                       int packetSize) {
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

        out.clear();
        in.clear();
        in.skip(2);

        final boolean wasDataRead = modificationIterator.nextEntry(entryCallback);

        if (!wasDataRead)
            return;

        // we'll write the size inverted at the start
        in.writeShort(0, ~(in.readUnsignedShort(2)));
        socketChannel.write(out);

    }


}

