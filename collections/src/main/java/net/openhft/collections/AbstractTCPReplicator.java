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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

/**
 * @author Rob Austin.
 */
public class AbstractTCPReplicator {

    private static final Logger LOG = LoggerFactory.getLogger(TcpClientSocketReplicator.class.getName());

    /**
     * used to exchange identifiers and timestamps between the server and client
     *
     * @param map
     * @param socketChannel
     * @param attached
     * @throws IOException
     * @throws InterruptedException
     */
    private void doHandShaking(final ReplicatedSharedHashMap map,
                               final SocketChannel socketChannel,
                               final Attached attached) throws IOException, InterruptedException {

        if (attached.remoteIdentifier == Byte.MIN_VALUE) {

            final byte remoteIdentifier = attached.entryReader.identifierFromBuffer();

            if (remoteIdentifier != Byte.MIN_VALUE) {
                attached.remoteIdentifier = remoteIdentifier;
                sendTimeStamp(map, attached.entryWriter, attached, remoteIdentifier);
            }
        }

        if (attached.remoteIdentifier != Byte.MIN_VALUE &&
                attached.remoteTimestamp == Long.MIN_VALUE) {

            attached.remoteTimestamp = attached.entryReader.timeStampFromBuffer();

            if (attached.remoteTimestamp != Long.MIN_VALUE) {
                attached.remoteModificationIterator.dirtyEntries(attached.remoteTimestamp);
                attached.setHandShakingComplete();
                attached.entryReader.entriesFromBuffer();
            }
        }
    }

    protected void onWrite(final SelectionKey key) throws InterruptedException, IOException {
        final SocketChannel socketChannel = (SocketChannel) key.channel();
        final Attached attached = (Attached) key.attachment();

        if (attached.remoteModificationIterator != null)
            attached.entryWriter.entriesToBuffer(
                    attached.remoteModificationIterator);

        attached.entryWriter.writeBufferToSocket(socketChannel);
    }


    protected void onRead(final ReplicatedSharedHashMap map,
                          final SelectionKey key) throws IOException, InterruptedException {

        final SocketChannel socketChannel = (SocketChannel) key.channel();
        final Attached attached = (Attached) key.attachment();

        if (attached.entryReader.readSocketToBuffer(socketChannel) <= 0)
            return;

        if (attached.isHandShakingComplete())
            attached.entryReader.entriesFromBuffer();
        else
            doHandShaking(map, socketChannel, attached);

    }

    void sendTimeStamp(final ReplicatedSharedHashMap map,
                       final TcpSocketChannelEntryWriter entryWriter,
                       final Attached attached,
                       final byte remoteIdentifier) throws IOException {

        if (LOG.isDebugEnabled()) {
            LOG.debug("server-connection id={}, remoteIdentifier={}",
                    map.identifier(), remoteIdentifier);
        }

        if (remoteIdentifier == map.identifier())
            throw new IllegalStateException("Non unique identifiers id=" + map.identifier());

        attached.remoteModificationIterator = map.acquireModificationIterator(remoteIdentifier);

        entryWriter.timestampToBuffer(map.lastModificationTime(remoteIdentifier));
    }


    static class Attached {

        public TcpSocketChannelEntryReader entryReader;
        public ReplicatedSharedHashMap.ModificationIterator remoteModificationIterator;
        public long remoteTimestamp = Long.MIN_VALUE;
        private boolean handShakingComplete;
        public byte remoteIdentifier = Byte.MIN_VALUE;
        public TcpSocketChannelEntryWriter entryWriter;

        boolean isHandShakingComplete() {
            return handShakingComplete;
        }

        void setHandShakingComplete() {
            handShakingComplete = true;
        }

    }
}
