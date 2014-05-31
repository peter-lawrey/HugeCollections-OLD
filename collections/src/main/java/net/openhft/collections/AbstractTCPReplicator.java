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

    protected void doRead(ReplicatedSharedHashMap map, SelectionKey key) throws IOException, InterruptedException {
        final SocketChannel socketChannel = (SocketChannel) key.channel();
        final Attached a = (Attached) key.attachment();

        a.entryReader.compact();
        int len = a.entryReader.read(socketChannel);

        if (len > 0) {

            if (a.isHandShakingComplete())
                a.entryReader.readAll(socketChannel);
            else
                doHandShaking(map, socketChannel, a);
        }
    }

    private void doHandShaking(ReplicatedSharedHashMap map, SocketChannel socketChannel, Attached a) throws IOException, InterruptedException {
        if (a.remoteIdentifier == Byte.MIN_VALUE) {

            final byte remoteIdentifier = a.entryReader.readIdentifier();

            if (remoteIdentifier != Byte.MIN_VALUE) {
                a.remoteIdentifier = remoteIdentifier;
                sendTimeStamp(map, a.entryWriter, a, remoteIdentifier);
            }
        }


        if (a.remoteIdentifier != Byte.MIN_VALUE && a.remoteTimestamp == Long
                .MIN_VALUE) {
            a.remoteTimestamp = a.entryReader.readTimeStamp();
            if (a.remoteTimestamp != Long.MIN_VALUE) {
                a.remoteModificationIterator.dirtyEntries(a.remoteTimestamp);
                a.setHandShakingComplete();
                a.entryReader.readAll(socketChannel);
            }
        }
    }

    protected void doWrite(SelectionKey key) throws InterruptedException, IOException {
        final SocketChannel socketChannel = (SocketChannel) key.channel();
        final Attached a = (Attached) key.attachment();

        if (a.remoteModificationIterator != null)
            a.entryWriter.writeAll(socketChannel,
                    a.remoteModificationIterator);

        a.entryWriter.sendAll(socketChannel);
    }


    void sendTimeStamp(ReplicatedSharedHashMap map,
                       TcpSocketChannelEntryWriter entryWriter,
                       Attached a, byte remoteIdentifier) throws IOException {
        LOG.info("remoteIdentifier=" + remoteIdentifier);
        if (LOG.isDebugEnabled()) {
            // Pre-check prevents autoboxing of identifiers, i. e. garbage creation.
            // Subtle gain, but.
            LOG.debug("server-connection id={}, remoteIdentifier={}",
                    map.identifier(), remoteIdentifier);
        }

        if (remoteIdentifier == map.identifier())
            throw new IllegalStateException("Non unique identifiers id=" + map.identifier());
        a.remoteModificationIterator =
                map.acquireModificationIterator(remoteIdentifier);

        entryWriter.writeTimestamp(map.lastModificationTime(remoteIdentifier));
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
