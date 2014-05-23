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
import net.openhft.lang.thread.NamedThreadFactory;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static net.openhft.collections.ReplicatedSharedHashMap.EntryExternalizable;
import static net.openhft.collections.ReplicatedSharedHashMap.ModificationIterator;

/**
 * Used with a {@see net.openhft.collections.ReplicatedSharedHashMap} to send data between the
 * maps using na nio, non blocking, server socket connection
 * <p/>
 * {@see net.openhft.collections.InSocketReplicator}
 *
 * @author Rob Austin.
 */
public class UdpWriteSocketReplicator implements Closeable {

    private static final Logger LOG =
            LoggerFactory.getLogger(UdpWriteSocketReplicator.class.getName());

    private final ReplicatedSharedHashMap map;
    private final int port;
    private final ServerSocketChannel serverChannel;
    private final UdpSocketChannelEntryWriter socketChannelEntryWriter;
    private final byte localIdentifier;
    private final EntryExternalizable externalizable;
    private final int serializedEntrySize;

    private short packetSize;
    private final ExecutorService executorService;

    public UdpWriteSocketReplicator(@NotNull final ReplicatedSharedHashMap map,
                                    @NotNull final EntryExternalizable externalizable,
                                    final int port,
                                    @NotNull final UdpSocketChannelEntryWriter socketChannelEntryWriter,
                                    final short packetSize,
                                    final int serializedEntrySize) throws IOException {

        this.externalizable = externalizable;
        this.map = map;
        this.port = port;
        this.serverChannel = ServerSocketChannel.open();
        this.localIdentifier = map.getIdentifier();
        this.socketChannelEntryWriter = socketChannelEntryWriter;
        this.serializedEntrySize = serializedEntrySize;
        this.packetSize = packetSize;

        // out bound
        executorService = newSingleThreadExecutor(new NamedThreadFactory("OutSocketReplicator-" + localIdentifier, true));

        executorService.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    UdpWriteSocketReplicator.this.packetSize = packetSize;
                    process();
                } catch (Exception e) {
                    LOG.error("", e);
                }
            }

        });
    }


    @Override
    public void close() throws IOException {
        executorService.shutdownNow();

        if (serverChannel != null)
            serverChannel.close();
    }

    /**
     * binds to the server socket and process data
     * This method will block until interrupted
     *
     * @throws Exception
     */
    private void process() throws Exception {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Listening on port " + port);
        }

        Selector selector = Selector.open();
        DatagramChannel channel = DatagramChannel.open();

        // set the port the process channel will listen to
        channel.bind(new InetSocketAddress(port));

        // set non-blocking mode for the listening socket
        this.serverChannel.configureBlocking(false);

        // register the ServerSocketChannel with the Selector
        this.serverChannel.register(selector, SelectionKey.OP_WRITE);

        for (; ; ) {
            // this may block for a long time, upon return the
            // selected set contains keys of the ready channels
            final int n = selector.select();

            if (n == 0) {
                continue;    // nothing to do
            }

            // get an iterator over the set of selected keys
            final Iterator it = selector.selectedKeys().iterator();

            // look at each key in the selected set
            while (it.hasNext()) {
                SelectionKey key = (SelectionKey) it.next();

                // remove key from selected set, it's been handled
                it.remove();

                try {

                    if (key.isValid()) {
                        continue;
                    }

                    if (key.isWritable()) {
                        final SocketChannel socketChannel = (SocketChannel) key.channel();
                        final Attached attachment = (Attached) key.attachment();
                        socketChannelEntryWriter.writeAll(socketChannel, attachment.remoteModificationIterator);
                    }

                } catch (Exception e) {

                    if (this.serverChannel.isOpen())
                        LOG.error("", e);

                    // Close channel and nudge selector
                    try {
                        key.channel().close();
                    } catch (IOException ex) {
                        // do nothing
                    }
                }

            }
        }
    }

    private static class Attached {

        final TcpSocketChannelEntryReader tcpSocketChannelEntryReader;
        final ModificationIterator remoteModificationIterator;
        private final byte remoteIdentifier;

        private Attached(TcpSocketChannelEntryReader tcpSocketChannelEntryReader, ModificationIterator remoteModificationIterator, byte remoteIdentifier) {
            this.tcpSocketChannelEntryReader = tcpSocketChannelEntryReader;
            this.remoteModificationIterator = remoteModificationIterator;
            this.remoteIdentifier = remoteIdentifier;
        }
    }

}


