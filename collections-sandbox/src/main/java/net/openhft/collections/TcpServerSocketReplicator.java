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

import net.openhft.lang.thread.NamedThreadFactory;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
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
 class TcpServerSocketReplicator implements Closeable {

    private static final Logger LOG =
            LoggerFactory.getLogger(TcpServerSocketReplicator.class.getName());

    private final ReplicatedSharedHashMap map;
    private final int port;
    private final ServerSocketChannel serverChannel;
    private final TcpSocketChannelEntryWriter tcpSocketChannelEntryWriter;
    private final byte localIdentifier;
    private final EntryExternalizable externalizable;
    private final int serializedEntrySize;

    private short packetSize;
    private final ExecutorService executorService;

    public TcpServerSocketReplicator(@NotNull final ReplicatedSharedHashMap map,
                                     @NotNull final EntryExternalizable externalizable,
                                     final int port,
                                     @NotNull final TcpSocketChannelEntryWriter socketChannelEntryWriter,
                                     final short packetSize,
                                     final int serializedEntrySize) throws IOException {

        this.externalizable = externalizable;
        this.map = map;
        this.port = port;
        this.serverChannel = ServerSocketChannel.open();
        this.localIdentifier = map.getIdentifier();
        this.tcpSocketChannelEntryWriter = socketChannelEntryWriter;
        this.serializedEntrySize = serializedEntrySize;
        this.packetSize = packetSize;

        // out bound
        executorService = newSingleThreadExecutor(new NamedThreadFactory("TcpServerSocketReplicator-" + localIdentifier, true));

        executorService.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    TcpServerSocketReplicator.this.packetSize = packetSize;
                    process();
                } catch (Exception e) {
                    LOG.error("", e);
                }
            }

        });
    }


    @Override
    public void close() throws IOException {

        if (serverChannel != null) {
            serverChannel.socket().close();
            serverChannel.close();
        }

        executorService.shutdown();
    }

    /**
     * binds to the server socket and process data
     * This method will block until interrupted
     *
     * @throws Exception
     */
    private void process() throws Exception {

        if (LOG.isDebugEnabled())
            LOG.debug("Listening on port " + port);

        // allocate an unbound process socket channel

        // Get the associated ServerSocket to bind it with
        ServerSocket serverSocket = serverChannel.socket();

        // create a new Selector for use below
        Selector selector = Selector.open();

        serverSocket.setReuseAddress(true);

        // set the port the process channel will listen to
        serverSocket.bind(new InetSocketAddress(port));

        // set non-blocking mode for the listening socket
        serverChannel.configureBlocking(false);

        // register the ServerSocketChannel with the Selector
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);

        while (serverChannel.isOpen()) {
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

                // Is a new connection coming in?
                if (key.isAcceptable()) {

                    final ServerSocketChannel server = (ServerSocketChannel) key.channel();
                    final SocketChannel channel = server.accept();

                    // set the new channel non-blocking
                    channel.configureBlocking(false);

                    final TcpSocketChannelEntryReader tcpSocketChannelEntryReader = new TcpSocketChannelEntryReader(serializedEntrySize, this.externalizable, packetSize);

                    final byte remoteIdentifier = tcpSocketChannelEntryReader.readIdentifier(channel);
                    tcpSocketChannelEntryWriter.sendIdentifier(channel, localIdentifier);

                    final long remoteTimestamp = tcpSocketChannelEntryReader.readTimeStamp(channel);

                    final ModificationIterator remoteModificationIterator = map.acquireModificationIterator(remoteIdentifier);
                    remoteModificationIterator.dirtyEntries(remoteTimestamp);

                    // register it with the selector and store the ModificationIterator for this key
                    final Attached attached = new Attached(tcpSocketChannelEntryReader, remoteModificationIterator, remoteIdentifier);
                    channel.register(selector, SelectionKey.OP_WRITE | SelectionKey.OP_READ, attached);

                    if (remoteIdentifier == map.getIdentifier())
                        throw new IllegalStateException("Non unique identifiers id=" + map.getIdentifier());

                    tcpSocketChannelEntryWriter.sendTimestamp(channel, map.getLastModificationTime(remoteIdentifier));

                    if (LOG.isDebugEnabled())
                        LOG.debug("server-connection id=" + map.getIdentifier() + ", remoteIdentifier=" + remoteIdentifier);

                    // process any writer.remaining(), this can occur because reading socket for the bootstrap,
                    // may read more than just 9 writer
                    tcpSocketChannelEntryReader.readAll(channel);
                }
                try {

                    if (key.isWritable()) {
                        final SocketChannel socketChannel = (SocketChannel) key.channel();
                        final Attached attachment = (Attached) key.attachment();
                        tcpSocketChannelEntryWriter.writeAll(socketChannel, attachment.remoteModificationIterator);
                    }

                    if (key.isReadable()) {
                        final SocketChannel socketChannel = (SocketChannel) key.channel();
                        final Attached attachment = (Attached) key.attachment();
                        attachment.tcpSocketChannelEntryReader.readAll(socketChannel);
                    }

                } catch (Exception e) {

                    if (serverChannel.isOpen())
                        LOG.error("", e);

                    // Close channel and nudge selector
                    try {
                        key.channel().close();
                    } catch (IOException ex) {
                        // do nothing
                    }
                    return;
                }

                // remove key from selected set, it's been handled
                it.remove();
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


