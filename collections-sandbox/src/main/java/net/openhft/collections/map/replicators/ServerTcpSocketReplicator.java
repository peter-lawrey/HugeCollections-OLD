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
import net.openhft.collections.VanillaSharedReplicatedHashMap;
import net.openhft.lang.thread.NamedThreadFactory;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static net.openhft.collections.ReplicatedSharedHashMap.EntryExternalizable;
import static net.openhft.collections.ReplicatedSharedHashMap.ModificationIterator;
import static net.openhft.collections.map.replicators.SocketChannelEntryReader.Bootstrap;

/**
 * Used with a {@see net.openhft.collections.ReplicatedSharedHashMap} to send data between the
 * maps using na nio, non blocking, server socket connection
 * <p/>
 * {@see net.openhft.collections.InSocketReplicator}
 *
 * @author Rob Austin.
 */
public class ServerTcpSocketReplicator implements Closeable {

    private static final Logger LOG =
            Logger.getLogger(VanillaSharedReplicatedHashMap.class.getName());

    private final ReplicatedSharedHashMap map;
    private final int port;
    private final ServerSocketChannel serverChannel;
    private final SocketChannelEntryWriter socketChannelEntryWriter;
    private final byte localIdentifier;
    private final EntryExternalizable externalizable;
    private final int maxEntrySize;
    private final AtomicBoolean isClosed = new AtomicBoolean();

    public ServerTcpSocketReplicator(@NotNull final ReplicatedSharedHashMap map,
                                     @NotNull final EntryExternalizable externalizable,
                                     int port,
                                     @NotNull final SocketChannelEntryWriter socketChannelEntryWriter) throws IOException {

        this.externalizable = externalizable;
        this.map = map;
        this.port = port;
        this.serverChannel = ServerSocketChannel.open();
        this.localIdentifier = map.getIdentifier();
        this.socketChannelEntryWriter = socketChannelEntryWriter;
        this.maxEntrySize = map.maxEntrySize();


        // out bound
        newSingleThreadExecutor(new NamedThreadFactory("OutSocketReplicator-" + localIdentifier, true)).execute(new Runnable() {

            @Override
            public void run() {
                try {
                    process();
                } catch (Exception e) {
                    LOG.log(Level.SEVERE, "", e);
                }
            }

        });
    }

    public static short toMaxNumberOfEntriesPerChunk(final double packetSizeInBytes, final double entrySize) {

        final double maxNumberOfEntriesPerChunkD = packetSizeInBytes / entrySize;
        final int maxNumberOfEntriesPerChunk0 = (int) maxNumberOfEntriesPerChunkD;

        return (short) ((maxNumberOfEntriesPerChunkD != (double) ((int) maxNumberOfEntriesPerChunkD)) ?
                maxNumberOfEntriesPerChunk0 :
                maxNumberOfEntriesPerChunk0 + 1);
    }

    @Override
    public void close() throws IOException {
        isClosed.set(true);
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

        LOG.info("Listening on port " + port);

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

        while (!isClosed.get()) {
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

                    final SocketChannelEntryReader socketChannelEntryReader = new SocketChannelEntryReader(maxEntrySize, this.externalizable);
                    final Bootstrap bootstrap = socketChannelEntryReader.readBootstrap(channel);

                    final ModificationIterator remoteModificationIterator = map.acquireModificationIterator(bootstrap.identifier);
                    remoteModificationIterator.dirtyEntries(bootstrap.timeStamp);

                    // register it with the selector and store the ModificationIterator for this key
                    final Attached attached = new Attached(socketChannelEntryReader, remoteModificationIterator);
                    channel.register(selector, SelectionKey.OP_WRITE | SelectionKey.OP_READ, attached);


                    if (bootstrap.identifier == map.getIdentifier())
                        throw new IllegalStateException("Non unique identifiers id=" + map.getIdentifier());

                    // notify remote map to start to receive data for {@code localIdentifier}
                    socketChannelEntryWriter.sendBootstrap(channel, map.lastModification(), localIdentifier);

                    LOG.info("server-connection id=" + map.getIdentifier() + ", remoteIdentifier=" + bootstrap.identifier);
                }
                try {

                    if (key.isWritable()) {
                        final SocketChannel socketChannel = (SocketChannel) key.channel();
                        final Attached attachment = (Attached) key.attachment();
                        socketChannelEntryWriter.writeAll(socketChannel, attachment.remoteModificationIterator);
                    }

                    if (key.isReadable()) {
                        final SocketChannel socketChannel = (SocketChannel) key.channel();
                        final Attached attachment = (Attached) key.attachment();
                        attachment.socketChannelEntryReader.readAll(socketChannel);
                    }

                } catch (Exception e) {

                    //  if (!isClosed.get()) {
                    LOG.log(Level.SEVERE, "", e);
                    // Close channel and nudge selector
                    try {
                        key.channel().close();
                    } catch (IOException ex) {
                        // do nothing
                    }
                    // }
                }

                // remove key from selected set, it's been handled
                it.remove();
            }
        }
    }

    private static class Attached {

        final SocketChannelEntryReader socketChannelEntryReader;
        final ModificationIterator remoteModificationIterator;

        Attached(SocketChannelEntryReader socketChannelEntryReader, ModificationIterator remoteModificationIterator) {
            this.socketChannelEntryReader = socketChannelEntryReader;
            this.remoteModificationIterator = remoteModificationIterator;
        }
    }

}


