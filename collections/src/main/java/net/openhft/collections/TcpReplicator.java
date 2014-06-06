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
import net.openhft.lang.thread.NamedThreadFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static java.nio.channels.SelectionKey.*;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static net.openhft.collections.ReplicatedSharedHashMap.EntryExternalizable;
import static net.openhft.collections.ReplicatedSharedHashMap.ModificationIterator;
import static net.openhft.collections.TcpReplicator.AbstractConnector.Details;

/**
 * Used with a {@see net.openhft.collections.ReplicatedSharedHashMap} to send data between the maps using a
 * socket connection
 *
 * {@see net.openhft.collections.OutSocketReplicator}
 *
 * @author Rob Austin.
 */
class TcpReplicator implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(TcpReplicator.class.getName());
    private static final int BUFFER_SIZE = 0x100000; // 1Mb

    private final ExecutorService executorService;
    private final Set<Closeable> closeables = new CopyOnWriteArraySet<Closeable>();
    private final Connector connector = new Connector();
    private final Selector selector;
    private SelectableChannel serverSocketChannel;
    private Queue<SelectableChannel> pendingRegistrations = new ConcurrentLinkedQueue<SelectableChannel>();
    private final long heartBeatInterval;

    TcpReplicator(@NotNull final ReplicatedSharedHashMap map,
                  final int serializedEntrySize,
                  @NotNull final EntryExternalizable externalizable,
                  @NotNull final TcpReplicatorBuilder tcpReplicatorBuilder) throws IOException {

        heartBeatInterval = tcpReplicatorBuilder.heartBeatInterval();
        this.selector = Selector.open();
        this.executorService = newSingleThreadExecutor(
                new NamedThreadFactory("TcpSocketReplicator-" + map.identifier(), true));

        this.executorService.execute(new Runnable() {
                                         @Override
                                         public void run() {
                                             try {
                                                 process(map,
                                                         serializedEntrySize,
                                                         externalizable,
                                                         tcpReplicatorBuilder);

                                             } catch (IOException e) {
                                                 if (selector.isOpen())
                                                     LOG.error("", e);
                                             }
                                         }
                                     }
        );
    }


    private void process(@NotNull final ReplicatedSharedHashMap map,
                         final int serializedEntrySize,
                         @NotNull final EntryExternalizable externalizable,
                         @NotNull final TcpReplicatorBuilder tcpReplicatorBuilder) throws IOException {

        final byte identifier = map.identifier();
        final short packetSize = tcpReplicatorBuilder.packetSize();

        try {

            connector.asyncConnect(identifier, tcpReplicatorBuilder.endpoints(),
                    tcpReplicatorBuilder.serverInetSocketAddress());

            for (; ; ) {

                register(pendingRegistrations);

                if (!selector.isOpen())
                    return;

                final int nSelectedKeys = selector.select(100);

                if (nSelectedKeys == 0) {
                    continue;    // nothing to do
                }

                // its less resource intensive to set this less frequently and use an approximation
                final long approxTime = System.currentTimeMillis();


                final Set<SelectionKey> selectedKeys = selector.selectedKeys();

                for (final SelectionKey key : selectedKeys) {
                    try {
                        if (!key.isValid())
                            continue;

                        if (key.isAcceptable())
                            onAccept(key, serializedEntrySize, externalizable, packetSize,
                                    map.identifier(), tcpReplicatorBuilder.heartBeatInterval());

                        if (key.isConnectable())
                            onConnect(packetSize, serializedEntrySize, externalizable, key,
                                    map.identifier(), tcpReplicatorBuilder.heartBeatInterval());

                        if (key.isReadable())
                            onRead(map, key, approxTime, tcpReplicatorBuilder.heartBeatInterval());

                        if (key.isWritable())
                            onWrite(key, approxTime);

                        checkHeartbeat(key, approxTime, identifier);

                    } catch (CancelledKeyException e) {
                        quietClose(key, e);
                    } catch (ClosedSelectorException e) {
                        quietClose(key, e);
                    } catch (IOException e) {
                        quietClose(key, e);
                    } catch (Exception e) {
                        LOG.info("", e);
                        close(key);
                    }

                }

                selectedKeys.clear();
            }
        } catch (ClosedSelectorException e) {
            if (LOG.isDebugEnabled())
                LOG.debug("", e);
        } catch (ClosedChannelException e) {
            if (LOG.isDebugEnabled())
                LOG.debug("", e);
        } catch (ConnectException e) {
            if (LOG.isDebugEnabled())
                LOG.debug("", e);
        } catch (Exception e) {
            LOG.error("", e);
        } finally {
            if (selector != null)
                try {
                    selector.close();
                } catch (IOException e) {
                    if (LOG.isDebugEnabled())
                        LOG.debug("", e);
                }
            close();
        }
    }

    /**
     * closes and only logs the exception at debug
     *
     * @param key
     * @param e
     */
    private void quietClose(SelectionKey key, Exception e) {
        if (LOG.isDebugEnabled())
            LOG.debug("", e);
        close(key);
    }

    private void close(SelectionKey key) {
        try {
            key.channel().provider().openSocketChannel().close();
            key.channel().close();
        } catch (IOException ex) {
            // do nothing
        }
    }

    private void checkHeartbeat(SelectionKey key,
                                final long approxTimeOutTime,
                                final byte identifier) throws ConnectException {

        final Attached attached = (Attached) key.attachment();

        // we wont attempt to reconnect the server socket
        if (key.channel() == serverSocketChannel || attached == null)
            return;

        if (attached.isServer || !attached.isHandShakingComplete())
            return;

        final SocketChannel channel = (SocketChannel) key.channel();

        if (approxTimeOutTime > attached.entryReader.lastHeartBeatReceived + attached.remoteHeartbeatInterval) {
            if (LOG.isDebugEnabled())
                LOG.debug("lost connection, attempting to reconnect. identifier=" + identifier);
            try {
                channel.socket().close();
                channel.close();
            } catch (IOException e) {
                LOG.debug("", e);
            }

            connector.asyncReconnect(identifier, channel.socket());
            throw new ConnectException("LostConnection : missed heartbeat from identifier=" + attached
                    .remoteIdentifier + " (attempting an automatic reconnection)");
        }
    }


    static abstract class AbstractConnector implements Runnable {

        private final Details details;

        public AbstractConnector(Details details) {
            this.details = details;
        }


        abstract SelectableChannel connect() throws IOException, InterruptedException;

        static class Details {

            final SocketAddress address;
            final Queue<SelectableChannel> pendingRegistrations;
            final Set<Closeable> closeables;
            long reconnectionInterval;
            final byte identifier;

            Details(@NotNull SocketAddress address,
                    Set<Closeable> closeables,
                    long reconnectionInterval,
                    byte identifier,
                    final Queue<SelectableChannel> pendingRegistrations) {
                this.address = address;
                this.pendingRegistrations = pendingRegistrations;
                this.closeables = closeables;
                this.reconnectionInterval = reconnectionInterval;
                this.identifier = identifier;
            }
        }

        public void run() {
            try {
                if (details.reconnectionInterval > 0)
                    Thread.sleep(details.reconnectionInterval);
                synchronized (details.closeables) {
                    final SelectableChannel socketChannel = connect();
                    if (socketChannel == null)
                        return;
                    details.pendingRegistrations.add(socketChannel);
                    details.closeables.add(socketChannel);

                }

            } catch (InterruptedException e) {
                // do nothing
            } catch (IOException e) {
                LOG.error("", e);
            }

        }

    }

    private class Connector {

        private final Map<InetSocketAddress, Integer> connectionAttempts = new HashMap<InetSocketAddress, Integer>();

        /**
         * used to connect both client and server sockets
         *
         * @param identifier
         * @param clientSocket a queue containing the SocketChannel as they become connected
         * @return
         */
        private void asyncReconnect(
                final byte identifier,
                final Socket clientSocket) {

            final InetSocketAddress inetSocketAddress = new InetSocketAddress(clientSocket.getInetAddress()
                    .getHostName(), clientSocket.getPort());

            final Integer lastAttempts = connectionAttempts.get(inetSocketAddress);
            final Integer attempts = lastAttempts == null ? 1 : lastAttempts + 1;

            if (attempts < 5)
                connectionAttempts.put(inetSocketAddress, attempts);

            long reconnectionInterval = attempts * 1000;
            asyncConnect0(identifier, null, Collections.singleton(inetSocketAddress), reconnectionInterval);
        }


        /**
         * used to connect both client and server sockets
         *
         * @param identifier
         * @param clientEndpoints
         * @param serverPort
         * @return
         */
        private void asyncConnect(
                final byte identifier,
                final Set<InetSocketAddress> clientEndpoints,
                final InetSocketAddress serverPort) {

            asyncConnect0(identifier, serverPort, clientEndpoints, 0);
        }

        void setSuccessfullyConnected(final Socket socket) {
            final InetSocketAddress inetSocketAddress = new InetSocketAddress(socket.getInetAddress()
                    .getHostName(), socket.getPort());

            connectionAttempts.remove(inetSocketAddress);
        }


        /**
         * used to connect both client and server sockets
         *
         * @param identifier
         * @param clientEndpoints      a queue containing the SocketChannel as they become connected
         * @param reconnectionInterval
         * @return
         */
        private void asyncConnect0(
                final byte identifier,
                final @Nullable SocketAddress serverEndpoint,
                final @NotNull Set<? extends SocketAddress> clientEndpoints,
                final long reconnectionInterval) {


            if (LOG.isDebugEnabled()) {
                LOG.debug("reconnecting to identifier=" + identifier);
            }

            if (serverEndpoint != null) {
                final Details details = new Details
                        (serverEndpoint, closeables,
                                reconnectionInterval, identifier, pendingRegistrations);

                final Thread thread = new Thread(new ServerConnector(details));
                thread.setName("server-tcp-connector-" + serverEndpoint);
                thread.setDaemon(true);
                thread.start();
            }

            for (final SocketAddress endpoint : clientEndpoints) {
                final Details details = new Details(endpoint, closeables,
                        reconnectionInterval, identifier, pendingRegistrations);
                final Thread thread = new Thread(new ClientConnector(details));
                thread.setName("client-tcp-connector-" + endpoint);
                thread.setDaemon(true);
                thread.start();
            }


        }
    }


    private static class ServerConnector extends AbstractConnector {

        private final Details details;

        private ServerConnector(Details details) {
            super(details);
            this.details = details;
        }

        SelectableChannel connect() throws
                IOException, InterruptedException {

            ServerSocketChannel serverChannel = ServerSocketChannel.open();

            serverChannel.socket().setReceiveBufferSize(BUFFER_SIZE);
            serverChannel.configureBlocking(false);
            final ServerSocket serverSocket = serverChannel.socket();
            serverSocket.setReuseAddress(true);
            serverSocket.bind(details.address);
            return serverChannel;

        }

    }


    private static class ClientConnector extends AbstractConnector {

        private final Details details;

        private ClientConnector(@NotNull Details details) {
            super(details);
            this.details = details;
        }

        /**
         * blocks until connected
         */
        SelectableChannel connect()
                throws IOException, InterruptedException {

            boolean success = false;

            for (; ; ) {

                final SocketChannel socketChannel = SocketChannel.open();

                try {

                    socketChannel.configureBlocking(false);
                    socketChannel.socket().setReuseAddress(false);
                    socketChannel.socket().setSoLinger(false, 0);
                    socketChannel.socket().setSoTimeout(0);
                    socketChannel.socket().setTcpNoDelay(true);

                    synchronized (details.closeables) {
                        details.closeables.add(socketChannel.socket());
                        socketChannel.connect(details.address);
                        details.closeables.add(socketChannel);
                    }
                    if (LOG.isDebugEnabled())
                        LOG.debug("successfully connected to {}, local-id={}", details.address,
                                details.identifier);

                    success = true;
                    return socketChannel;

                } catch (IOException e) {
                    throw e;
                } finally {
                    if (!success)
                        try {

                            try {
                                socketChannel.socket().close();
                            } catch (Exception e) {
                                LOG.error("", e);
                            }

                            socketChannel.close();
                        } catch (IOException e) {
                            LOG.error("", e);
                        }
                }
            }

        }
    }

    private void onConnect(short packetSize,
                           int serializedEntrySize,
                           @NotNull final EntryExternalizable externalizable,
                           @NotNull final SelectionKey key,
                           final byte identifier,
                           final long heartBeatIntervalMilliseconds) throws IOException, InterruptedException {

        final SocketChannel channel = (SocketChannel) key.channel();
        if (channel == null)
            return;
        try {
            if (!channel.finishConnect()) {
                return;
            }
        } catch (ConnectException e) {
            quietClose(key, e);
            connector.asyncReconnect(identifier, channel.socket());
            throw e;
        }

        connector.setSuccessfullyConnected(channel.socket());

        channel.configureBlocking(false);
        channel.socket().setTcpNoDelay(true);
        channel.socket().setSoTimeout(0);
        channel.socket().setSoLinger(false, 0);

        final Attached attached = new Attached();

        attached.entryReader = new TcpSocketChannelEntryReader(serializedEntrySize,
                externalizable, packetSize);

        attached.entryWriter = new TcpSocketChannelEntryWriter(serializedEntrySize,
                externalizable, packetSize, heartBeatInterval);

        channel.register(selector, OP_WRITE | OP_READ, attached);

        // register it with the selector and store the ModificationIterator for this key
        attached.entryWriter.identifierToBuffer(identifier);
    }


    @Override
    public void close() throws IOException {

        synchronized (this.closeables) {
            for (Closeable closeable : this.closeables) {
                try {
                    closeable.close();
                } catch (IOException e) {
                    LOG.error("", e);
                }
            }
        }
        closeables.clear();

        executorService.shutdownNow();
        selector.close();
    }


    private void onAccept(SelectionKey key,
                          final int serializedEntrySize,
                          final EntryExternalizable externalizable,
                          final short packetSize,
                          final int localIdentifier, final long heartBeatPeriod) throws IOException {
        final ServerSocketChannel server = (ServerSocketChannel) key.channel();
        final SocketChannel channel = server.accept();
        channel.configureBlocking(false);
        channel.socket().setReuseAddress(true);
        channel.socket().setTcpNoDelay(true);
        channel.socket().setSoTimeout(0);
        channel.socket().setSoLinger(false, 0);

        final Attached attached = new Attached();
        channel.register(selector, OP_WRITE | OP_READ, attached);

        attached.entryReader = new TcpSocketChannelEntryReader(serializedEntrySize,
                externalizable, packetSize);

        attached.entryWriter = new TcpSocketChannelEntryWriter(serializedEntrySize,
                externalizable, packetSize, heartBeatInterval);

        attached.isServer = true;
        attached.entryWriter.identifierToBuffer(localIdentifier);
    }

    /**
     * used to exchange identifiers and timestamps between the server and client
     *
     * @param map
     * @param attached
     * @param localHeartbeatInterval
     * @throws java.io.IOException
     * @throws InterruptedException
     */
    private void doHandShaking(final ReplicatedSharedHashMap map,
                               final Attached attached,
                               final long localHeartbeatInterval) throws IOException, InterruptedException {

        final TcpSocketChannelEntryWriter writer = attached.entryWriter;
        final TcpSocketChannelEntryReader reader = attached.entryReader;

        if (attached.remoteIdentifier == Byte.MIN_VALUE) {

            final byte remoteIdentifier = reader.identifierFromBuffer();

            if (remoteIdentifier == Byte.MIN_VALUE)
                return;

            attached.remoteIdentifier = remoteIdentifier;

            if (LOG.isDebugEnabled()) {
                LOG.debug("server-connection id={}, remoteIdentifier={}",
                        map.identifier(), remoteIdentifier);
            }

            if (remoteIdentifier == map.identifier()) {
                throw new IllegalStateException("Where are connecting to a remote " +
                        "map with the same " +
                        "identifier as this map, identifier=" + map.identifier() + ", " +
                        "please change either this maps identifier or the remote one");
            }

            attached.remoteModificationIterator = map.acquireModificationIterator(remoteIdentifier);
            writer.writeRemoteBootstrapTimestamp(map.lastModificationTime(remoteIdentifier));

            // tell the remote node, what are heartbeat interval is
            writer.writeRemoteHeartbeatInterval(localHeartbeatInterval);
        }

        if (attached.remoteBootstrapTimestamp == Long.MIN_VALUE) {

            attached.remoteBootstrapTimestamp = reader.remoteBootstrapTimestamp();

            if (attached.remoteBootstrapTimestamp == Long.MIN_VALUE)
                return;
        }

        if (!attached.hasRemoteHeartbeatInterval) {

            long value = reader.remoteHeartbeatIntervalFromBuffer();

            if (value == Long.MIN_VALUE)
                return;

            if (value < 0) {
                LOG.error("value=" + value);
            }


            // we add a 10% safety margin to the timeout time due to latency fluctuations on the network,
            // in other words we wont consider a connection to have
            // timed out, unless the heartbeat interval has exceeded 10% of the expected time.
            attached.remoteHeartbeatInterval = (long) (value * 1.1);
            attached.hasRemoteHeartbeatInterval = true;

            // now we're finished we can get on with reading the entries
            attached.setHandShakingComplete();
            attached.remoteModificationIterator.dirtyEntries(attached.remoteBootstrapTimestamp);
            reader.entriesFromBuffer();
        }
    }

    private void onWrite(final SelectionKey key, final long approxTime) throws InterruptedException, IOException {
        final SocketChannel socketChannel = (SocketChannel) key.channel();
        final Attached attached = (Attached) key.attachment();

        if (attached.remoteModificationIterator != null)
            attached.entryWriter.entriesToBuffer(attached.remoteModificationIterator);

        try {
            attached.entryWriter.writeBufferToSocket(socketChannel, attached.isHandShakingComplete(),
                    approxTime);
        } catch (IOException e) {
            quietClose(key, e);
            if (!attached.isServer)
                connector.asyncReconnect(attached.remoteIdentifier, socketChannel.socket());
            throw e;

        }

    }

    private void onRead(final ReplicatedSharedHashMap map,
                        final SelectionKey key,
                        final long approxTime,
                        final long localHeartbeatInterval) throws IOException, InterruptedException {

        final SocketChannel socketChannel = (SocketChannel) key.channel();
        final Attached attached = (Attached) key.attachment();

        try {
            if (attached.entryReader.readSocketToBuffer(socketChannel) <= 0)
                return;

        } catch (IOException e) {
            if (!attached.isServer)
                connector.asyncReconnect(map.identifier(), socketChannel.socket());
            throw e;
        }


        if (LOG.isDebugEnabled())
            LOG.debug("heartbeat or data received.");

        attached.entryReader.lastHeartBeatReceived = approxTime;

        if (attached.isHandShakingComplete())
            attached.entryReader.entriesFromBuffer();
        else
            doHandShaking(map, attached, localHeartbeatInterval);

    }

    /**
     * Registers the SocketChannel with the selector
     *
     * @param selectableChannels the SelectableChannel to register
     * @throws ClosedChannelException
     */
    private void register(@NotNull final Queue<SelectableChannel> selectableChannels) throws
            ClosedChannelException {
        for (SelectableChannel sc = selectableChannels.poll(); sc != null; sc = selectableChannels.poll()) {
            if (sc instanceof ServerSocketChannel) {
                sc.register(selector, OP_ACCEPT);
                this.serverSocketChannel = sc;
            } else
                sc.register(selector, OP_CONNECT);
        }
    }

    static class Attached {

        public TcpSocketChannelEntryReader entryReader;
        public ModificationIterator remoteModificationIterator;


        public long remoteBootstrapTimestamp = Long.MIN_VALUE;
        private boolean handShakingComplete;
        public byte remoteIdentifier = Byte.MIN_VALUE;

        // the frequency the remote node will send a heartbeat
        public long remoteHeartbeatInterval = 100;
        public boolean hasRemoteHeartbeatInterval;

        public TcpSocketChannelEntryWriter entryWriter;
        public boolean isServer;

        boolean isHandShakingComplete() {
            return handShakingComplete;
        }

        void setHandShakingComplete() {
            handShakingComplete = true;
        }


    }

    /**
     * @author Rob Austin.
     */
    private static class TcpSocketChannelEntryWriter {

        private static final Logger LOG = LoggerFactory.getLogger(TcpSocketChannelEntryWriter.class);

        private final ByteBuffer out;
        private final ByteBufferBytes in;
        private final EntryCallback entryCallback;
        private final int serializedEntrySize;
        private long lastSentTime;
        private long heartBeatInterval;


        /**
         * @param serializedEntrySize the size of the entry
         * @param externalizable      supports reading and writing serialize entries
         * @param packetSize          the max TCP/IP packet size
         * @param heartBeatInterval
         */
        TcpSocketChannelEntryWriter(final int serializedEntrySize,
                                    @NotNull final EntryExternalizable externalizable,
                                    int packetSize, long heartBeatInterval) {
            this.serializedEntrySize = serializedEntrySize;
            this.heartBeatInterval = heartBeatInterval;
            out = ByteBuffer.allocateDirect(packetSize + serializedEntrySize);
            in = new ByteBufferBytes(out);
            entryCallback = new EntryCallback(externalizable, in);
        }


        /**
         * writes the timestamp into the buffer
         *
         * @param localIdentifier the current nodes identifier
         */
        void identifierToBuffer(final int localIdentifier) {
            in.writeByte(localIdentifier);
        }

        /**
         * sends the identity and timestamp of this node to a remote node
         *
         * @param timeStampOfLastMessage the last timestamp we received a message from that node
         */
        void writeRemoteBootstrapTimestamp(final long timeStampOfLastMessage) {
            in.writeLong(timeStampOfLastMessage);
        }

        /**
         * writes all the entries that have changed, to the buffer which will later be written to TCP/IP
         *
         * @param modificationIterator Holds a record of which entries have modification.
         */
        void entriesToBuffer(@NotNull final ModificationIterator modificationIterator)
                throws InterruptedException {

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
         * @param socketChannel         the socket to publish the buffer to
         * @param isHandshakingComplete
         * @param approxTime
         * @throws IOException
         */
        private void writeBufferToSocket(SocketChannel socketChannel,
                                         boolean isHandshakingComplete,
                                         final long approxTime) throws IOException {

            // if we still have some unwritten writer from last time
            if (in.position() > 0) {

                lastSentTime = approxTime;
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
                return;
            }


            if (isHandshakingComplete && lastSentTime + heartBeatInterval < approxTime) {
                System.out.println("lastSentTime=" + TimeUnit.MILLISECONDS.toSeconds(lastSentTime) +
                        ",remoteHeartbeatInterval=" + TimeUnit.MILLISECONDS.toSeconds(heartBeatInterval)
                        + ",approxTime=" + TimeUnit.MILLISECONDS.toSeconds(approxTime));
                lastSentTime = approxTime;
                writeHeartbeatToBuffer();
                if (LOG.isDebugEnabled())
                    LOG.debug("sending heartbeat");
                writeBufferToSocket(socketChannel, true, approxTime);
            }

        }

        /**
         * used to send an single zero byte if we have not send any data for up to the localHeartbeatInterval
         */
        private void writeHeartbeatToBuffer() {
            in.writeUnsignedShort(0);
        }

        private void writeRemoteHeartbeatInterval(long localHeartbeatInterval) {
            in.writeLong(localHeartbeatInterval);
        }
    }


    /**
     * {@inheritDoc}
     */
    static class EntryCallback extends VanillaSharedReplicatedHashMap.EntryCallback {

        private final EntryExternalizable externalizable;
        private final ByteBufferBytes in;

        EntryCallback(@NotNull final EntryExternalizable externalizable,
                      @NotNull final ByteBufferBytes in) {

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
     * Reads map entries from a socket, this could be a client or server socket
     *
     * @author Rob Austin.
     */
    private static class TcpSocketChannelEntryReader {

        public static final int SIZE_OF_UNSIGNED_SHORT = 2;
        private final EntryExternalizable externalizable;
        private final int serializedEntrySize;
        private final ByteBuffer in;
        private final ByteBufferBytes out;

        // we use Integer.MIN_VALUE as N/A
        private int sizeOfNextEntry = Integer.MIN_VALUE;
        private long lastHeartBeatReceived = System.currentTimeMillis();

        /**
         * @param serializedEntrySize the maximum size of an entry include the meta data
         * @param externalizable      supports reading and writing serialize entries
         * @param packetSize          the estimated size of a tcp/ip packet
         */
        private TcpSocketChannelEntryReader(final int serializedEntrySize,
                                            @NotNull final EntryExternalizable externalizable,
                                            final short packetSize) {

            this.serializedEntrySize = serializedEntrySize;
            in = ByteBuffer.allocateDirect(packetSize + serializedEntrySize);
            this.externalizable = externalizable;
            out = new ByteBufferBytes(in);
            out.limit(0);
            in.clear();
        }

        /**
         * reads from the socket and writes the contents to the buffer
         *
         * @param socketChannel the  socketChannel to read from
         * @return the number of bytes read
         * @throws IOException
         */
        private int readSocketToBuffer(@NotNull final SocketChannel socketChannel) throws IOException {

            compactBuffer();
            final int len = socketChannel.read(in);
            out.limit(in.position());
            return len;
        }

        /**
         * reads entries from the socket till it is empty
         *
         * @throws InterruptedException
         */
        private void entriesFromBuffer() throws InterruptedException {

            for (; ; ) {

                out.limit(in.position());

                // its set to MIN_VALUE when it should be read again
                if (sizeOfNextEntry == Integer.MIN_VALUE) {
                    if (out.remaining() < SIZE_OF_UNSIGNED_SHORT) {
                        return;
                    }

                    int value = out.readUnsignedShort();

                    // this is the heartbeat
                    if (value == 0)
                        continue;

                    //this is the heart beat
                    sizeOfNextEntry = value;
                }


                if (out.remaining() < sizeOfNextEntry) {
                    return;
                }

                final long nextEntryPos = out.position() + sizeOfNextEntry;
                final long limit = out.limit();
                out.limit(nextEntryPos);
                externalizable.readExternalEntry(out);

                out.limit(limit);
                // skip onto the next entry
                out.position(nextEntryPos);

                // to allow the sizeOfNextEntry to be read the next time around
                sizeOfNextEntry = Integer.MIN_VALUE;
            }

        }

        /**
         * compacts the buffer and updates the {@code in} and  {@code out} accordingly
         */
        private void compactBuffer() {

            // the serializedEntrySize used here may not be the maximum size of the entry in its serialized form
            // however, its only use as an indication that the buffer is becoming full and should be compacted
            // the buffer can be compacted at any time
            if (in.position() == 0 || in.remaining() > serializedEntrySize)
                return;

            in.limit(in.position());
            in.position((int) out.position());

            in.compact();
            out.position(0);
        }

        /**
         * @return the identifier or -1 if unsuccessful
         */
        byte identifierFromBuffer() {
            return (out.remaining() >= 1) ? out.readByte() : Byte.MIN_VALUE;
        }

        /**
         * @return the timestamp or -1 if unsuccessful
         */
        long remoteBootstrapTimestamp() {
            return (out.remaining() >= 8) ? out.readLong() : Long.MIN_VALUE;
        }

        public long remoteHeartbeatIntervalFromBuffer() {
            return (out.remaining() >= 8) ? out.readLong() : Long.MIN_VALUE;
        }
    }

}

