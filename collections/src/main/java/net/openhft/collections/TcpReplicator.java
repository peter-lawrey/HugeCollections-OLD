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

import net.openhft.lang.collection.ATSDirectBitSet;
import net.openhft.lang.collection.DirectBitSet;
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.NativeBytes;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import static java.nio.channels.SelectionKey.*;
import static net.openhft.collections.ReplicatedSharedHashMap.EntryExternalizable;
import static net.openhft.collections.ReplicatedSharedHashMap.ModificationIterator;

/**
 * Used with a {@see net.openhft.collections.ReplicatedSharedHashMap} to send data between the maps using a
 * socket connection <p/> {@see net.openhft.collections.OutSocketReplicator}
 *
 * @author Rob Austin.
 */
class TcpReplicator extends AbstractChannelReplicator implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(TcpReplicator.class.getName());
    private static final int BUFFER_SIZE = 0x100000; // 1MB

    private final Queue<Runnable> pendingRegistrations = new
            ConcurrentLinkedQueue<Runnable>();

    private final TcpReplicatorBuilder tcpReplicatorBuilder;

    private final Map<SocketAddress, AbstractConnector> connectorBySocket = new
            ConcurrentHashMap<SocketAddress, AbstractConnector>();
    private Throttler throttler;


    private final SelectionKey[] selectionKeysStore = new SelectionKey[128];

    private final DirectBitSet activeKeys = new ATSDirectBitSet(new ByteBufferBytes(
            ByteBuffer.allocate(16)));

    private final Heartbeat heartbeat = new Heartbeat(selectionKeysStore, activeKeys, closeables);

    // used to instruct the selector thread to set OP_WRITE on a key correlated by the bit index in the
    // bitset
    private KeyInterestUpdater opWriteUpdater = new KeyInterestUpdater(OP_WRITE, selectionKeysStore);

    private int selectorTimout;

    TcpReplicator(@NotNull final ReplicatedSharedHashMap map,
                  @NotNull final EntryExternalizable externalizable,
                  @NotNull final TcpReplicatorBuilder tcpReplicatorBuilder,
                  final int serializedEntrySize) throws IOException {

        super("TcpSocketReplicator-" + map.identifier());

        this.tcpReplicatorBuilder = tcpReplicatorBuilder;
        selectorTimout = tcpReplicatorBuilder.minIntervalMS();

        if (tcpReplicatorBuilder.throttle() > 0)
            throttler = new Throttler(selector, tcpReplicatorBuilder.throttleBucketIntervalMS(),
                    serializedEntrySize, tcpReplicatorBuilder.throttle());

        this.executorService.execute(
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            process(map, serializedEntrySize, externalizable, tcpReplicatorBuilder);
                        } catch (Exception e) {
                            LOG.error("", e);
                        }
                    }
                });
    }

    /**
     * TODO binds to the server socket and process data This method will block until interrupted
     */
    private void process(@NotNull final ReplicatedSharedHashMap map,
                         final int serializedEntrySize,
                         @NotNull final EntryExternalizable externalizable,
                         @NotNull final TcpReplicatorBuilder tcpReplicatorBuilder) throws IOException {

        final byte localIdentifier = map.identifier();
        final int packetSize = tcpReplicatorBuilder.packetSize();

        try {

            final InetSocketAddress address = tcpReplicatorBuilder.serverInetSocketAddress();
            final Details serverDetails = new Details(address, localIdentifier);
            connectorBySocket.put(address, new ServerConnector(serverDetails));

            for (InetSocketAddress client : tcpReplicatorBuilder.endpoints()) {
                final Details clientDetails = new Details(client, localIdentifier);
                connectorBySocket.put(client, new ClientConnector(clientDetails));
            }

            for (AbstractConnector connector : connectorBySocket.values()) {
                connector.connect();
            }

            while (selector.isOpen()) {

                if (!pendingRegistrations.isEmpty())
                    register(pendingRegistrations);

                final int nSelectedKeys = selector.select(selectorTimout);

                // its less resource intensive to set this less frequently and use an approximation
                final long approxTime = System.currentTimeMillis();

                if (throttler != null)
                    throttler.checkThrottleInterval();

                // check that we have sent and received heartbeats
                heartbeat.monitor(approxTime);

                // set the OP_WRITE when data is ready to send
                opWriteUpdater.applyUpdates();

                if (nSelectedKeys == 0)
                    continue;    // go back and check pendingRegistrations

                final Set<SelectionKey> selectionKeys = selector.selectedKeys();
                for (final SelectionKey key : selectionKeys) {
                    try {
                        if (!key.isValid())
                            continue;

                        if (key.isAcceptable())
                            onAccept(key, serializedEntrySize, externalizable, packetSize,
                                    map.identifier());

                        if (key.isConnectable())
                            onConnect(packetSize, serializedEntrySize, externalizable, key,
                                    map.identifier());

                        if (key.isReadable())
                            onRead(map, key, approxTime, tcpReplicatorBuilder.heartBeatInterval());

                        if (key.isWritable())
                            onWrite(key, approxTime);

                    } catch (CancelledKeyException e) {
                        quietClose(key, e);
                    } catch (ClosedSelectorException e) {
                        quietClose(key, e);
                    } catch (IOException e) {
                        quietClose(key, e);
                    } catch (InterruptedException e) {
                        quietClose(key, e);
                    } catch (Exception e) {
                        LOG.info("", e);
                        close(key);
                    }

                }
                selectionKeys.clear();

            }
        } catch (CancelledKeyException e) {
            if (LOG.isDebugEnabled())
                LOG.debug("", e);
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
        } finally

        {
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
     * used to periodically check that we receive heartbeats and send hearts beats out.
     */
    static class Heartbeat {

        private final SelectionKey[] selectionKeysStore;
        private final DirectBitSet activeKeys;
        private final Set<Closeable> closeables;

        public Heartbeat(@NotNull SelectionKey[] selectionKeysStore,
                         @NotNull DirectBitSet activeKeys,
                         @NotNull Set<Closeable> closeables) {
            this.closeables = closeables;
            this.selectionKeysStore = selectionKeysStore;
            this.activeKeys = activeKeys;
        }

        void monitor(long approxTime) {
            for (long i = activeKeys.nextSetBit(0); i >= 0; i = activeKeys.nextSetBit(i + 1)) {
                try {

                    SelectionKey key = selectionKeysStore[(int) i];

                    if (!key.isValid() || !key.channel().isOpen()) {
                        activeKeys.clear(i);
                        continue;
                    }

                    Attached attachment = (Attached) key.attachment();

                    if (attachment == null || attachment.remoteModificationIterator == null)
                        continue;

                    heartbeatCheckShouldSend(approxTime, key, attachment);
                    heartbeatCheckHasReceived(key, approxTime);

                } catch (CancelledKeyException e) {
                    throw e;
                } catch (Exception e) {
                    LOG.error("key.interestOps()=", e);
                }

            }
        }

        private void heartbeatCheckShouldSend(long approxTime, SelectionKey key, Attached attachment) {
            if (attachment.isHandShakingComplete() && attachment.entryWriter.lastSentTime +
                    attachment.entryWriter.heartBeatInterval < approxTime) {

                attachment.entryWriter.lastSentTime = approxTime;
                attachment.entryWriter.writeHeartbeatToBuffer();

                int ops = key.interestOps();
                if ((ops & (OP_CONNECT | OP_ACCEPT)) == 0)
                    key.interestOps(ops | OP_WRITE);

                if (LOG.isDebugEnabled())
                    LOG.debug("sending heartbeat");

            }
        }

        /**
         * check to see if we have lost connection with the remote node and if we have attempts a reconnect.
         *
         * @param key               the key relating to the heartbeat that we are checking
         * @param approxTimeOutTime the current time
         * @throws ConnectException
         */
        private void heartbeatCheckHasReceived(final SelectionKey key,
                                               final long approxTimeOutTime) throws ConnectException {

            final Attached attached = (Attached) key.attachment();

            // we wont attempt to reconnect the server socket
            if (attached == null || attached.isServer || !attached.isHandShakingComplete())
                return;

            final SocketChannel channel = (SocketChannel) key.channel();

            if (approxTimeOutTime > attached.entryReader.lastHeartBeatReceived + attached.remoteHeartbeatInterval) {
                if (LOG.isDebugEnabled())
                    LOG.debug("lost connection, attempting to reconnect. identifier=" + attached.localIdentifier);
                try {
                    channel.socket().close();
                    channel.close();
                    activeKeys.clear(attached.remoteIdentifier);
                    closeables.remove(channel);
                } catch (IOException e) {
                    LOG.debug("", e);
                }

                attached.connector.connectLater();

                throw new ConnectException("LostConnection : missed heartbeat from identifier=" + attached
                        .remoteIdentifier + " attempting to reconnect");
            }
        }
    }

    /**
     * closes and only logs the exception at debug
     *
     * @param key the SelectionKey
     * @param e   the Exception that caused the issue
     */
    private void quietClose(SelectionKey key, Exception e) {
        if (LOG.isDebugEnabled())
            LOG.debug("", e);
        close(key);
    }

    private void close(SelectionKey key) {
        try {

            SocketChannel socketChannel = key.channel().provider().openSocketChannel();
            if (throttler != null)
                throttler.remove(socketChannel);
            socketChannel.close();
            key.channel().close();
            closeables.remove(key.channel());

        } catch (IOException ex) {
            // do nothing
        }
    }


    private class ServerConnector extends AbstractConnector {

        private final Details details;

        private ServerConnector(@NotNull Details details) {
            super("TCP-ServerConnector-" + details.localIdentifier());
            this.details = details;
        }

        SelectableChannel doConnect() throws
                IOException, InterruptedException {

            final ServerSocketChannel serverChannel = ServerSocketChannel.open();

            serverChannel.socket().setReceiveBufferSize(BUFFER_SIZE);
            serverChannel.configureBlocking(false);
            final ServerSocket serverSocket = serverChannel.socket();
            serverSocket.setReuseAddress(true);

            serverSocket.bind(details.address());

            // these can be run on thi thread
            pendingRegistrations.add(new Runnable() {
                @Override
                public void run() {
                    final Attached attached = new Attached();

                    attached.localIdentifier = details.localIdentifier();
                    attached.connector = ServerConnector.this;
                    try {
                        serverChannel.register(TcpReplicator.this.selector, OP_ACCEPT, attached);
                    } catch (ClosedChannelException e) {
                        LOG.error("", e);
                    }

                }
            });

            selector.wakeup();

            return serverChannel;

        }


    }


    private class ClientConnector extends AbstractConnector {

        private final Details details;

        private ClientConnector(@NotNull Details details) {
            super("TCP-ClientConnector-" + details.localIdentifier());
            this.details = details;
        }

        /**
         * blocks until connected
         */
        SelectableChannel doConnect() throws IOException, InterruptedException {

            boolean success = false;

            for (; ; ) {

                final SocketChannel socketChannel = SocketChannel.open();

                try {

                    socketChannel.configureBlocking(false);
                    socketChannel.socket().setReuseAddress(false);
                    socketChannel.socket().setSoLinger(false, 0);
                    socketChannel.socket().setSoTimeout(0);
                    socketChannel.socket().setTcpNoDelay(true);

                    synchronized (TcpReplicator.this.closeables) {
                        try {
                            socketChannel.connect(details.address());
                        } catch (UnresolvedAddressException e) {
                            this.connectLater();
                        }
                        closeables.add(socketChannel);

                        // the registration has be be run on the same thread as the selector
                        pendingRegistrations.add(new Runnable() {
                            @Override
                            public void run() {
                                final Attached attached = new Attached();
                                attached.connector = ClientConnector.this;
                                attached.localIdentifier = details.localIdentifier();
                                try {
                                    socketChannel.register(selector, OP_CONNECT, attached);

                                } catch (ClosedChannelException e) {
                                    if (socketChannel.isOpen())
                                        LOG.error("", e);
                                }

                            }
                        });

                    }
                    selector.wakeup();
                    success = true;
                    return socketChannel;

                } finally {
                    if (!success)
                        try {

                            try {
                                socketChannel.socket().close();
                            } catch (Exception e) {
                                LOG.error("", e);
                            }

                            socketChannel.close();
                            if (throttler != null)
                                throttler.remove(socketChannel);
                        } catch (IOException e) {
                            LOG.error("", e);
                        }
                }
            }

        }
    }

    private void onConnect(int packetSize,
                           int serializedEntrySize,
                           @NotNull final EntryExternalizable externalizable,
                           @NotNull final SelectionKey key,
                           final byte identifier) throws IOException, InterruptedException {

        final SocketChannel channel = (SocketChannel) key.channel();

        final Attached attached = (Attached) key.attachment();

        if (channel == null)
            return;
        try {
            if (!channel.finishConnect()) {
                return;
            }
        } catch (SocketException e) {
            quietClose(key, e);
            attached.connector.connect();
            throw e;
        }

        attached.connector.setSuccessfullyConnected();
        if (LOG.isDebugEnabled())
            LOG.debug("successfully connected to {}, local-id={}", channel.socket().getInetAddress(),
                    identifier);

        channel.configureBlocking(false);
        channel.socket().setTcpNoDelay(true);
        channel.socket().setSoTimeout(0);
        channel.socket().setSoLinger(false, 0);

        attached.entryReader = new TcpSocketChannelEntryReader(serializedEntrySize,
                externalizable, packetSize);

        attached.entryWriter = new TcpSocketChannelEntryWriter(serializedEntrySize,
                externalizable, packetSize, tcpReplicatorBuilder.heartBeatInterval(), selector);

        channel.register(selector, OP_WRITE | OP_READ, attached);

        if (throttler != null)
            throttler.add(channel);

        // register it with the selector and store the ModificationIterator for this key
        attached.entryWriter.identifierToBuffer(identifier);
    }


    private void onAccept(SelectionKey key,
                          final int serializedEntrySize,
                          final EntryExternalizable externalizable,
                          final int packetSize,
                          final int localIdentifier) throws IOException {

        final ServerSocketChannel server = (ServerSocketChannel) key.channel();
        final SocketChannel channel = server.accept();
        channel.configureBlocking(false);
        channel.socket().setReuseAddress(true);
        channel.socket().setTcpNoDelay(true);
        channel.socket().setSoTimeout(0);
        channel.socket().setSoLinger(false, 0);

        final Attached attached = new Attached();
        channel.register(selector, OP_WRITE | OP_READ, attached);

        if (throttler != null)
            throttler.add(channel);

        attached.entryReader = new TcpSocketChannelEntryReader(serializedEntrySize,
                externalizable, packetSize);

        attached.entryWriter = new TcpSocketChannelEntryWriter(serializedEntrySize,
                externalizable, packetSize, tcpReplicatorBuilder.heartBeatInterval(), selector);

        attached.isServer = true;
        attached.entryWriter.identifierToBuffer(localIdentifier);
    }

    /**
     * used to exchange identifiers and timestamps between the server and client
     *
     * @param map
     * @param attached
     * @param localHeartbeatInterval
     * @param key
     * @throws java.io.IOException
     * @throws InterruptedException
     */
    private void doHandShaking(final ReplicatedSharedHashMap map,
                               final Attached attached,
                               final long localHeartbeatInterval, final SelectionKey key) throws IOException, InterruptedException {

        final TcpSocketChannelEntryWriter writer = attached.entryWriter;
        final TcpSocketChannelEntryReader reader = attached.entryReader;

        if (attached.remoteIdentifier == Byte.MIN_VALUE) {

            final byte remoteIdentifier = reader.identifierFromBuffer();

            if (remoteIdentifier == Byte.MIN_VALUE)
                return;

            attached.remoteIdentifier = remoteIdentifier;

            // we use the as iterating the activeKeys via the bitset wont create and Objects
            // but if we use the selector.keys() this will.
            selectionKeysStore[remoteIdentifier] = key;
            activeKeys.set(remoteIdentifier);

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

            attached.remoteModificationIterator = map.acquireModificationIterator(remoteIdentifier, attached);
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
            // timed out, unless the heartbeat interval has exceeded 25% of the expected time.
            attached.remoteHeartbeatInterval = (long) (value * 1.25);

            // we have to make our selector poll interval at least as short as the minimum selector timeout
            selectorTimout = Math.min(selectorTimout, (int) value);

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

        if (attached == null)
            return;


        if (attached.remoteModificationIterator != null)
            attached.entryWriter.entriesToBuffer(attached.remoteModificationIterator, socketChannel, attached);

        try {
            int len = attached.entryWriter.writeBufferToSocket(socketChannel,
                    approxTime);

            if (this.throttler != null)
                this.throttler.contemplateUnregisterWriteSocket(len);

        } catch (IOException e) {
            quietClose(key, e);
            if (!attached.isServer)
                attached.connector.connectLater();
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
                attached.connector.connectLater();
            throw e;
        }

        if (LOG.isDebugEnabled())
            LOG.debug("heartbeat or data received.");

        attached.entryReader.lastHeartBeatReceived = approxTime;

        if (attached.isHandShakingComplete())
            attached.entryReader.entriesFromBuffer();
        else
            doHandShaking(map, attached, localHeartbeatInterval, key);

    }


    class Attached implements ReplicatedSharedHashMap.ModificationNotifier {

        public TcpSocketChannelEntryReader entryReader;
        public TcpSocketChannelEntryWriter entryWriter;
        public ReplicatedSharedHashMap.ModificationIterator remoteModificationIterator;
        public AbstractConnector connector;
        public long remoteBootstrapTimestamp = Long.MIN_VALUE;
        private boolean handShakingComplete;
        public byte remoteIdentifier = Byte.MIN_VALUE;
        public byte localIdentifier;

        // the frequency the remote node will send a heartbeat
        public long remoteHeartbeatInterval = 100;
        public boolean hasRemoteHeartbeatInterval;

        //  public volatile boolean enableWrite;

        public boolean isServer;

        boolean isHandShakingComplete() {
            return handShakingComplete;
        }

        void setHandShakingComplete() {
            handShakingComplete = true;
        }

        /**
         * called whenever there is a change to the modification iterator
         */
        @Override
        public void onChange() {

            if (remoteIdentifier != Byte.MIN_VALUE)
                TcpReplicator.this.opWriteUpdater.set(remoteIdentifier);

            // enableWrite = true;
            selector.wakeup();
        }
    }


    /**
     * @author Rob Austin.
     */
    private class TcpSocketChannelEntryWriter {

        //     private static final Logger LOG = LoggerFactory.getLogger(TcpSocketChannelEntryWriter.class);

        private final ByteBuffer out;
        private final ByteBufferBytes in;
        private final EntryCallback entryCallback;
        private final int serializedEntrySize;
        private long lastSentTime;
        private long heartBeatInterval;

        private final Selector selector;


        /**
         * @param serializedEntrySize the size of the entry
         * @param externalizable      supports reading and writing serialize entries
         * @param packetSize          the max TCP/IP packet size
         * @param selector
         */
        TcpSocketChannelEntryWriter(final int serializedEntrySize,
                                    @NotNull final EntryExternalizable externalizable,
                                    int packetSize, long heartBeatInterval,
                                    Selector selector) {
            this.serializedEntrySize = serializedEntrySize;
            this.heartBeatInterval = heartBeatInterval;

            this.selector = selector;
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
         * @param socketChannel
         * @param attached
         */
        void entriesToBuffer(@NotNull final ModificationIterator modificationIterator, SocketChannel socketChannel, Attached attached)
                throws InterruptedException {

            final long start = in.position();

            for (; ; ) {

                final boolean wasDataRead = modificationIterator.nextEntry(entryCallback);

                if (!wasDataRead) {

                    // if we have no more data to write to the socket then we will
                    // un-register OP_WRITE on the selector, until more data becomes available
                    if (in.position() == 0 && attached.isHandShakingComplete()) {
                        disableWrite(socketChannel, attached);
                        return;
                    }

                    // if there was no data written to the buffer and we have not written any more data to
                    // the buffer, then give up
                    if (in.position() == start)
                        return;
                }

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
         * @param socketChannel the socket to publish the buffer to
         * @param approxTime
         * @throws IOException
         */
        private int writeBufferToSocket(SocketChannel socketChannel,
                                        final long approxTime) throws IOException {

            if (in.position() == 0)
                return 0;

            // if we still have some unwritten writer from last time
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

            return len;


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


        /**
         * removes back in the OP_WRITE from the selector, otherwise it'll spin loop It will get added back in
         * as soon as we have data to write
         *
         * @param socketChannel
         * @param attached
         */
        public synchronized void disableWrite(final SocketChannel socketChannel, Attached attached) {


            try {
                SelectionKey selectionKey1 = socketChannel.keyFor(selector);
                if (selectionKey1 != null) {


                    if (attached.isHandShakingComplete() && selector.isOpen()) {
                        if (LOG.isDebugEnabled())
                            LOG.debug("Disabling OP_WRITE to remoteIdentifier=" + attached
                                    .remoteIdentifier + ", localIdentifier=" + attached.localIdentifier);
                        selectionKey1.interestOps(selectionKey1.interestOps() & ~OP_WRITE);
                    }
                }

            } catch (Exception e) {

                LOG.error("", e);
            }

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
        public long lastHeartBeatReceived = System.currentTimeMillis();


        /**
         * @param serializedEntrySize the maximum size of an entry include the meta data
         * @param externalizable      supports reading and writing serialize entries
         * @param packetSize          the estimated size of a tcp/ip packet
         */
        private TcpSocketChannelEntryReader(final int serializedEntrySize,
                                            @NotNull final EntryExternalizable externalizable,
                                            final int packetSize) {

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

