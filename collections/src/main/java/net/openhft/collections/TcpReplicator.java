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
 * socket connection
 *
 * {@see net.openhft.collections.OutSocketReplicator}
 *
 * @author Rob Austin.
 */
class TcpReplicator extends AbstractChannelReplicator implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(TcpReplicator.class.getName());
    private static final int BUFFER_SIZE = 0x100000; // 1Mb

    //  private final DelegatingConnector delegatingConnector = new DelegatingConnector();
    private SelectableChannel serverSocketChannel;

    private final Queue<Runnable> pendingRegistrations = new
            ConcurrentLinkedQueue<Runnable>();

    private final TcpReplicatorBuilder tcpReplicatorBuilder;

    private final Map<SocketAddress, AbstractConnector> connectorBySocket = new
            ConcurrentHashMap<SocketAddress, AbstractConnector>();
    private Throttler throttler;

    TcpReplicator(@NotNull final ReplicatedSharedHashMap map,
                  final int serializedEntrySize,
                  @NotNull final EntryExternalizable externalizable,
                  @NotNull final TcpReplicatorBuilder tcpReplicatorBuilder) throws IOException {

        super("TcpSocketReplicator-" + map.identifier());

        this.tcpReplicatorBuilder = tcpReplicatorBuilder;

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

            final InetSocketAddress address = tcpReplicatorBuilder.serverInetSocketAddress();
            final Details serverDetails = new Details(address, closeables, identifier, pendingRegistrations);
            connectorBySocket.put(address, new ServerConnector(serverDetails));

            for (InetSocketAddress client : tcpReplicatorBuilder.endpoints()) {
                final Details clientDetails = new Details(client, closeables, identifier, pendingRegistrations);
                connectorBySocket.put(client, new ClientConnector(clientDetails));
            }

            for (AbstractConnector connector : connectorBySocket.values()) {
                connector.connect();
            }

            if (tcpReplicatorBuilder.throttle() > 0)
                throttler = new Throttler(selector, throttleInterval,
                        maxBytesInInterval);

            for (; ; ) {

                register(pendingRegistrations);

                if (!selector.isOpen())
                    return;

                final int nSelectedKeys = selector.select(100);

                if (throttler != null)
                    throttler.checkThrottleInterval();

                if (nSelectedKeys == 0) {
                    continue;    // nothing to do
                }

                // its less resource intensive to set this less frequently and use an approximation
                final long approxTime = System.currentTimeMillis();

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
                selectionKeys.clear();

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
                closeables.remove(channel);
            } catch (IOException e) {
                LOG.debug("", e);
            }

            attached.connector.connect();

            throw new ConnectException("LostConnection : missed heartbeat from identifier=" + attached
                    .remoteIdentifier + " attempting to reconnect");
        }
    }


    private class ServerConnector extends AbstractConnector {

        private final Details details;

        private ServerConnector(@NotNull Details details) {
            super("TCP-ServerConnector-" + details.getIdentifier(), details);
            this.details = details;
        }

        SelectableChannel doConnect() throws
                IOException, InterruptedException {

            final ServerSocketChannel serverChannel = ServerSocketChannel.open();

            serverChannel.socket().setReceiveBufferSize(BUFFER_SIZE);
            serverChannel.configureBlocking(false);
            final ServerSocket serverSocket = serverChannel.socket();
            serverSocket.setReuseAddress(true);

            serverSocket.bind(details.getAddress());

            // these can be run on thi thread
            pendingRegistrations.add(new Runnable() {
                @Override
                public void run() {
                    final Attached attached = new Attached();
                    attached.connector = ServerConnector.this;
                    try {
                        serverChannel.register(TcpReplicator.this.selector, OP_ACCEPT, attached);
                    } catch (ClosedChannelException e) {
                        LOG.error("", e);
                    }

                }
            });


            return serverChannel;

        }


    }


    private class ClientConnector extends AbstractConnector {

        private final Details details;

        private ClientConnector(@NotNull Details details) {
            super("TCP-ClientConnector-" + details.getIdentifier(), details);
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

                    final Set<Closeable> closeables = details.getCloseables();
                    synchronized (closeables) {

                        closeables.add(socketChannel.socket());
                        try {
                            socketChannel.connect(details.getAddress());
                        } catch (UnresolvedAddressException e) {
                            this.connect();
                        }
                        closeables.add(socketChannel);

                        // the registration has be be run on the same thread as the selector
                        pendingRegistrations.add(new Runnable() {
                            @Override
                            public void run() {
                                final Attached attached = new Attached();
                                attached.connector = ClientConnector.this;
                                try {
                                    socketChannel.register(selector, OP_CONNECT, attached);
                                    if (throttler != null)
                                        throttler.add(socketChannel);
                                } catch (ClosedChannelException e) {
                                    if (socketChannel.isOpen())
                                        LOG.error("", e);
                                }

                            }
                        });

                    }

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
                            throttler.remove(socketChannel);
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
                externalizable, packetSize, tcpReplicatorBuilder.heartBeatInterval());

        channel.register(selector, OP_WRITE | OP_READ, attached);

        // register it with the selector and store the ModificationIterator for this key
        attached.entryWriter.identifierToBuffer(identifier);
    }


    private void onAccept(SelectionKey key,
                          final int serializedEntrySize,
                          final EntryExternalizable externalizable,
                          final short packetSize,
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

        attached.entryReader = new TcpSocketChannelEntryReader(serializedEntrySize,
                externalizable, packetSize);

        attached.entryWriter = new TcpSocketChannelEntryWriter(serializedEntrySize,
                externalizable, packetSize, tcpReplicatorBuilder.heartBeatInterval());

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
                attached.connector.connect();
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
                attached.connector.connect();
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


    static class Attached extends AbstractAttached {

        public TcpSocketChannelEntryReader entryReader;
        public ReplicatedSharedHashMap.ModificationIterator remoteModificationIterator;
        public AbstractConnector connector;


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
        public long lastHeartBeatReceived = System.currentTimeMillis();


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

