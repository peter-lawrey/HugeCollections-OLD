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

import net.openhft.lang.model.constraints.NotNull;
import net.openhft.lang.thread.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.lang.Math.round;
import static java.nio.channels.SelectionKey.OP_WRITE;
import static java.util.concurrent.TimeUnit.DAYS;

/**
 * @author Rob Austin.
 */
abstract class AbstractChannelReplicator implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractChannelReplicator.class);
    static final int BITS_IN_A_BYTE = 8;

    final ExecutorService executorService;
    final Selector selector;
    final Set<Closeable> closeables = Collections.synchronizedSet(new LinkedHashSet<Closeable>());

    AbstractChannelReplicator(String name) throws IOException {
        executorService = Executors.newSingleThreadExecutor(
                new NamedThreadFactory(name, true));
        selector = Selector.open();
        closeables.add(selector);
    }


    /**
     * Registers the SocketChannel with the selector
     *
     * @param selectableChannels the SelectableChannel to register
     * @throws ClosedChannelException
     */
    void register(@NotNull final Queue<Runnable> selectableChannels) throws
            ClosedChannelException {
        for (Runnable runnable = selectableChannels.poll(); runnable != null;
             runnable = selectableChannels.poll()) {
            try {
                runnable.run();
            } catch (Exception e) {
                LOG.info("", e);
            }
        }
    }


    @Override
    public void close() {
        synchronized (this.closeables) {
            for (Closeable closeable : this.closeables) {
                try {
                    closeable.close();
                } catch (IOException e) {
                    LOG.error("", e);
                }
            }
            closeables.clear();
        }
        executorService.shutdownNow();
    }

    /**
     * forces the TCP and UDP replicators to rebootstrap
     */
    public abstract void forceBootstrap();

    /**
     * throttles 'writes' to ensure the network is not swamped, this is achieved by periodically
     * de-registering the write selector during periods of high volume.
     */
    static class Throttler {

        private final Selector selector;
        private final Set<SelectableChannel> channels = new CopyOnWriteArraySet<SelectableChannel>();
        private final long throttleInterval;
        private final long maxBytesInInterval;

        private long lastTime = System.currentTimeMillis();
        private long bytesWritten;

        Throttler(@NotNull Selector selector,
                  long throttleIntervalInMillis,
                  long serializedEntrySize,
                  long bitsPerDay) {

            this.selector = selector;
            this.throttleInterval = throttleIntervalInMillis;
            double bytesPerMs = ((double) bitsPerDay) / DAYS.toMillis(1) / BITS_IN_A_BYTE;
            this.maxBytesInInterval = round(bytesPerMs * throttleInterval) - serializedEntrySize;
        }

        public void add(SelectableChannel selectableChannel) {
            channels.add(selectableChannel);
        }

        public void remove(SelectableChannel socketChannel) {
            channels.remove(socketChannel);
        }

        /**
         * re register the 'write' on the selector if the throttleInterval has passed
         *
         * @throws java.nio.channels.ClosedChannelException
         */
        public void checkThrottleInterval() throws ClosedChannelException {
            final long time = System.currentTimeMillis();

            if (lastTime + throttleInterval >= time)
                return;

            lastTime = time;
            bytesWritten = 0;


            if (LOG.isDebugEnabled())
                LOG.debug("Restoring OP_WRITE on all channels");

            for (SelectableChannel selectableChannel : channels) {

                final SelectionKey selectionKey = selectableChannel.keyFor(selector);
                if (selectionKey != null)
                    selectionKey.interestOps(selectionKey.interestOps() | OP_WRITE);

            }
        }


        /**
         * checks the number of bytes written in this interval, if this number of bytes exceeds a threshold,
         * the selected will de-register the socket that is being written to, until the interval is finished.
         *
         * @param len the number of bytes just written
         * @throws ClosedChannelException
         */
        public void contemplateUnregisterWriteSocket(int len) throws ClosedChannelException {
            bytesWritten += len;
            if (bytesWritten > maxBytesInInterval) {
                for (SelectableChannel channel : channels) {
                    final SelectionKey selectionKey = channel.keyFor(selector);
                    if (selectionKey != null)
                        selectionKey.interestOps(selectionKey.interestOps() & ~OP_WRITE);

                    if (LOG.isDebugEnabled())
                        LOG.debug("Throttling UDP writes");
                }
            }
        }
    }

    /**
     * details about the socket connection
     */
    static class Details {

        private final InetSocketAddress address;
        private final byte localIdentifier;

        Details(@NotNull final InetSocketAddress address, final byte localIdentifier) {
            this.address = address;
            this.localIdentifier = localIdentifier;
        }

        public InetSocketAddress address() {
            return address;
        }

        public byte localIdentifier() {
            return localIdentifier;
        }

        @Override
        public String toString() {
            return "Details{" +
                    "address=" + address +
                    ", localIdentifier=" + localIdentifier +
                    '}';
        }
    }

    abstract class AbstractConnector {

        private final String name;
        private int connectionAttempts = 0;
        private volatile SelectableChannel socketChannel;

        public AbstractConnector(String name) {
            this.name = name;
        }

        abstract SelectableChannel doConnect() throws IOException, InterruptedException;

        /**
         * connects or reconnects, but first waits a period of time proportional to the {@code
         * connectionAttempts}
         */
        public final void connectLater() {
            try {
                if (socketChannel != null)
                    socketChannel.close();
                socketChannel = null;

            } catch (IOException e1) {
                LOG.error("", e1);
            }

            final long reconnectionInterval = connectionAttempts * 100;
            if (connectionAttempts < 5)
                connectionAttempts++;
            doConnect(reconnectionInterval);

        }

        /**
         * connects or reconnects immediately
         */
        public void connect() {
            doConnect(0);
        }


        /**
         * @param reconnectionInterval the period to wait before connecting
         */
        private void doConnect(final long reconnectionInterval) {

            final Thread thread = new Thread(new Runnable() {

                public void run() {
                    SelectableChannel socketChannel = null;
                    try {
                        if (reconnectionInterval > 0)
                            Thread.sleep(reconnectionInterval);

                        synchronized (closeables) {
                            socketChannel = doConnect();
                            if (socketChannel == null)
                                return;

                            closeables.add(socketChannel);
                            AbstractConnector.this.socketChannel = socketChannel;
                        }

                    } catch (Exception e) {
                        if (socketChannel != null)
                            try {
                                socketChannel.close();
                                AbstractConnector.this.socketChannel = null;
                            } catch (IOException e1) {
                                //
                            }
                        LOG.error("", e);
                    }

                }
            });

            thread.setName(name);
            thread.setDaemon(true);
            thread.start();
        }

        public void setSuccessfullyConnected() {
            connectionAttempts = 0;
        }
    }

    interface ChannelReplicatorBuilder {
    }

}
