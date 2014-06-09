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
import java.net.SocketAddress;
import java.nio.channels.*;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.nio.channels.SelectionKey.OP_WRITE;

/**
 * @author Rob Austin.
 */
abstract class AbstractChannelReplicator implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractChannelReplicator.class.getName());
    static final int BITS_IN_A_BYTE = 8;

    final ExecutorService executorService;
    final Selector selector;
    final Set<Closeable> closeables = new CopyOnWriteArraySet<Closeable>();

    final int throttleInterval = 100;
    long maxBytesInInterval;


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
        for (Runnable runnable = selectableChannels.poll(); runnable != null; runnable = selectableChannels.poll()) {
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
        }
        closeables.clear();
        executorService.shutdownNow();
    }

    static class AbstractAttached {
        AbstractConnector connector;
    }

    /**
     * throttles 'writes' to ensure the network is not swamped, this is achieved by periodically
     * de-registering the write selector during periods of high volume.
     */
    static class Throttler {

        private long lastTime = System.currentTimeMillis();
        private final Set<SelectableChannel> channels = new CopyOnWriteArraySet<SelectableChannel>();
        private long byteWritten;
        private Selector selector;
        private final int throttleInterval;
        private final long maxBytesInInterval;


        Throttler(@NotNull Selector selector,
                  int throttleInterval,
                  long maxBytesInInterval) {

            this.selector = selector;
            this.throttleInterval = throttleInterval;
            this.maxBytesInInterval = maxBytesInInterval;
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
            byteWritten = 0;

            for (SelectableChannel selectableChannel : channels) {

                Object attachment = null;

                try {
                    final SelectionKey selectionKey = selectableChannel.keyFor(selector);
                    attachment = selectionKey.attachment();
                    selectableChannel.register(selector, OP_WRITE, attachment);


                } catch (IOException e) {
                    if (LOG.isDebugEnabled())
                        LOG.debug("", e);
                    try {

                        if (attachment != null)
                            ((AbstractAttached) attachment).connector.connect();
                    } catch (Exception e1) {
                        LOG.error("", e);
                    }
                }
            }
        }


        /**
         * checks the number of bytes written in the interval, so see if we should de-register the 'write' on
         * the selector, If it has it deRegisters the selector
         *
         * @param len the number of bytes just written
         * @throws ClosedChannelException
         */
        public void checkUnregisterSelector(int len) throws ClosedChannelException {
            byteWritten += len;
            if (byteWritten > maxBytesInInterval) {

                for (SelectableChannel channel : channels) {
                    channel.register(selector, 0);

                    if (LOG.isDebugEnabled())
                        LOG.debug("Throttling UDP writes");
                }
            }
        }


    }

    static class Details {

        private final SocketAddress address;
        private final Queue<Runnable> pendingRegistrations;
        private final Set<Closeable> closeables;

        private final byte identifier;

        Details(@NotNull SocketAddress address,
                @NotNull Set<Closeable> closeables,
                byte identifier,
                @NotNull Queue<Runnable> pendingRegistrations) {
            this.address = address;
            this.pendingRegistrations = pendingRegistrations;
            this.closeables = closeables;

            this.identifier = identifier;
        }

        public SocketAddress getAddress() {
            return address;
        }


        public Set<Closeable> getCloseables() {
            return closeables;
        }


        public byte getIdentifier() {
            return identifier;
        }


    }

    static abstract class AbstractConnector {

        int connectionAttempts;

        private final String name;
        private final Details details;

        private volatile SelectableChannel socketChannel;

        public AbstractConnector(String name, @NotNull final Details details) {
            this.name = name;
            this.details = details;
        }

        abstract SelectableChannel doConnect() throws IOException, InterruptedException;

        /**
         * if its already connected then the existing connection is close and its reconnected
         *
         * @throws IOException
         * @throws InterruptedException
         */
        public final void connect() {
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

            final Thread thread = new Thread(new Runnable() {

                public void run() {
                    SelectableChannel socketChannel = null;
                    try {
                        if (reconnectionInterval > 0)
                            Thread.sleep(reconnectionInterval);

                        synchronized (details.closeables) {
                            socketChannel = doConnect();
                            if (socketChannel == null)
                                return;

                            details.closeables.add(socketChannel);
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

}
