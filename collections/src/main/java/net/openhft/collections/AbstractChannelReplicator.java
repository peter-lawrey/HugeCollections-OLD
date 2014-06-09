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
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.Selector;
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
    Throttler throttler;
    final int throttleInterval = 100;
    long maxBytesInInterval;


    AbstractChannelReplicator(String name) throws IOException {
        executorService = Executors.newSingleThreadExecutor(
                new NamedThreadFactory(name, true));
        selector = Selector.open();
        closeables.add(selector);
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


    static class Details {

        private final SocketAddress address;
        private final Queue<SelectableChannel> pendingRegistrations;
        private final Set<Closeable> closeables;
        private long reconnectionInterval;
        private final byte identifier;

        Details(@NotNull SocketAddress address,
                @NotNull Set<Closeable> closeables,
                long reconnectionInterval,
                byte identifier,
                @NotNull Queue<SelectableChannel> pendingRegistrations) {
            this.address = address;
            this.pendingRegistrations = pendingRegistrations;
            this.closeables = closeables;
            this.reconnectionInterval = reconnectionInterval;
            this.identifier = identifier;
        }

        public SocketAddress getAddress() {
            return address;
        }

        public Queue<SelectableChannel> getPendingRegistrations() {
            return pendingRegistrations;
        }

        public Set<Closeable> getCloseables() {
            return closeables;
        }

        public long getReconnectionInterval() {
            return reconnectionInterval;
        }

        public byte getIdentifier() {
            return identifier;
        }

        public void setReconnectionInterval(int reconnectionInterval) {
            this.reconnectionInterval = reconnectionInterval;
        }
    }

    static abstract class AbstractConnector implements Runnable {

        private final Details details;

        public AbstractConnector(@NotNull final Details details) {
            this.details = details;
        }

        abstract SelectableChannel connect() throws IOException, InterruptedException;

        final void reconnect(SelectableChannel server) throws IOException, InterruptedException {
            try {
                server.close();
            } catch (IOException e1) {
                LOG.error("", e1);
            }

            connect();
        }

        public void run() {
            SelectableChannel socketChannel = null;
            try {
                if (details.reconnectionInterval > 0)
                    Thread.sleep(details.reconnectionInterval);
                else
                    details.reconnectionInterval = 500;
                synchronized (details.closeables) {
                    socketChannel = connect();
                    if (socketChannel == null)
                        return;
                    details.pendingRegistrations.add(socketChannel);
                    details.closeables.add(socketChannel);
                }

            } catch (Exception e) {
                if (socketChannel != null)
                    try {
                        socketChannel.close();
                    } catch (IOException e1) {
                        //
                    }
                LOG.error("", e);
            }

        }

    }


    /**
     * throttles 'writes' to ensure the network is not swamped, this is achieved by periodically
     * de-registering the write selector during periods of high volume.
     */
    static class Throttler {

        private long lastTime = System.currentTimeMillis();
        private final Set<SelectableChannel> channels;
        private long byteWritten;
        private Selector selector;
        private final int throttleInterval;
        private final long maxBytesInInterval;
        private final AbstractConnector serverConnector;

        Throttler(@NotNull Set<SelectableChannel> channels,
                  @NotNull Selector selector,
                  int throttleInterval,
                  long maxBytesInInterval,
                  @NotNull AbstractConnector serverConnector) {
            this.channels = channels;
            this.selector = selector;
            this.throttleInterval = throttleInterval;
            this.maxBytesInInterval = maxBytesInInterval;
            this.serverConnector = serverConnector;
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

            for (SelectableChannel channel : channels) {
                try {
                    channel.register(selector, OP_WRITE);
                } catch (IOException e) {
                    if (LOG.isDebugEnabled())
                        LOG.debug("", e);
                    try {
                        serverConnector.reconnect(channel);
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
}
