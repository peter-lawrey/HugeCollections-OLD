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

package net.openhft.chronicle.sandbox.queue.locators.shared.remote.channel.provider;

import net.openhft.lang.model.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

/**
 * Created by Rob Austin
 */
public class ClientSocketChannelProvider extends AbstractSocketChannelProvider {

    private static Logger LOG = LoggerFactory.getLogger(ClientSocketChannelProvider.class);

    private volatile boolean closed;
    private final Thread thread;

    public ClientSocketChannelProvider(final int port, @NotNull final String host) {
        // let IllegalArgumentException to be thrown in the main thread
        // if either the host or the post is invalid
        final InetSocketAddress remote = new InetSocketAddress(host, port);

        thread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (!closed) {
                    SocketChannel result = null;
                    try {
                        result = SocketChannel.open(remote);
                        LOG.info("successfully connected to host={} , port={}", host, port);
                        result.socket().setReceiveBufferSize(RECEIVE_BUFFER_SIZE);
                        socketChannel = result;
                        latch.countDown();
                        return;
                    } catch (IOException e) {
                        LOG.warn("host: {}, port: {}, error: {}", host, port, e);
                        if (result != null) {
                            try {
                                result.close();
                            } catch (IOException e1) {
                                LOG.warn("host: {}, port: {}, error: {}", host, port, e1);
                            }
                        }
                    }
                    try {
                        Thread.sleep(DELAY_BETWEEN_CONNECTION_ATTEMPTS_IN_MILLIS);
                    } catch (InterruptedException e) {
                        LOG.warn("host: {}, port: {}, error: {}", host, port, e);
                    }
                }
            }
        });
        thread.start();
    }

    /**
     * {@inheritDoc}
     */
    public void close() throws IOException {
        closed = true;
        thread.interrupt();
        closeAndWait(socketChannel, LOG);
    }

}
