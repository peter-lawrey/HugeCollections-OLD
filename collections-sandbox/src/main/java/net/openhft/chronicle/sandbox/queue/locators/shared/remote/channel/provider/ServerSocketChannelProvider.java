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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

/**
 * Created by Rob Austin
 */
public class ServerSocketChannelProvider extends AbstractSocketChannelProvider implements SocketChannelProvider {

    public static final int RECEIVE_BUFFER_SIZE = 256 * 1024;
    private static Logger LOG = LoggerFactory.getLogger(ServerSocketChannelProvider.class);
    private volatile ServerSocketChannel serverSocket;
    volatile boolean closed;
    private final Thread thread;

    public ServerSocketChannelProvider(final int port) {

        thread = new Thread(new Runnable() {

            @Override
            public void run() {
                while (!closed) {

                    try {
                        if (closed)
                            return;

                        serverSocket = ServerSocketChannel.open();
                        serverSocket.socket().setReuseAddress(true);
                        serverSocket.socket().bind(new InetSocketAddress(port));
                        serverSocket.configureBlocking(true);
                        LOG.info("Server waiting for client on port " + port);
                        serverSocket.socket().setReceiveBufferSize(RECEIVE_BUFFER_SIZE);
                        final SocketChannel result = serverSocket.accept();

                        socketChannel.set(result);
                        latch.countDown();
                        return;
                    } catch (Exception e) {
                        if (closed)
                            return;
                        LOG.warn("port={}", port, e);
                        if (serverSocket != null) {
                            try {
                                serverSocket.close();
                            } catch (IOException e1) {
                                LOG.warn("port={}", port, e);
                            }
                        }
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        LOG.warn("port={}", port, e);
                    }
                }
            }

        });
        thread.start();

    }


    public void close() throws IOException {
        closed = true;
        thread.interrupt();
        if (serverSocket != null) {
            serverSocket.close();
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                LOG.warn("", e);
            }
        }

    }
}
