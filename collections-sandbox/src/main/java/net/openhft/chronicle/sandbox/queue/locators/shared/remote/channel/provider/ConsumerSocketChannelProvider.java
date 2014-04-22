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

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by Rob Austin
 */
public class ConsumerSocketChannelProvider implements SocketChannelProvider {

    public static final int RECEIVE_BUFFER_SIZE = 256 * 1024;
    private static Logger LOG = Logger.getLogger(ConsumerSocketChannelProvider.class.getName());
    private final AtomicReference<SocketChannel> socketChannel = new AtomicReference<SocketChannel>();
    private final CountDownLatch latch = new CountDownLatch(1);

    public ConsumerSocketChannelProvider(final int port, @NotNull final String host) {

        new Thread(new Runnable() {
            @Override
            public void run() {

                SocketChannel result = null;
                try {
                    result = SocketChannel.open(new InetSocketAddress(host, port));
                    result.socket().setReceiveBufferSize(RECEIVE_BUFFER_SIZE);

                    socketChannel.set(result);
                    latch.countDown();
                } catch (Exception e) {
                    LOG.log(Level.SEVERE, "", e);
                    if (result != null)
                        try {
                            result.close();
                        } catch (IOException e1) {
                            e1.printStackTrace();
                        }
                }
            }
        }).start();
    }


    @Override
    public SocketChannel getSocketChannel() throws IOException, InterruptedException {

        final SocketChannel result = socketChannel.get();
        if (result != null)
            return result;

        latch.await();
        return socketChannel.get();
    }
}
