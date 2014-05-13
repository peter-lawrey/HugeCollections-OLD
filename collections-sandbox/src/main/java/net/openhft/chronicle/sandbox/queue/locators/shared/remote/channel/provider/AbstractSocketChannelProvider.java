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

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Rob Austin.
 */
abstract class AbstractSocketChannelProvider implements SocketChannelProvider {

    final AtomicReference<SocketChannel> socketChannel = new AtomicReference<SocketChannel>();
    final CountDownLatch latch = new CountDownLatch(1);

    public SocketChannel getSocketChannel() throws InterruptedException {

        final SocketChannel result = socketChannel.get();
        if (result != null)
            return result;

        latch.await();
        return socketChannel.get();
    }

    @Override
    public abstract void close() throws IOException;


}
