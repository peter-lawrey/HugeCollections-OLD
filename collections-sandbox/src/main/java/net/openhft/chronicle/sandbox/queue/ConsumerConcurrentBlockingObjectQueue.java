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

package net.openhft.chronicle.sandbox.queue;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;

import static net.openhft.chronicle.sandbox.queue.ConcurrentBlockingObjectQueueBuilder.Type.REMOTE_CONSUMER;

/**
 * Created by Rob Austin
 */
public class ConsumerConcurrentBlockingObjectQueue<E> extends BlockingQueueDelegate<E> {

    final ConcurrentBlockingObjectQueueBuilder<E> builder = new ConcurrentBlockingObjectQueueBuilder<E>();
    final BlockingQueue<E> delegate;

    /**
     * @param capacity Creates an BlockingQueue with the given (fixed) capacity
     * @param clazz
     * @param hostname
     */
    public ConsumerConcurrentBlockingObjectQueue(int capacity, Class<E> clazz, String hostname) throws IOException {
        builder.setCapacity(capacity);

        builder.setClazz(clazz);
        builder.setType(REMOTE_CONSUMER);
        builder.setHost(hostname);

        //todo change the way we default the max size, there is an unsafe method we can use
        builder.setMaxSize(10);

        delegate = builder.create();
    }

    // todo
    public ConsumerConcurrentBlockingObjectQueue(int capacity, boolean b, Class<E> clazz, final String hostname) throws IOException {
        this(capacity, clazz, hostname);
    }

    // todo
    public ConsumerConcurrentBlockingObjectQueue(int capacity, boolean b, Collection<E> elements, Class<E> clazz, final String hostname) throws IOException {
        this(capacity, clazz, hostname);
    }


    @Override
    protected BlockingQueue<E> getDelegate() {
        return delegate;
    }
}
