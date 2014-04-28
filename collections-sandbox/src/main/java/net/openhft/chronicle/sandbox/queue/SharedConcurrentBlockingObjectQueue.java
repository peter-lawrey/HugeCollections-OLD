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

import static net.openhft.chronicle.sandbox.queue.ConcurrentBlockingObjectQueueBuilder.Type.SHARED;

/**
 * Created by Rob Austin
 */
public class SharedConcurrentBlockingObjectQueue<E> extends BlockingQueueDelegate<E> {

    final ConcurrentBlockingObjectQueueBuilder<E> builder = new ConcurrentBlockingObjectQueueBuilder<E>();
    final BlockingQueue<E> delegate;


    /**
     * @param capacity Creates an BlockingQueue with the given (fixed) capacity
     * @param clazz
     */
    public SharedConcurrentBlockingObjectQueue(int capacity, Class<E> clazz) throws IOException {
        builder.setCapacity(capacity);
        builder.setType(SHARED);
        builder.setClazz(clazz);


        //todo change the way we default the max size, there is an unsafe method we can use
        // builder.setMaxSize(10);
        delegate = builder.create();
    }

    // todo
    public SharedConcurrentBlockingObjectQueue(int capacity, boolean b, Class<E> clazz) throws IOException {
        this(capacity, clazz);
    }

    // todo
    public SharedConcurrentBlockingObjectQueue(int capacity, boolean b, Collection<E> elements, Class<E> clazz) throws IOException {
        this(capacity, clazz);
    }


    @Override
    public BlockingQueue<E> getDelegate() {
        return delegate;
    }
}
