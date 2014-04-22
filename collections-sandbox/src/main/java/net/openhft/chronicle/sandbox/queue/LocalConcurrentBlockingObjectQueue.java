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

/**
 * Acts a delegate wrapping a default builder
 */
public class LocalConcurrentBlockingObjectQueue<E> extends BlockingQueueDelegate<E> {

    final ConcurrentBlockingObjectQueueBuilder<E> builder = new ConcurrentBlockingObjectQueueBuilder<E>();
    final BlockingQueue<E> delegate;

    /**
     * @param capacity Creates an BlockingQueue with the given (fixed) capacity
     */
    public LocalConcurrentBlockingObjectQueue(int capacity) {
        builder.setCapacity(capacity);

        BlockingQueue<E> delegate0 = null;
        try {
            delegate0 = builder.create();
        } catch (IOException e) {
            // this won't occur in the local version
        }

        delegate = delegate0;
    }


    // todo
    public LocalConcurrentBlockingObjectQueue(int capacity, boolean b, Collection<Integer> elements) {
        this(capacity);
    }

    @Override
    protected BlockingQueue<E> getDelegate() {
        return delegate;
    }
}
