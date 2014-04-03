package net.openhft.chronicle.sandbox.queue;

import net.openhft.chronicle.sandbox.queue.locators.LazyVolatileBufferIndexLocator;

import java.util.Collection;

/**
 * Created by Rob Austin
 */
public class LazyVolatileConcurrentBlockingObjectQueue extends AbstractConcurrentBlockingObjectQueue {

    /**
     * Creates an BlockingQueue with the default capacity of 1024
     */
    public LazyVolatileConcurrentBlockingObjectQueue() {
        super(new LazyVolatileBufferIndexLocator());
    }

    /**
     * @param capacity Creates an BlockingQueue with the given (fixed) capacity
     */
    public LazyVolatileConcurrentBlockingObjectQueue(int capacity) {
        super(capacity, new LazyVolatileBufferIndexLocator());
    }

    public LazyVolatileConcurrentBlockingObjectQueue(int capacity, boolean b) {
        super(capacity, new LazyVolatileBufferIndexLocator());
    }

    public LazyVolatileConcurrentBlockingObjectQueue(int capacity, boolean b, Collection<Integer> elements) {
        super(capacity, new LazyVolatileBufferIndexLocator());
    }

}
