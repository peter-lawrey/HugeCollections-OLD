package net.openhft.chronicle.sandbox.queue;

import net.openhft.chronicle.sandbox.queue.locators.LazyVolatileBufferIndexLocator;
import net.openhft.chronicle.sandbox.queue.locators.LocalDataLocator;

import java.util.Collection;

/**
 * Created by Rob Austin
 */
public class LazyVolatileConcurrentBlockingObjectQueue extends AbstractConcurrentBlockingObjectQueue {

    public static final int DEFAULT_CAPACITY = 1024;

    /**
     * Creates an BlockingQueue with the default capacity of 1024
     */
    public LazyVolatileConcurrentBlockingObjectQueue() {
        super(new LazyVolatileBufferIndexLocator(), new LocalDataLocator(DEFAULT_CAPACITY));
    }

    /**
     * @param capacity Creates an BlockingQueue with the given (fixed) capacity
     */
    public LazyVolatileConcurrentBlockingObjectQueue(int capacity) {
        super(new LazyVolatileBufferIndexLocator(), new LocalDataLocator(capacity));
    }

    public LazyVolatileConcurrentBlockingObjectQueue(int capacity, boolean b) {
        super(new LazyVolatileBufferIndexLocator(), new LocalDataLocator(capacity));
    }

    public LazyVolatileConcurrentBlockingObjectQueue(int capacity, boolean b, Collection<Integer> elements) {
        super(new LazyVolatileBufferIndexLocator(), new LocalDataLocator(capacity));
    }

}
