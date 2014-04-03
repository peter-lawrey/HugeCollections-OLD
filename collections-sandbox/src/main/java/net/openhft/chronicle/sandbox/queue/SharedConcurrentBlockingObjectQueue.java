package net.openhft.chronicle.sandbox.queue;

import net.openhft.chronicle.sandbox.queue.locators.LazyVolatileBufferIndexLocator;
import net.openhft.chronicle.sandbox.queue.locators.SharedBufferIndexLocator;

import java.io.IOException;
import java.util.Collection;

/**
 * Created by Rob Austin
 */
public class SharedConcurrentBlockingObjectQueue<E> extends AbstractConcurrentBlockingObjectQueue<E> {

    /**
     * Creates an BlockingQueue with the default capacity of 1024
     */
    public SharedConcurrentBlockingObjectQueue() {
        super(new LazyVolatileBufferIndexLocator());
    }

    /**
     * @param capacity Creates an BlockingQueue with the given (fixed) capacity
     */
    public SharedConcurrentBlockingObjectQueue(int capacity) throws IOException {
        super(capacity, new SharedBufferIndexLocator());
    }

    public SharedConcurrentBlockingObjectQueue(int capacity, boolean b) throws IOException {
        super(capacity, new SharedBufferIndexLocator());
    }

    public SharedConcurrentBlockingObjectQueue(int capacity, boolean b, Collection<Integer> elements) throws IOException {
        super(capacity, new SharedBufferIndexLocator());
    }

}
