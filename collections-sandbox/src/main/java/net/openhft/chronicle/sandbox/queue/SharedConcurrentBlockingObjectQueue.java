package net.openhft.chronicle.sandbox.queue;

import net.openhft.chronicle.sandbox.queue.locators.LocalDataLocator;
import net.openhft.chronicle.sandbox.queue.locators.SharedBufferIndexLocator;

import java.io.IOException;
import java.util.Collection;

/**
 * Created by Rob Austin
 */
public class SharedConcurrentBlockingObjectQueue<E> extends AbstractConcurrentBlockingObjectQueue<E> {

    public static final int DEFAULT_CAPACITY = 1024;


    /**
     * Creates an BlockingQueue with the default capacity of 1024
     */
    public SharedConcurrentBlockingObjectQueue() throws IOException {

        super(new SharedBufferIndexLocator(), new LocalDataLocator(DEFAULT_CAPACITY));
    }

    /**
     * @param capacity Creates an BlockingQueue with the given (fixed) capacity
     */
    public SharedConcurrentBlockingObjectQueue(int capacity) throws IOException {
        super(new SharedBufferIndexLocator(), new LocalDataLocator(capacity));
    }

    public SharedConcurrentBlockingObjectQueue(int capacity, boolean b) throws IOException {
        super(new SharedBufferIndexLocator(), new LocalDataLocator(capacity));
    }

    public SharedConcurrentBlockingObjectQueue(int capacity, boolean b, Collection<Integer> elements) throws IOException {
        super(new SharedBufferIndexLocator(), new LocalDataLocator(capacity));
    }

}
