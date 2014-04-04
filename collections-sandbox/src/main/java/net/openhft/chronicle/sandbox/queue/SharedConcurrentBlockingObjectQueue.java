package net.openhft.chronicle.sandbox.queue;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;

/**
 * Created by Rob Austin
 */
public class SharedConcurrentBlockingObjectQueue<E> extends BlockingQueueDelegate<E> {

    final ConcurrentBlockingObjectQueueBuilder<E> builder = new ConcurrentBlockingObjectQueueBuilder<E>();
    final BlockingQueue<E> delegate;


    public SharedConcurrentBlockingObjectQueue() throws IOException {
        this(1024);
    }

    /**
     * @param capacity Creates an BlockingQueue with the given (fixed) capacity
     */
    public SharedConcurrentBlockingObjectQueue(int capacity) throws IOException {
        builder.setCapacity(capacity);
        builder.isShared(true);
        delegate = builder.create();
    }

    public SharedConcurrentBlockingObjectQueue(int capacity, boolean b) throws IOException {
        this(capacity);
    }

    public SharedConcurrentBlockingObjectQueue(int capacity, boolean b, Collection<Integer> elements) throws IOException {
        this(capacity);
    }


    @Override
    protected BlockingQueue<E> getDelegate() {
        return delegate;
    }
}
