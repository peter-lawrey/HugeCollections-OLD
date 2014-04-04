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

    public LocalConcurrentBlockingObjectQueue() throws IOException {
        this(1024);
    }

    /**
     * @param capacity Creates an BlockingQueue with the given (fixed) capacity
     */
    public LocalConcurrentBlockingObjectQueue(int capacity) {
        builder.setCapacity(capacity);
        builder.isShared(false);

        BlockingQueue<E> delegate0 = null;
        try {
            delegate0 = builder.create();
        } catch (IOException e) {
            // this won't occur in the local version
        }

        delegate = delegate0;
    }

    public LocalConcurrentBlockingObjectQueue(int capacity, boolean b) {
        this(capacity);
    }

    public LocalConcurrentBlockingObjectQueue(int capacity, boolean b, Collection<Integer> elements) {
        this(capacity);
    }

    @Override
    protected BlockingQueue<E> getDelegate() {
        return delegate;
    }
}
