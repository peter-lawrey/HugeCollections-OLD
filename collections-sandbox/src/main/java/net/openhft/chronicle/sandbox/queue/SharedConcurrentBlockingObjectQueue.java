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


    public SharedConcurrentBlockingObjectQueue(Class<E> clazz) throws IOException {
        this(1024, clazz);
    }

    /**
     * @param capacity Creates an BlockingQueue with the given (fixed) capacity
     * @param clazz
     */
    public SharedConcurrentBlockingObjectQueue(int capacity, Class<E> clazz) throws IOException {
        builder.setCapacity(capacity);
        builder.isShared(true);
        builder.setClazz(clazz);

        delegate = builder.create();
    }

    public SharedConcurrentBlockingObjectQueue(int capacity, boolean b, Class<E> clazz) throws IOException {
        this(capacity, clazz);
    }

    public SharedConcurrentBlockingObjectQueue(int capacity, boolean b, Collection<Integer> elements, Class<E> clazz) throws IOException {
        this(capacity, clazz);
    }


    @Override
    protected BlockingQueue<E> getDelegate() {
        return delegate;
    }
}
