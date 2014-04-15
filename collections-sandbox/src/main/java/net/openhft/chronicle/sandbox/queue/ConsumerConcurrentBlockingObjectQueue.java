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
