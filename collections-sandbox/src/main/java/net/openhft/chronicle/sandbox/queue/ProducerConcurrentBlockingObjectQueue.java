package net.openhft.chronicle.sandbox.queue;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;

import static net.openhft.chronicle.sandbox.queue.ConcurrentBlockingObjectQueueBuilder.Type.REMOTE_PRODUCER;

/**
 * Created by Rob Austin
 */
public class ProducerConcurrentBlockingObjectQueue<E> extends BlockingQueueDelegate<E> {

    final ConcurrentBlockingObjectQueueBuilder<E> builder = new ConcurrentBlockingObjectQueueBuilder<E>();
    final BlockingQueue<E> delegate;

    /**
     * @param capacity Creates an BlockingQueue with the given (fixed) capacity
     * @param clazz
     */
    public ProducerConcurrentBlockingObjectQueue(int capacity, Class<E> clazz) throws IOException {
        builder.setCapacity(capacity);

        builder.setClazz(clazz);
        builder.setType(REMOTE_PRODUCER);
        builder.setPort(8095);

        //todo change the way we default the max size, there is an unsafe method we can use
        builder.setMaxSize(10);


        delegate = builder.create();
    }

    // todo
    public ProducerConcurrentBlockingObjectQueue(int capacity, boolean b, Class<E> clazz) throws IOException {
        this(capacity, clazz);
    }

    // todo
    public ProducerConcurrentBlockingObjectQueue(int capacity, boolean b, Collection<E> elements, Class<E> clazz) throws IOException {
        this(capacity, clazz);
    }


    @Override
    protected BlockingQueue<E> getDelegate() {
        return delegate;
    }
}
