package net.openhft.chronicle.sandbox.queue;

import net.openhft.chronicle.sandbox.queue.locators.BufferIndexLocator;
import net.openhft.chronicle.sandbox.queue.locators.DataLocator;
import net.openhft.chronicle.sandbox.queue.locators.local.LazyVolatileBufferIndexLocator;
import net.openhft.chronicle.sandbox.queue.locators.local.LocalDataLocator;
import net.openhft.chronicle.sandbox.queue.locators.shared.SharedBufferIndexLocator;
import net.openhft.chronicle.sandbox.queue.locators.shared.SharedLocalDataLocator;
import net.openhft.lang.io.DirectBytes;
import net.openhft.lang.io.MappedStore;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.concurrent.BlockingQueue;

/**
 * Created by Rob Austin
 */
public class ConcurrentBlockingObjectQueueBuilder<E> {

    private int capacity;
    private boolean isShared;
    private int maxSize;

    public void setMaxSize(int maxSize) {
        this.maxSize = maxSize;
    }

    public int getCapacity() {
        return capacity;
    }

    public void setCapacity(int capacity) {
        this.capacity = capacity;
    }

    public void isShared(boolean isShared) {
        this.isShared = isShared;
    }


    public BlockingQueue<E> create() throws IOException {

        final DataLocator dataLocator;
        final BufferIndexLocator bufferIndexLocator;

        if (isShared) {


            final String tmp = System.getProperty("java.io.tmpdir");
            final File file = new File(tmp + "/share-queue-test" + System.nanoTime());
            final MappedStore ms = new MappedStore(file, FileChannel.MapMode.READ_WRITE, 8);
            final DirectBytes indexLocationBytes = ms.createSlice(0, 8);

            dataLocator = new SharedLocalDataLocator(capacity, maxSize);
            bufferIndexLocator = new SharedBufferIndexLocator(indexLocationBytes);


        } else {
            bufferIndexLocator = new LazyVolatileBufferIndexLocator();
            dataLocator = new LocalDataLocator(capacity);
        }

        return new ConcurrentBlockingObjectQueue<E>(bufferIndexLocator, dataLocator);

    }

}
