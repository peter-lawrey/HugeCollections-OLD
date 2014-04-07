package net.openhft.chronicle.sandbox.queue;

import net.openhft.chronicle.sandbox.queue.locators.DataLocator;
import net.openhft.chronicle.sandbox.queue.locators.RingIndex;
import net.openhft.chronicle.sandbox.queue.locators.local.LocalDataLocator;
import net.openhft.chronicle.sandbox.queue.locators.local.LocalRingIndex;
import net.openhft.chronicle.sandbox.queue.locators.shared.SharedDataLocator;
import net.openhft.chronicle.sandbox.queue.locators.shared.SharedRingIndex;
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

    public static final int SIZE_OF_INT = 4;

    public static final int LOCK_TIME_OUT_NS = 100;
    private int capacity;
    private boolean isShared;
    private int maxSize;
    private Class<E> clazz;

    /**
     * returns the value to nearest {@parm powerOf2}
     */
    private static int align(int capacity, int powerOf2) {
        return (capacity + powerOf2 - 1) & ~(powerOf2 - 1);
    }

    public void setMaxSize(int maxSize) {
        this.maxSize = maxSize;
    }

    public void setCapacity(int capacity) {
        // the ring buffer works by having 1 item spare, that's why we add one
        this.capacity = capacity + 1;
    }

    public void setClazz(Class<E> clazz) {
        this.clazz = clazz;
    }

    public void isShared(boolean isShared) {
        this.isShared = isShared;
    }

    public BlockingQueue<E> create() throws IOException {

        final DataLocator dataLocator;
        final RingIndex ringIndex;

        if (isShared) {


            final String tmp = System.getProperty("java.io.tmpdir");
            final File file = new File(tmp + "/share-queue-test" + System.nanoTime());

            int ringIndexLocationsStart = 0;
            int ringIndexLocationsLen = SIZE_OF_INT * 2;
            int storeStart = ringIndexLocationsLen;
            int storeLen = capacity * align(maxSize, 4);

            final MappedStore ms = new MappedStore(file, FileChannel.MapMode.READ_WRITE, ringIndexLocationsLen + storeLen);

            final DirectBytes ringIndexSlice = ms.createSlice(ringIndexLocationsStart, ringIndexLocationsLen);
            ringIndex = new SharedRingIndex(ringIndexSlice);

            // provides an index to the data in the ring buffer, the size of this index is proportional to the capacity of the ring buffer
            final DirectBytes storeSlice = ms.createSlice(storeStart, storeLen);
            final DirectBytes writerSlice = ms.createSlice(storeStart, storeLen);
            dataLocator = new SharedDataLocator(clazz, capacity, maxSize, storeSlice, writerSlice);

        } else {
            ringIndex = new LocalRingIndex();
            dataLocator = new LocalDataLocator(capacity);
        }

        return new ConcurrentBlockingObjectQueue<E>(ringIndex, dataLocator);

    }


}
