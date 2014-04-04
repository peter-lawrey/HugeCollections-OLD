package net.openhft.chronicle.sandbox.queue.locators.shared;

import net.openhft.chronicle.sandbox.queue.locators.DataLocator;

/**
 * Created by Rob Austin
 */
public class SharedLocalDataLocator<E> implements DataLocator<E> {

    // intentionally not volatile, as we are carefully ensuring that the memory barriers are controlled below by other objects
    private final Object[] data;
    private final int capacity;
    private final int valueMaxSize;


    /**
     * @param capacity
     * @param valueMaxSize the maximum size that an value can be, if a value is written to the buffer that is larger than this an exception will be thrown
     */
    public SharedLocalDataLocator(int capacity, int valueMaxSize) {
        this.valueMaxSize = valueMaxSize;

        // the ring buffer works by having 1 item spare, that's why we add one
        this.capacity = capacity + 1;

        this.data = new Object[this.capacity];
    }

    @Override
    public E getData(final int index) {
        return (E) data[index];
    }

    @Override
    public void setData(final int index, final E value) {
        data[index] = value;

    }

    @Override
    public void writeAll(final E[] newData, final int length) {
        System.arraycopy(newData, 0, data, 0, length);
    }

    @Override
    public int getCapacity() {
        return capacity;
    }
}
