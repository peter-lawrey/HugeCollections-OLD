package net.openhft.chronicle.sandbox.queue.locators.local;

import net.openhft.chronicle.sandbox.queue.locators.DataLocator;

/**
 * Created by Rob Austin
 */
public class LocalDataLocator<E> implements DataLocator<E> {

    // intentionally not volatile, as we are carefully ensuring that the memory barriers are controlled below by other objects
    private final Object[] data;
    private final int capacity;

    public LocalDataLocator(int capacity) {

        // the ring buffer works by having 1 item spare, that's why we add one
        this.capacity = capacity;
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
