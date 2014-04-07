package net.openhft.chronicle.sandbox.queue.locators;

/**
 * Created by Rob Austin
 */
public interface DataLocator<E> {

    E getData(int readLocation);

    int setData(int index, E value);

    void writeAll(E[] newData, int length);

    int getCapacity();
}
