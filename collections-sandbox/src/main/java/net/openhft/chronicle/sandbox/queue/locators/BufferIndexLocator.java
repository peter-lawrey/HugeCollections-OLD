package net.openhft.chronicle.sandbox.queue.locators;

/**
 * Created by Rob Austin
 */
public interface BufferIndexLocator {

    int getWriteLocation();

    int getReadLocation();

    void lazySetReadLocation(int nextReadLocation);

    void lazySetWriteLocation(int nextWriteLocation);

}
