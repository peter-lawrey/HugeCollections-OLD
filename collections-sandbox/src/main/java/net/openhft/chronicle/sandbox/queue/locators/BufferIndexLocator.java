package net.openhft.chronicle.sandbox.queue.locators;

/**
 * Created by Rob Austin
 */
public interface BufferIndexLocator {
    int getWriteLocation();

    void setWriteLocation(int i);

    int getReadLocation();

    void setReadLocation(int i);


    void lazySetReadLocation(int nextReadLocation);

    void lazySetWriteLocation(int nextWriteLocation);

}
