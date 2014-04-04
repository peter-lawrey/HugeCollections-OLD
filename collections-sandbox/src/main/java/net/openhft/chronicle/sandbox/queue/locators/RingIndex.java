package net.openhft.chronicle.sandbox.queue.locators;

/**
 * Created by Rob Austin
 */
public interface RingIndex {

    int getWriterLocation();

    void setWriterLocation(int nextWriteLocation);

    int getReadLocation();

    void setReadLocation(int nextReadLocation);

}
