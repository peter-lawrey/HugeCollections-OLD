package net.openhft.chronicle.sandbox.queue.locators.shared;

/**
 * can either be a writer index or a reader index
 */
public interface Index {

    /**
     * if this is being used for a producer then it will setReadLocation, if its being used by a producer it will setWriteLocation
     *
     * @param index the index to be set
     */
    void setNextLocation(int index);

    /**
     * get the index where the data is stored in the byte buffer
     */
    int getWriterLocation();
}
