package net.openhft.chronicle.sandbox.queue.locators.shared;

import net.openhft.chronicle.sandbox.queue.locators.RingIndex;
import net.openhft.lang.io.DirectBytes;

import java.io.IOException;

/**
 * Created by Rob Austin
 */
public class SharedRingIndex implements RingIndex {

    private static final int READ_OFFSET = 0;
    private static final int WRITE_OFFSET = READ_OFFSET + 4;

    private final DirectBytes indexLocationBytes;
    private int producerWriteLocation;

    public SharedRingIndex(DirectBytes indexLocationBytes) throws IOException {
        this.indexLocationBytes = indexLocationBytes;
    }

    @Override
    public int getWriterLocation() {
        return indexLocationBytes.readVolatileInt(WRITE_OFFSET);
    }

    @Override
    public void setWriterLocation(int nextWriteLocation) {
        indexLocationBytes.writeOrderedInt(WRITE_OFFSET, nextWriteLocation);
    }

    @Override
    public int getReadLocation() {
        return indexLocationBytes.readVolatileInt(READ_OFFSET);
    }

    @Override
    public void setReadLocation(int nextReadLocation) {
        indexLocationBytes.writeOrderedInt(READ_OFFSET, nextReadLocation);
    }

    @Override
    public int getProducerWriteLocation() {
        return this.producerWriteLocation;
    }

    @Override
    public void setProducerWriteLocation(int nextWriteLocation) {
        this.producerWriteLocation = nextWriteLocation;
    }

    @Override
    public String toString() {
        return "readLocation=" + getReadLocation() + ", writerLocation=" + getWriterLocation();
    }
}
