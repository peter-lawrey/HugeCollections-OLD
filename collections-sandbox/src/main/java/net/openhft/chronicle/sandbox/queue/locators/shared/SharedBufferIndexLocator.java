package net.openhft.chronicle.sandbox.queue.locators.shared;

import net.openhft.chronicle.sandbox.queue.locators.BufferIndexLocator;
import net.openhft.lang.io.DirectBytes;

import java.io.IOException;

/**
 * Created by Rob Austin
 */
public class SharedBufferIndexLocator implements BufferIndexLocator {

    private static final int READ_OFFSET = 0;
    private static final int WRITE_OFFSET = READ_OFFSET + 4;

    private final DirectBytes indexLocationBytes;

    public SharedBufferIndexLocator(DirectBytes indexLocationBytes) throws IOException {
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
}
