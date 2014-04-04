package net.openhft.chronicle.sandbox.queue.locators;

import net.openhft.lang.io.DirectBytes;
import net.openhft.lang.io.MappedStore;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * Created by Rob Austin
 */
public class SharedBufferIndexLocator implements BufferIndexLocator {

    private static final int READ_OFFSET = 0;
    private static final int WRITE_OFFSET = READ_OFFSET + 4;
    final DirectBytes directBytes;

    public SharedBufferIndexLocator() throws IOException {
        final String tmp = System.getProperty("java.io.tmpdir");
        final File file = new File(tmp + "/share-queue-test" + System.nanoTime());
        this.directBytes = new MappedStore(file, FileChannel.MapMode.READ_WRITE, 8).createSlice();
    }

    @Override
    public int getWriterLocation() {
        return directBytes.readVolatileInt(WRITE_OFFSET);
    }

    @Override
    public void setWriterLocation(int nextWriteLocation) {
        directBytes.writeOrderedInt(WRITE_OFFSET, nextWriteLocation);
    }

    @Override
    public int getReadLocation() {
        return directBytes.readVolatileInt(READ_OFFSET);

    }

    @Override
    public void setReadLocation(int nextReadLocation) {
        directBytes.writeOrderedInt(READ_OFFSET, nextReadLocation);
    }
}
