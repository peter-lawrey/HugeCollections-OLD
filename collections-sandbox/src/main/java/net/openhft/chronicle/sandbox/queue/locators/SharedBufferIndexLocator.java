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
    public int getWriteLocation() {
        return directBytes.readInt(WRITE_OFFSET);
    }

    @Override
    public void setWriteLocation(int nextWriteLocation) {
        directBytes.writeInt(nextWriteLocation, WRITE_OFFSET);
    }

    @Override
    public int getReadLocation() {
        return directBytes.readInt(READ_OFFSET);

    }

    @Override
    public void setReadLocation(int nextReadLocation) {
        directBytes.writeInt(nextReadLocation, READ_OFFSET);
    }
}
