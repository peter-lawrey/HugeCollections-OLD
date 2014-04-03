package net.openhft.chronicle.sandbox.queue.locators;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * Created by Rob Austin
 */
public class VolatileBufferIndexLocator implements BufferIndexLocator {

    private static final long READ_LOCATION_OFFSET;
    private static final long WRITE_LOCATION_OFFSET;
    private static final Unsafe unsafe;

    static {
        try {
            final Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);
            READ_LOCATION_OFFSET = unsafe.objectFieldOffset
                    (VolatileBufferIndexLocator.class.getDeclaredField("readLocation"));
            WRITE_LOCATION_OFFSET = unsafe.objectFieldOffset
                    (VolatileBufferIndexLocator.class.getDeclaredField("writeLocation"));
        } catch (Exception e) {
            throw new AssertionError(e);
        }

    }

    volatile int readLocation;
    volatile int writeLocation;

    @Override
    public int getWriteLocation() {
        return writeLocation;
    }

    @Override
    public void setWriteLocation(int location) {
        writeLocation = location;
    }

    @Override
    public int getReadLocation() {
        return readLocation;
    }

    @Override
    public void setReadLocation(int location) {
        readLocation = location;
    }

    @Override
    public void lazySetReadLocation(int nextReadLocation) {
        unsafe.putOrderedInt(this, READ_LOCATION_OFFSET, nextReadLocation);
    }

    @Override
    public void lazySetWriteLocation(int nextWriteLocation) {
        unsafe.putOrderedInt(this, WRITE_LOCATION_OFFSET, nextWriteLocation);
    }
}
