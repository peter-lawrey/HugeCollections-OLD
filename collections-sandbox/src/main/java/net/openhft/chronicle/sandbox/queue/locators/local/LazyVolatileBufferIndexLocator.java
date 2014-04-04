package net.openhft.chronicle.sandbox.queue.locators.local;

import net.openhft.chronicle.sandbox.queue.locators.BufferIndexLocator;
import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * Created by Rob Austin
 */
public class LazyVolatileBufferIndexLocator implements BufferIndexLocator {

    private static final long READ_LOCATION_OFFSET;
    private static final long WRITE_LOCATION_OFFSET;
    private static final Unsafe unsafe;

    static {
        try {
            final Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);
            READ_LOCATION_OFFSET = unsafe.objectFieldOffset
                    (LazyVolatileBufferIndexLocator.class.getDeclaredField("readLocation"));
            WRITE_LOCATION_OFFSET = unsafe.objectFieldOffset
                    (LazyVolatileBufferIndexLocator.class.getDeclaredField("writeLocation"));
        } catch (Exception e) {
            throw new AssertionError(e);
        }

    }

    volatile int readLocation;
    volatile int writeLocation;

    @Override
    public int getWriterLocation() {
        return writeLocation;
    }

    @Override
    public void setWriterLocation(int nextWriteLocation) {
        unsafe.putOrderedInt(this, WRITE_LOCATION_OFFSET, nextWriteLocation);
    }

    @Override
    public int getReadLocation() {
        return readLocation;
    }

    @Override
    public void setReadLocation(int nextReadLocation) {
        unsafe.putOrderedInt(this, READ_LOCATION_OFFSET, nextReadLocation);
    }
}
