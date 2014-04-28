/*
 * Copyright 2014 Higher Frequency Trading
 * <p/>
 * http://www.higherfrequencytrading.com
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.sandbox.queue.locators.local;

import net.openhft.chronicle.sandbox.queue.locators.RingIndex;
import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * Created by Rob Austin
 */
public class LocalRingIndex implements RingIndex {

    private static final long READ_LOCATION_OFFSET;
    private static final long WRITE_LOCATION_OFFSET;
    private static final Unsafe unsafe;

    static {
        try {
            final Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);
            READ_LOCATION_OFFSET = unsafe.objectFieldOffset
                    (LocalRingIndex.class.getDeclaredField("readLocation"));
            WRITE_LOCATION_OFFSET = unsafe.objectFieldOffset
                    (LocalRingIndex.class.getDeclaredField("writeLocation"));
        } catch (Exception e) {
            throw new AssertionError(e);
        }

    }

    volatile int readLocation;
    volatile int writeLocation;
    private int producerWriteLocation;

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

    @Override
    public int getProducerWriteLocation() {
        return this.producerWriteLocation;
    }

    @Override
    public void setProducerWriteLocation(int nextWriteLocation) {
        this.producerWriteLocation = nextWriteLocation;
    }
}
