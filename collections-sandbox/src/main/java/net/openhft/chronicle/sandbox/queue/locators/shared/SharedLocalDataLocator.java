/*
 * Copyright 2013 Peter Lawrey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.sandbox.queue.locators.shared;

import net.openhft.chronicle.sandbox.queue.locators.DataLocator;
import net.openhft.lang.io.DirectBytes;
import org.jetbrains.annotations.NotNull;

/**
 * Created by Rob Austin
 */
public class SharedLocalDataLocator<E> implements DataLocator<E> {

    public static final int ALIGN = 4;
    public static final int LOCK_SIZE = 8;

    // intentionally not volatile, as we are carefully ensuring that the memory barriers are controlled below by other objects
    private final int capacity;
    private final int valueMaxSize;

    private final DirectBytes storeSlice;
    private final Class<E> valueClass;

    /**
     * @param capacity     the maximum number of values that can be stored in the ring
     * @param valueMaxSize the maximum size that an value can be, if a value is written to the buffer that is larger than this an exception will be thrown
     * @param valueClass
     */
    public SharedLocalDataLocator(final int capacity,
                                  final int valueMaxSize,
                                  @NotNull final DirectBytes storeSlice,
                                  @NotNull final Class<E> valueClass) {

        if (valueMaxSize == 0)
            throw new IllegalArgumentException("valueMaxSize has to be greater than 0.");

        this.valueMaxSize = valueMaxSize;
        this.valueClass = valueClass;
        this.capacity = capacity;
        this.storeSlice = storeSlice;
    }

    // todo remove the synchronized
    @Override
    public synchronized E getData(final int index) {

        if (valueClass == null) {
            // It not possible to read as no data has been written
            return null;
        }

        int offset = getOffset(index);

        try {

            storeSlice.busyLockLong(offset);
            storeSlice.position(offset + LOCK_SIZE);
            return storeSlice.readInstance(valueClass, null);

        } catch (InterruptedException e) {
            e.printStackTrace();
            return null;
        } finally {
            storeSlice.unlockLong(offset);
        }
    }


    // todo remove the synchronized
    @Override
    public synchronized int setData(final int index, final E value) {

        final Class aClass = (Class) value.getClass();
        if (!valueClass.equals(aClass))
            throw new IllegalArgumentException("All data must be of type, class=" + valueClass);

        int offset = getOffset(index);
        try {
            storeSlice.busyLockLong(offset);

            final long start = offset + LOCK_SIZE;
            storeSlice.position(start);
            storeSlice.writeInstance(aClass, value);

            if (storeSlice.position() - start > valueMaxSize)
                throw new IllegalArgumentException("Object too large, valueMaxSize=" + valueMaxSize + ", actual-size=" + (storeSlice.position() - start));

        } catch (InterruptedException e) {
            e.printStackTrace();

        } finally {
            storeSlice.unlockLong(offset);
        }

        return 0;
    }


    // todo optimise this later, it not the quickest way to do this, we should revisit the use of writeAll()
    @Override
    public void writeAll(final E[] newData, final int length) {

        for (int i = 0; i < length; i++) {
            setData(i, newData[i]);
        }

    }

    @Override
    public int getCapacity() {
        return capacity;
    }

    @Override
    public String toString() {

        final StringBuilder b = new StringBuilder();

        for (int i = 0; i < capacity; i++) {
            b.append(i + "=" + getData(i) + ", ");
        }

        b.deleteCharAt(b.length() - 2);
        return b.toString();

    }


    /**
     * calculates the offset for a given index
     *
     * @param index=
     * @return
     */
    private int getOffset(int index) {
        int position = index * valueMaxSize;
        return (position + ALIGN - 1) & ~(ALIGN - 1);
    }

}
