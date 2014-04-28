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

package net.openhft.chronicle.sandbox.queue.locators.shared;

import net.openhft.chronicle.sandbox.queue.locators.DataLocator;
import net.openhft.lang.io.AbstractBytes;
import org.jetbrains.annotations.NotNull;

import java.util.logging.Logger;

/**
 * similar to LocalDataLocator.class but works with AbstractBytes
 */
public class BytesDataLocator<E, BYTES extends AbstractBytes> implements DataLocator<E>, OffsetProvider, SliceProvider<BYTES> {

    public static final int ALIGN = 4;
    private static Logger LOG = Logger.getLogger(BytesDataLocator.class.getName());
    protected final int valueMaxSize;
    @NotNull
    final BYTES readerSlice;
    @NotNull
    final Class<E> valueClass;
    // intentionally not volatile, as we are carefully ensuring that the memory barriers are controlled below by other objects
    private final int capacity;
    @NotNull
    private final BYTES writerSlice;

    /**
     * @param valueClass
     * @param capacity     the maximum number of values that can be stored in the ring
     * @param valueMaxSize the maximum size that an value can be, if a value is written to the buffer that is larger than this an exception will be thrown
     * @param readerSlice  an instance of the MessageStore used by the reader
     * @param writerSlice  an instance of the MessageStore used by the writer
     */
    public BytesDataLocator(@NotNull final Class<E> valueClass,
                            final int capacity,
                            final int valueMaxSize,
                            @NotNull final BYTES readerSlice,
                            @NotNull final BYTES writerSlice) {

        if (valueMaxSize == 0)
            throw new IllegalArgumentException("valueMaxSize has to be greater than 0.");

        this.valueMaxSize = valueMaxSize;
        this.valueClass = valueClass;
        this.capacity = capacity;
        this.readerSlice = readerSlice;
        this.writerSlice = writerSlice;
    }

    @Override
    @NotNull
    public BYTES getWriterSlice() {
        return writerSlice;
    }

    @Override
    @NotNull
    public BYTES getReaderSlice() {
        return readerSlice;
    }

    @Override
    public E getData(final int index) {
        readerSlice.position(getOffset(index));
        return (E) readerSlice.readInstance(valueClass, null);
    }

    @Override
    /**
     * @returns the number of bytes written
     */
    public int setData(final int index, final E value) {

        final Class aClass = (Class) value.getClass();
        if (!valueClass.equals(aClass))
            throw new IllegalArgumentException("All data must be of type, class=" + valueClass);

        int offset = getOffset(index);

        writerSlice.position(offset);
        writerSlice.writeInstance(aClass, value);

        final long actualSize = (writerSlice.position() - offset);
        if (actualSize > valueMaxSize)
            throw new IllegalArgumentException("Object too large, valueMaxSize=" + valueMaxSize + ", actual-size=" + actualSize);

        return (int) actualSize;
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
            b.append("data[").append(i).append("]=").append(getData(i)).append(", ");
        }

        b.deleteCharAt(b.length() - 2);
        return b.toString();

    }

    /**
     * calculates the offset for a given index
     *
     * @param index=
     * @return the offset at {@param index}
     */
    public int getOffset(int index) {
        int position = index * valueMaxSize;
        return (position + ALIGN - 1) & ~(ALIGN - 1);
    }

}

