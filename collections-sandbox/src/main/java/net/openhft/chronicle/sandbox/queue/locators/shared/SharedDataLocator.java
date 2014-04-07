package net.openhft.chronicle.sandbox.queue.locators.shared;

import net.openhft.chronicle.sandbox.queue.locators.DataLocator;
import net.openhft.lang.io.DirectBytes;
import org.jetbrains.annotations.NotNull;

/**
 * Created by Rob Austin
 */
public class SharedDataLocator<E> implements DataLocator<E> {

    public static final int ALIGN = 4;

    // intentionally not volatile, as we are carefully ensuring that the memory barriers are controlled below by other objects
    private final int capacity;
    private final int valueMaxSize;

    @NotNull
    private final DirectBytes readerSlice;

    @NotNull
    private final Class<E> valueClass;

    @NotNull
    private final DirectBytes writerSlice;

    /**
     * @param valueClass
     * @param capacity     the maximum number of values that can be stored in the ring
     * @param valueMaxSize the maximum size that an value can be, if a value is written to the buffer that is larger than this an exception will be thrown
     * @param readerSlice  an instance of the MessageStore used by the reader
     * @param writerSlice  an instance of the MessageStore used by the writer
     */
    public SharedDataLocator(@NotNull final Class<E> valueClass,
                             final int capacity,
                             final int valueMaxSize,
                             @NotNull final DirectBytes readerSlice,
                             @NotNull final DirectBytes writerSlice) {

        if (valueMaxSize == 0)
            throw new IllegalArgumentException("valueMaxSize has to be greater than 0.");

        this.valueMaxSize = valueMaxSize;
        this.valueClass = valueClass;
        this.capacity = capacity;
        this.readerSlice = readerSlice;
        this.writerSlice = writerSlice;
    }

    @Override
    public E getData(final int index) {
        readerSlice.position(getOffset(index));
        return (E) readerSlice.readInstance(valueClass, null);
    }

    @Override
    public void setData(final int index, final E value) {

        final Class aClass = (Class) value.getClass();
        if (!valueClass.equals(aClass))
            throw new IllegalArgumentException("All data must be of type, class=" + valueClass);

        int offset = getOffset(index);

        writerSlice.position(offset);
        writerSlice.writeInstance(aClass, value);

        if (writerSlice.position() - offset > valueMaxSize)
            throw new IllegalArgumentException("Object too large, valueMaxSize=" + valueMaxSize + ", actual-size=" + (writerSlice.position() - offset));

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
    private int getOffset(int index) {
        int position = index * valueMaxSize;
        return (position + ALIGN - 1) & ~(ALIGN - 1);
    }

}

