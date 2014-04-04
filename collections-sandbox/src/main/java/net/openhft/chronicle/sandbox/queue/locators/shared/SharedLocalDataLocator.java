package net.openhft.chronicle.sandbox.queue.locators.shared;

import net.openhft.chronicle.sandbox.queue.locators.DataLocator;
import net.openhft.lang.io.DirectBytes;
import org.jetbrains.annotations.NotNull;

/**
 * Created by Rob Austin
 */
public class SharedLocalDataLocator<E> implements DataLocator<E> {

    // intentionally not volatile, as we are carefully ensuring that the memory barriers are controlled below by other objects
    private final int capacity;
    private final int valueMaxSize;

    private final DirectBytes storeSlice;


    // we create the using array, to reduce object creation on the read side
    // the objects in this array will be reused
    private final Object[] using;

    private final Class<E> valueClass;

    /**
     * @param capacity     the maxiumm number of values that can be stored in the ring
     * @param valueMaxSize the maximum size that an value can be, if a value is written to the buffer that is larger than this an exception will be thrown
     * @param valueClass
     */
    public SharedLocalDataLocator(final int capacity,
                                  @NotNull final int valueMaxSize,
                                  @NotNull final DirectBytes storeSlice,
                                  @NotNull final Class<E> valueClass) {

        if (valueMaxSize == 0)
            throw new IllegalArgumentException("valueMaxSize has to be greater than 0.");
        this.valueMaxSize = valueMaxSize;
        this.valueClass = valueClass;
        this.capacity = capacity;
        this.storeSlice = storeSlice;
        this.using = new Object[capacity];
    }

    @Override
    public E getData(final int index) {

        if (valueClass == null) {
            // It not possible to read as no data has been written
            return null;
        }

        storeSlice.position(index * valueMaxSize);
        storeSlice.alignPositionAddr(4);
        return (E) storeSlice.readInstance(valueClass, (E) using[index]);
    }

    @Override
    public void setData(final int index, final E value) {

        final Class aClass = (Class) value.getClass();
        if (!valueClass.equals(aClass))
            throw new IllegalArgumentException("All data must be of type, class=" + valueClass);

        storeSlice.position(index * valueMaxSize);
        storeSlice.alignPositionAddr(4);
        storeSlice.writeInstance(aClass, value);
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
}
