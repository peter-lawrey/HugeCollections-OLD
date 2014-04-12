package net.openhft.chronicle.sandbox.queue.locators.shared;

/**
 * Created by Rob Austin
 */
public interface OffsetProvider {

    /**
     * calculates the offset for a given index
     *
     * @param index=
     * @return the offset at {@param index}
     */
    int getOffset(int index);
}
