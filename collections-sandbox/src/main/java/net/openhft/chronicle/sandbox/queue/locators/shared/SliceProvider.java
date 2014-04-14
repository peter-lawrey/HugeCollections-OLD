package net.openhft.chronicle.sandbox.queue.locators.shared;

import net.openhft.lang.io.AbstractBytes;
import org.jetbrains.annotations.NotNull;

/**
 * Created by Rob Austin
 */
public interface SliceProvider<BYTES extends AbstractBytes> {
    @NotNull
    BYTES getWriterSlice();

    @NotNull
    BYTES getReaderSlice();
}
