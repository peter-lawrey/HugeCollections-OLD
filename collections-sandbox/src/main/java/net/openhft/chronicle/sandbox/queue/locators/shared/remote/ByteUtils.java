package net.openhft.chronicle.sandbox.queue.locators.shared.remote;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

/**
 * Created by Rob Austin
 */
public class ByteUtils {

    /**
     * display the hex data of a byte buffer from the position() to the limit()
     *
     * @param buffer the buffer you wish to toString()
     * @return hex representation of the buffer, from example [0D ,OA, FF]
     */
    public static String toString(@NotNull final ByteBuffer buffer) {

        final ByteBuffer slice = buffer.slice();
        final StringBuilder builder = new StringBuilder("[");

        while (slice.hasRemaining()) {
            final byte b = slice.get();
            builder.append(String.format("%02X ", b));
            builder.append(",");
        }

        // remove the last comma
        builder.deleteCharAt(builder.length() - 1);
        builder.append("]");
        return builder.toString();
    }
}
