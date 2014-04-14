package net.openhft.chronicle.sandbox.queue.locators.shared.remote;

import java.nio.ByteBuffer;

/**
 * Created by Rob Austin
 */
public class ByteUtils {

    public static String toString(ByteBuffer buffer) {

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
