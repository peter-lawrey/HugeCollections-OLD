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

    public static CharSequence toCharSequence(@NotNull final ByteBuffer buffer) {

        final ByteBuffer slice = buffer.slice();
        final StringBuilder builder = new StringBuilder();

        while (slice.hasRemaining()) {
            final byte b = slice.get();
            builder.append((char)b);
        }


        return builder.toString();
    }
}
