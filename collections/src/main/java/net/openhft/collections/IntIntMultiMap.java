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

package net.openhft.collections;

import net.openhft.lang.Maths;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.DirectStore;

/**
 * Supports a simple interface for int -> int[] off heap.
 */
public class IntIntMultiMap {
    public static final int UNSET = Integer.MIN_VALUE;
    private static final long UNSET_ENTRY = UNSET * 0x100000001L;
    public static final int KEY = 0;
    public static final int VALUE = 4;
    public static final int ENTRY_SIZE = 8;
    private final int capacityMask;
    private final int capacityMask2;
    private final Bytes bytes;
    private int size = 0;

    public IntIntMultiMap(int size) {
        size = Maths.nextPower2(size, 16);
        capacityMask = size - 1;
        capacityMask2 = (size - 1) * ENTRY_SIZE;
        bytes = new DirectStore(null, size * ENTRY_SIZE, false).createSlice();
        clear();
    }

    public IntIntMultiMap(Bytes bytes) {
        size = (int) (bytes.capacity() / ENTRY_SIZE);
        assert size == Maths.nextPower2(size, 16);
        capacityMask = size - 1;
        capacityMask2 = (size - 1) * ENTRY_SIZE;
        this.bytes = bytes;
    }

    /**
     * Add an entry as an int/int pair.  Allow duplicate keys, but not key/values.
     *
     * @param key   to add
     * @param value to add
     */
    public void put(int key, int value) {
        int pos = (key & capacityMask) << 3; // 8 bytes per entry
        for (int i = 0; i <= capacityMask; i++) {
            int key2 = bytes.readInt(pos + KEY);
            if (key2 == UNSET) {
                bytes.writeInt(pos + KEY, key);
                bytes.writeInt(pos + VALUE, value);
                size++;
                return;
            }
            if (key2 == key) {
                int value2 = bytes.readInt(pos + VALUE);
                if (value2 == value)
                    return;
            }
            pos = (pos + ENTRY_SIZE) & capacityMask2;
        }
        throw new IllegalStateException("IntIntMultiMap is full");
    }

    /**
     * Remove a key/value combination.
     *
     * @param key   to remove
     * @param value to remove
     * @return whether a match was found.
     */
    public boolean remove(int key, int value) {
        int pos = (key & capacityMask) << 3; // 8 bytes per entry
        int pos0 = -1;
        // find the end of the chain.
        boolean found = false;
        for (int i = 0; i <= capacityMask; i++) {
            int key2 = bytes.readInt(pos + KEY);
            if (key2 == key) {
                int value2 = bytes.readInt(pos + VALUE);
                if (value2 == value) {
                    found = true;
                    pos0 = pos;
                }
            } else if (key2 == UNSET) {
                break;
            }
            pos = (pos + ENTRY_SIZE) & capacityMask2;
        }
        if (!found)
            return false;
        size--;
        int pos2 = pos;
        // now work back up the chain from pos to pos0;
        // Note: because of the mask, the pos can be actually less than pos0, thus using != operator instead of >=
        while (pos != pos0) {
            pos = (pos - ENTRY_SIZE) & capacityMask2;
            int key2 = bytes.readInt(pos + KEY);
            if (key2 == key) {
                // swap values and clear
                if (pos != pos0) {
                    int value2 = bytes.readInt(pos + VALUE);
                    bytes.writeInt(pos0 + VALUE, value2);
                }
                bytes.writeLong(pos, UNSET_ENTRY);
                break;
            }
        }
        pos = (pos + ENTRY_SIZE) & capacityMask2;
        // re-inset any values in between pos and pos2.
        while (pos < pos2) {
            int key2 = bytes.readInt(pos + KEY);
            int value2 = bytes.readInt(pos + VALUE);
            // clear the entry
            bytes.writeLong(pos, UNSET_ENTRY);
            size--;
            // this might put it back in the same place or a different one.
            put(key2, value2);
            pos = (pos + ENTRY_SIZE) & capacityMask2;
        }
        return true;
    }

    /**
     * stateful method which starts a search for a key.
     *
     * @param key to search for
     */

    private int searchKey = -1;
    private int searchPos = -1;

    public void startSearch(int key) {
        searchPos = (key & capacityMask) << 3; // 8 bytes per entry
        searchKey = key;
    }

    /**
     * @return the next int value for the last search or UNSET
     */
    public int nextInt() {
        for (int i = 0; i <= capacityMask; i++) {
            int key2 = bytes.readInt(searchPos + KEY);
            if (key2 == UNSET)
                return UNSET;
            int pos = searchPos;
            searchPos = (searchPos + ENTRY_SIZE) & capacityMask2;
            if (key2 == searchKey) {
                int value2 = bytes.readInt(pos + VALUE);
                return value2;
            }
        }
        return UNSET;
    }

    public int capacity() {
        return capacityMask;
    }

    public int size() {
        return size;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{ ");
        for (int i = 0, pos = 0; i <= capacityMask; i++, pos += ENTRY_SIZE) {
            int key = bytes.readInt(pos + KEY);
            int value = bytes.readInt(pos + VALUE);
            if (key != UNSET)
                sb.append(key).append('=').append(value).append(", ");
        }
        if (sb.length() > 2) {
            sb.setLength(sb.length() - 2);
            return sb.append(" }").toString();
        }
        return "{ }";
    }

    public void clear() {
        for (int pos = 0; pos < bytes.capacity(); pos += ENTRY_SIZE)
            bytes.writeLong(pos, UNSET_ENTRY);
    }
}
