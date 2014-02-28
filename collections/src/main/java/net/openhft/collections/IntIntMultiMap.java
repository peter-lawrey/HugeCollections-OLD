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

    public int unsetKey() {
        return 0;
    }

    public int unsetValue() {
        return Integer.MIN_VALUE;
    }

    /**
     * Add an entry as an int/int pair.  Allow duplicate keys, but not key/values.
     *
     * @param key   to add
     * @param value to add
     */
    public void put(int key, int value) {
        if (key == unsetKey())
            throw new IllegalArgumentException("Cannot add a key with unset value " + unsetKey());
        int pos = (key & capacityMask) << 3; // 8 bytes per entry
        for (int i = 0; i <= capacityMask; i++) {
            long entry = bytes.readLong(pos);
//            int key2 = bytes.readInt(pos + KEY);
            int key2 = (int) (entry >> 32);
            if (key2 == unsetKey()) {
                bytes.writeLong(pos, ((long) key << 32) | (value & 0xFFFFFFFFL));
                size++;
                return;
            }
            if (key2 == key) {
//                int value2 = bytes.readInt(pos + VALUE);
                int value2 = (int) entry;
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
        if (key == unsetKey())
            throw new IllegalArgumentException("Cannot remove a key with unset value " + unsetKey());
        int pos = (key & capacityMask) << 3; // 8 bytes per entry
        int pos0 = -1;
        // find the end of the chain.
        boolean found = false;
        for (int i = 0; i <= capacityMask; i++) {
            long entry = bytes.readLong(pos);
//            int key2 = bytes.readInt(pos + KEY);
            int key2 = (int) (entry >> 32);
            if (key2 == key) {
//                int value2 = bytes.readInt(pos + VALUE);
                int value2 = (int) entry;
                if (value2 == value) {
                    found = true;
                    pos0 = pos;
                }
            } else if (key2 == unsetKey()) {
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
            long entry = bytes.readLong(pos);
//            int key2 = bytes.readInt(pos + KEY);
            int key2 = (int) (entry >> 32);
            if (key2 == key) {
                // swap values and zeroOut
                if (pos != pos0) {
                    long entry2 = bytes.readLong(pos);
                    bytes.writeLong(pos0, entry2);
                }
                bytes.writeLong(pos, ((long) unsetKey() << 32) | (unsetValue() & 0xFFFFFFFFL));
                break;
            }
        }
        pos = (pos + ENTRY_SIZE) & capacityMask2;
        // re-inset any values in between pos and pos2.
        while (pos < pos2) {
            long entry2 = bytes.readLong(pos);
            int key2 = (int) (entry2 >> 32);
            int value2 = (int) entry2;
            // zeroOut the entry
            bytes.writeLong(pos, ((long) unsetKey() << 32) | (unsetValue() & 0xFFFFFFFFL));
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
        if (key == unsetKey())
            throw new IllegalArgumentException("Cannot startSearch a key with unset value " + unsetKey());

        searchPos = (key & capacityMask) << 3; // 8 bytes per entry
        searchKey = key;
    }

    /**
     * @return the next int value for the last search or unsetKey()
     */
    public int nextInt() {
        for (int i = 0; i <= capacityMask; i++) {
            long entry = bytes.readLong(searchPos);
//            int key2 = bytes.readInt(searchPos + KEY);
            int key2 = (int) (entry >> 32);
            if (key2 == unsetKey())
                return unsetValue();
            int pos = searchPos;
            searchPos = (searchPos + ENTRY_SIZE) & capacityMask2;
            if (key2 == searchKey) {
//                int value2 = bytes.readInt(pos + VALUE);
                int value2 = (int) entry;
                return value2;
            }
        }
        return unsetValue();
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
            long entry = bytes.readLong(pos);
            int key = (int) (entry >> 32); // bytes.readInt(pos + KEY);
            int value = (int) entry; // bytes.readInt(pos + VALUE);
            if (key != unsetKey())
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
            bytes.writeLong(pos, unsetKey() * 100000001L);
    }
}
