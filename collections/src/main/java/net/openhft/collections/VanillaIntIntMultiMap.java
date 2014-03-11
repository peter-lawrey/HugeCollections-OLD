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
class VanillaIntIntMultiMap implements IntIntMultiMap {
    private static final int ENTRY_SIZE = 8;

    private static final int UNSET_KEY = 0;
    private static final int HASH_INSTEAD_OF_UNSET_KEY = -1;
    private static final int UNSET_VALUE = Integer.MIN_VALUE;
    /**
     * hash is in 32 higher order bits, because in Intel's little-endian
     * they are written first in memory, and in memory we have keys and values
     * in natural order: 4 bytes of k1, 4 bytes of v1, 4 bytes of k2, ...
     * and this is somehow compatible with previous version of this class,
     * where keys were written before values explicitly.
     * <p/>
     * However, this layout increases latency of map operations
     * by 1 clock cycle :), because we always need to perform shift to obtain
     * the key between memory read and comparison with UNSET_KEY.
     */
    private static final long UNSET_ENTRY = Integer.MIN_VALUE & 0xFFFFFFFFL;

    private final int capacity;
    private final int capacityMask;
    private final int capacityMask2;
    private final Bytes bytes;

    public VanillaIntIntMultiMap(int minCapacity) {
        if (minCapacity < 0)
            throw new IllegalArgumentException();
        capacity = Maths.nextPower2(minCapacity, 16);
        capacityMask = capacity - 1;
        capacityMask2 = (capacity - 1) * ENTRY_SIZE;
        bytes = new DirectStore(null, capacity * ENTRY_SIZE, false).createSlice();
        clear();
    }

    public VanillaIntIntMultiMap(Bytes bytes) {
        capacity = (int) (bytes.capacity() / ENTRY_SIZE);
        assert capacity == Maths.nextPower2(capacity, 16);
        capacityMask = capacity - 1;
        capacityMask2 = (capacity - 1) * ENTRY_SIZE;
        this.bytes = bytes;
    }

    @Override
    public void put(int key, int value) {
        if (!putLimited(key, value, capacityMask + 1))
            throw new IllegalStateException("VanillaIntIntMultiMap is full");
    }

    public boolean putLimited(int key, int value, int limit) {
        if (key == UNSET_KEY)
            key = HASH_INSTEAD_OF_UNSET_KEY;
        int pos = (key & capacityMask) << 3; // 8 bytes per entry
        for (int i = 0; i < limit; i++) {
            long entry = bytes.readLong(pos);
            int hash2 = (int) (entry >> 32);
            if (hash2 == UNSET_KEY) {
                bytes.writeLong(pos, (((long) key) << 32) | (value & 0xFFFFFFFFL));
                return true;
            }
            if (hash2 == key) {
                int value2 = (int) entry;
                if (value2 == value)
                    return true;
            }
            pos = (pos + ENTRY_SIZE) & capacityMask2;
        }
        return false;
    }

    @Override
    public boolean remove(int key, int value) {
        if (key == UNSET_KEY)
            key = HASH_INSTEAD_OF_UNSET_KEY;
        int pos = (key & capacityMask) << 3; // 8 bytes per entry
        int pos0 = -1;
        // find the end of the chain.
        boolean found = false;
        for (int i = 0; i <= capacityMask; i++) {
            long entry = bytes.readLong(pos);
//            int hash2 = bytes.readInt(pos + KEY);
            int hash2 = (int) (entry >> 32);
            if (hash2 == key) {
//                int value2 = bytes.readInt(pos + VALUE);
                int value2 = (int) entry;
                if (value2 == value) {
                    found = true;
                    pos0 = pos;
                }
            } else if (hash2 == UNSET_KEY) {
                break;
            }
            pos = (pos + ENTRY_SIZE) & capacityMask2;
        }
        if (!found)
            return false;
        int pos2 = pos;
        // now work back up the chain from pos to pos0;
        // Note: because of the mask, the pos can be actually less than pos0,
        // thus using != operator instead of >=
        while (pos != pos0) {
            pos = (pos - ENTRY_SIZE) & capacityMask2;
            long entry = bytes.readLong(pos);
//            int hash2 = bytes.readInt(pos + KEY);
            int hash2 = (int) (entry >> 32);
            if (hash2 == key) {
                // swap values and zeroOut
                if (pos != pos0) {
                    long entry2 = bytes.readLong(pos);
                    bytes.writeLong(pos0, entry2);
                }
                bytes.writeLong(pos, UNSET_ENTRY);
                break;
            }
        }
        pos = (pos + ENTRY_SIZE) & capacityMask2;
        // re-inset any values in between pos and pos2.
        while (pos < pos2) {
            long entry2 = bytes.readLong(pos);
            int hash2 = (int) (entry2 >> 32);
            int value2 = (int) entry2;
            // zeroOut the entry
            bytes.writeLong(pos, UNSET_ENTRY);
            // this might put it back in the same place or a different one.
            put(hash2, value2);
            pos = (pos + ENTRY_SIZE) & capacityMask2;
        }
        return true;
    }

    /////////////////////
    // Stateful methods

    private int searchHash = -1;
    private int searchPos = -1;

    @Override
    public int firstPos() {
        int pos = 0;
        while (pos < capacity * ENTRY_SIZE) {
            long entry = bytes.readLong(pos);
            int hash2 = (int) (entry >> 32);
            if (hash2 != UNSET_KEY) {
                return (int) entry;
            }
            pos = pos + ENTRY_SIZE;
        }
        return -1;
    }

    @Override
    public int nextKeyAfter(int key) { //todo: merge implementation with first position method
        startSearch(key);
        while (searchPos < capacity * ENTRY_SIZE) {
            long entry = bytes.readLong(searchPos);
            int hash2 = (int) (entry >> 32);
            if (hash2 != UNSET_KEY && hash2 != searchHash) {
                return (int) entry;
            }
            searchPos = searchPos + ENTRY_SIZE;
        }
        return -1;
    }

    @Override
    public int startSearch(int key) {
        if (key == UNSET_KEY)
            key = HASH_INSTEAD_OF_UNSET_KEY;

        searchPos = (key & capacityMask) << 3; // 8 bytes per entry
        return searchHash = key;
    }

    @Override
    public int nextPos() {
        for (int i = 0; i < capacity; i++) {
            long entry = bytes.readLong(searchPos);
            int hash2 = (int) (entry >> 32);
            if (hash2 == UNSET_KEY) {
                return UNSET_VALUE;
            }
            searchPos = (searchPos + ENTRY_SIZE) & capacityMask2;
            if (hash2 == searchHash) {
                return (int) entry;
            }
        }
        return UNSET_VALUE;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{ ");
        for (int i = 0, pos = 0; i < capacity; i++, pos += ENTRY_SIZE) {
            long entry = bytes.readLong(pos);
            int key = (int) (entry >> 32);
            int value = (int) entry;
            if (key != UNSET_KEY)
                sb.append(key).append('=').append(value).append(", ");
        }
        if (sb.length() > 2) {
            sb.setLength(sb.length() - 2);
            return sb.append(" }").toString();
        }
        return "{ }";
    }

    @Override
    public void clear() {
        for (int pos = 0; pos < bytes.capacity(); pos += ENTRY_SIZE) {
            bytes.writeLong(pos, UNSET_ENTRY);
        }
    }
}
