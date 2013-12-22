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

import net.openhft.lang.LongHashable;
import net.openhft.lang.Maths;
import net.openhft.lang.io.DirectBytes;
import net.openhft.lang.io.DirectStore;
import net.openhft.lang.io.MultiStoreBytes;
import net.openhft.lang.io.NativeBytes;
import net.openhft.lang.io.serialization.BytesMarshallable;
import net.openhft.lang.io.serialization.impl.VanillaBytesMarshallerFactory;

import java.util.*;

/**
 * User: plawrey Date: 07/12/13 Time: 10:38
 */
public class HugeHashMap<K, V> extends AbstractMap<K, V> implements HugeMap<K, V> {
    private final Segment<K, V>[] segments;
    private final int segmentMask;
    private final int segmentShift;
    private final boolean csKey;
    private final boolean longHashable;
    private final boolean bytesMarshallable;
//    private final Class<K> kClass;
//    private final Class<V> vClass;


    public HugeHashMap() {
        this(HugeConfig.DEFAULT, (Class<K>) Object.class, (Class<V>) Object.class);
    }

    public HugeHashMap(HugeConfig config, Class<K> kClass, Class<V> vClass) {
//        this.kClass = kClass;
//        this.vClass = vClass;
        final int segmentCount = config.getSegments();
        segmentMask = segmentCount - 1;
        segmentShift = Maths.intLog2(segmentCount);
        csKey = CharSequence.class.isAssignableFrom(kClass);
        longHashable = LongHashable.class.isAssignableFrom(kClass);
        bytesMarshallable = BytesMarshallable.class.isAssignableFrom(vClass);
        //noinspection unchecked
        segments = (Segment<K, V>[]) new Segment[segmentCount];
        for (int i = 0; i < segmentCount; i++)
            segments[i] = new Segment<K, V>(config, csKey, bytesMarshallable, vClass);
    }

    protected long hash(K key) {
        long h;
        if (csKey) {
            h = Maths.hash((CharSequence) key);
        } else if (longHashable) {
            h = ((LongHashable) key).longHashCode();
        } else {
            h = (long) key.hashCode() << 31;
        }
        h += (h >>> 42) - (h >>> 21);
        h += (h >>> 14) - (h >>> 7);
        return h;
    }

    @Override
    public V put(K key, V value) {
        long h = hash(key);
        int segment = (int) (h & segmentMask);
        // leave the remaining hashCode
        h >>>= segmentShift;
        segments[segment].put(h, key, value, true, true);
        return null;
    }

    @Override
    public V get(Object key) {
        return get((K) key, null);
    }

    @Override
    public V get(K key, V value) {
        long h = hash(key);
        int segment = (int) (h & segmentMask);
        // leave the remaining hashCode
        h >>>= segmentShift;
        return segments[segment].get(h, key, value);
    }

    @Override
    public V remove(Object key) {
        long h = hash((K) key);
        int segment = (int) (h & segmentMask);
        // leave the remaining hashCode
        h >>>= segmentShift;
        segments[segment].remove(h, (K) key);
        return null;
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public V putIfAbsent(K key, V value) {
        long h = hash(key);
        int segment = (int) (h & segmentMask);
        // leave the remaining hashCode
        h >>>= segmentShift;
        segments[segment].put(h, key, value, false, true);
        return null;
    }

    @Override
    public boolean remove(Object key, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V replace(K key, V value) {
        long h = hash(key);
        int segment = (int) (h & segmentMask);
        // leave the remaining hashCode
        h >>>= segmentShift;
        segments[segment].put(h, key, value, true, false);
        return null;
    }

    @Override
    public boolean isEmpty() {
        for (Segment<K, V> segment : segments) {
            if (segment.size() > 0)
                return false;
        }
        return true;
    }

    @Override
    public int size() {
        long total = 0;
        for (Segment<K, V> segment : segments) {
            total += segment.size();
        }
        return (int) Math.min(Integer.MAX_VALUE, total);
    }

    @Override
    public long offHeapUsed() {
        long total = 0;
        for (Segment<K, V> segment : segments) {
            total += segment.offHeapUsed();
        }
        return total;
    }

    @Override
    public void clear() {
        for (Segment<K, V> segment : segments) {
            segment.clear();
        }
    }

    static class Segment<K, V> {
        final VanillaBytesMarshallerFactory bmf = new VanillaBytesMarshallerFactory();
        final IntIntMultiMap smallMap;
        final Map<K, DirectStore> map = new HashMap<K, DirectStore>();
        final DirectBytes tmpBytes;
        final MultiStoreBytes bytes = new MultiStoreBytes();
        final DirectStore store;
        final BitSet usedSet;
        final int smallEntrySize;
        final int entriesPerSegment;
        final boolean csKey;
        final StringBuilder sbKey;
        final boolean bytesMarshallable;
        final Class<V> vClass;
        long offHeapUsed = 0;
        long size = 0;

        Segment(HugeConfig config, boolean csKey, boolean bytesMarshallable, Class<V> vClass) {
            this.csKey = csKey;
            this.bytesMarshallable = bytesMarshallable;
            this.vClass = vClass;
            smallEntrySize = (config.getSmallEntrySize() + 7) & ~7; // round to next multiple of 8.
            entriesPerSegment = config.getEntriesPerSegment();
            store = new DirectStore(bmf, smallEntrySize * entriesPerSegment, false);
            usedSet = new BitSet(config.getEntriesPerSegment());
            smallMap = new IntIntMultiMap(entriesPerSegment * 2);
            tmpBytes = new DirectStore(bmf, 64 * smallEntrySize, false).createSlice();
            offHeapUsed = tmpBytes.capacity() + store.size();
            sbKey = csKey ? new StringBuilder() : null;
        }

        public synchronized void put(long h, K key, V value, boolean ifPresent, boolean ifAbsent) {
            // search for the previous entry
            int hash = intHashFor(h);
            smallMap.startSearch(hash);
            boolean foundSmall = false, foundLarge = false;
            while (true) {
                int pos = smallMap.nextInt();
                if (pos == IntIntMultiMap.UNSET) {
                    final DirectStore store = map.get(key);
                    if (store == null) {
                        if (ifPresent && !ifAbsent)
                            return;
                        break;
                    }
                    if (ifAbsent)
                        return;
                    bytes.storePositionAndSize(store, 0, store.size());
                    foundLarge = true;
                    break;
                } else {
                    bytes.storePositionAndSize(store, pos * smallEntrySize, smallEntrySize);
                    K key2 = getKey();
                    if (equals(key, key2)) {
                        if (ifAbsent)
                            return;
                        foundSmall = true;
                        break;
                    }
                }
            }

            tmpBytes.reset();
            if (csKey)
                tmpBytes.writeUTFΔ((CharSequence) key);
            else
                tmpBytes.writeObject(key);
            long startOfValuePos = tmpBytes.position();
            if (bytesMarshallable)
                ((BytesMarshallable) value).writeMarshallable(tmpBytes);
            else
                tmpBytes.writeObject(value);
            long size = tmpBytes.position();
            if (size <= smallEntrySize) {
                if (foundSmall) {
                    bytes.position(0);
                    bytes.write(tmpBytes, 0, size);
                    return;
                } else if (foundLarge) {
                    remove(hash, key);
                }
                // look for a free spot.
                int position = (int) (h & (entriesPerSegment - 1));
                int free = usedSet.previousClearBit(position);
                if (free < 0)
                    free = usedSet.nextClearBit(position + 1);
                if (free < entriesPerSegment) {
                    bytes.storePositionAndSize(store, free * smallEntrySize, smallEntrySize);
                    bytes.write(tmpBytes, 0, size);
                    smallMap.put(hash, free);
                    usedSet.set(free);
                    this.size++;
                    return;
                }
            }
            if (foundSmall) {
                remove(hash, key);
            } else if (foundLarge) {
                // can it be reused.
                if (bytes.capacity() <= size || bytes.capacity() - size < (size >> 3)) {
                    bytes.write(tmpBytes, startOfValuePos, size);
                    return;
                }
                remove(hash, key);
            }
            size = size - startOfValuePos;
            DirectStore store = new DirectStore(bmf, size);
            bytes.storePositionAndSize(store, 0, size);
            bytes.write(tmpBytes, startOfValuePos, size);
            map.put(key, store);
            offHeapUsed += size;
            this.size++;
        }

        private int intHashFor(long h) {
            int hash = (int) h;
            if (hash == IntIntMultiMap.UNSET)
                hash = 0;
            return hash;
        }

        public synchronized V get(long h, K key, V value) {
            int hash = intHashFor(h);
            smallMap.startSearch(hash);
            while (true) {
                int pos = smallMap.nextInt();
                if (pos == IntIntMultiMap.UNSET) {
                    final DirectStore store = map.get(key);
                    if (store == null)
                        return null;
                    bytes.storePositionAndSize(store, 0, store.size());
                    break;
                } else {
                    bytes.storePositionAndSize(store, pos * smallEntrySize, smallEntrySize);
                    K key2 = getKey();
                    if (equals(key, key2))
                        break;
                }
            }
            if (bytesMarshallable) {
                try {
                    V v = value == null ? (V) NativeBytes.UNSAFE.allocateInstance(vClass) : value;
                    ((BytesMarshallable) v).readMarshallable(bytes);
                    return v;
                } catch (InstantiationException e) {
                    throw new AssertionError(e);
                }
            }
            return (V) bytes.readObject();
        }

        private boolean equals(K key, K key2) {
            return csKey ? equalsCS((CharSequence) key, (CharSequence) key2) : key.equals(key2);
        }

        private static boolean equalsCS(CharSequence key, CharSequence key2) {
            if (key.length() != key2.length())
                return false;
            for (int i = 0; i < key.length(); i++)
                if (key.charAt(i) != key2.charAt(i))
                    return false;
            return true;
        }

        private K getKey() {
            if (csKey) {
                sbKey.setLength(0);
                bytes.readUTFΔ(sbKey);
                return (K) sbKey;
            }
            return (K) bytes.readObject();
        }

        public synchronized boolean remove(long h, K key) {
            int hash = intHashFor(h);
            smallMap.startSearch(hash);
            boolean found = false;
            while (true) {
                int pos = smallMap.nextInt();
                if (pos == IntIntMultiMap.UNSET) {
                    break;
                }
                bytes.storePositionAndSize(store, pos * smallEntrySize, smallEntrySize);
                K key2 = getKey();
                if (equals(key, key2)) {
                    usedSet.clear(pos);
                    smallMap.remove(hash, pos);
                    found = true;
                    this.size--;
                    break;
                }
            }
            DirectStore remove = map.remove(key);
            if (remove == null)
                return found;
            offHeapUsed -= remove.size();
            remove.free();
            this.size--;
            return true;
        }

        public synchronized long offHeapUsed() {
            return offHeapUsed;
        }

        public synchronized long size() {
            return this.size;
        }

        public void clear() {
            usedSet.clear();
            smallMap.clear();
            for (DirectStore directStore : map.values()) {
                directStore.free();
            }
            map.clear();
        }
    }
}
