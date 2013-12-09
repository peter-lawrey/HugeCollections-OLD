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
    private final boolean stringKey;
    private final boolean longHashable;
    private final boolean bytesMarshallable;
    private final Class<K> kClass;
    private final Class<V> vClass;


    public HugeHashMap() {
        this(HugeConfig.DEFAULT, (Class<K>) Object.class, (Class<V>) Object.class);
    }

    public HugeHashMap(HugeConfig config, Class<K> kClass, Class<V> vClass) {
        this.kClass = kClass;
        this.vClass = vClass;
        final int segmentCount = config.getSegments();
        segments = new Segment[segmentCount];
        for (int i = 0; i < segmentCount; i++)
            segments[i] = new Segment<K, V>(config);
        segmentMask = segmentCount - 1;
        segmentShift = Maths.intLog2(segmentCount);
        stringKey = CharSequence.class.isAssignableFrom(kClass);
        longHashable = LongHashable.class.isAssignableFrom(kClass);
        bytesMarshallable = BytesMarshallable.class.isAssignableFrom(vClass);
    }

    protected long hash(K key) {
        long h = longHashable ? ((LongHashable) key).longHashCode() : (long) key.hashCode() << 31;
        if (stringKey) {
            CharSequence cs = (CharSequence) key;
            int length = cs.length();
            if (length > 2)
                h ^= (cs.charAt(length - 2) << 8) + cs.charAt(length - 1);
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
        segments[segment].put(h, key, value);
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
        throw new UnsupportedOperationException();
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
        throw new UnsupportedOperationException();
    }

    @Override
    public long offHeapUsed() {
        throw new UnsupportedOperationException();
    }

    class Segment<K, V> {
        final VanillaBytesMarshallerFactory bmf = new VanillaBytesMarshallerFactory();
        final Map<K, Integer> smallMap = new HashMap<K, Integer>();
        final Map<K, DirectStore> map = new HashMap<K, DirectStore>();
        final DirectStore tmpStore = new DirectStore(bmf, 64 * 1024);
        final DirectBytes tmpBytes = tmpStore.createSlice();
        final MultiStoreBytes bytes = new MultiStoreBytes();
        final DirectStore store;
        final BitSet usedSet;
        final int smallEntrySize;
        final int entriesPerSegment;

        Segment(HugeConfig config) {
            smallEntrySize = config.getSmallEntrySize();
            entriesPerSegment = config.getEntriesPerSegment();
            store = new DirectStore(bmf, smallEntrySize * entriesPerSegment);
            usedSet = new BitSet(config.getEntriesPerSegment());
        }

        public synchronized void put(long h, K key, V value) {
            tmpBytes.reset();
            if (bytesMarshallable)
                ((BytesMarshallable) value).writeMarshallable(tmpBytes);
            else
                tmpBytes.writeObject(value);
            long size = tmpBytes.position();
            if (size <= smallEntrySize) {
                // look for a free stop.
                int position = (int) (h & (entriesPerSegment - 1));
                int free = usedSet.previousClearBit(position);
                if (free < 0)
                    free = usedSet.nextClearBit(position + 1);
                if (free < entriesPerSegment) {
                    bytes.storePositionAndSize(store, free * smallEntrySize, smallEntrySize);
                    bytes.writeStartToPosition(tmpBytes);
                    smallMap.put(key, free);
                    return;
                }
            }
            DirectStore store = new DirectStore(bmf, size);
            bytes.storePositionAndSize(store, 0, size);
            bytes.writeStartToPosition(tmpBytes);
            map.put(key, store);
        }

        public synchronized V get(long h, K key, V value) {
            final Integer pos = smallMap.get(key);
            if (pos == null) {
                final DirectStore store = map.get(key);
                if (store == null) return null;
                bytes.storePositionAndSize(store, 0, store.size());
            } else {
                bytes.storePositionAndSize(store, pos * smallEntrySize, smallEntrySize);
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

        public synchronized void remove(long h, K key) {
            map.remove(key);
        }
    }
}
