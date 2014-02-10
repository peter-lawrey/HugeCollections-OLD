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

import net.openhft.lang.io.DirectBytes;
import net.openhft.lang.io.DirectStore;
import net.openhft.lang.io.MappedStore;
import net.openhft.lang.io.NativeBytes;
import net.openhft.lang.sandbox.collection.ATSDirectBitSet;

import java.io.File;
import java.util.AbstractMap;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class SharedHashMap<K, V> extends AbstractMap<K, V> implements ConcurrentMap<K, V> {
    final ThreadLocal<DirectBytes> localBytes = new ThreadLocal<DirectBytes>();
    private final SharedHashMapBuilder builder;
    private final File file;
    private final MappedStore ms;
    private final Segment<K, V>[] segments;

    public SharedHashMap(SharedHashMapBuilder builder, File file, MappedStore ms) {
        this.builder = builder;
        this.file = file;
        this.ms = ms;
        segments = new Segment[builder.segments()];
        long address = ms.address() + SharedHashMapBuilder.HEADER_SIZE;
        int segmentSize = builder.segmentSize();
        for (int i = 0; i < segments.length; i++) {
            segments[i] = new Segment<K, V>(new NativeBytes(ms.bytesMarshallerFactory(), address, address, segmentSize, new AtomicInteger(1)));
            address += segmentSize;
        }
    }

    protected DirectBytes acquireBytes() {
        DirectBytes bytes = localBytes.get();
        if (bytes == null)
            localBytes.set(bytes = new DirectStore(ms.bytesMarshallerFactory(), builder.entrySize() * 2, false).createSlice());
        return bytes;
    }

    @Override
    public V get(Object key) {
        DirectBytes bytes = acquireBytes();
        bytes.clear().writeObject(key);
        long hash = longHashCode(bytes);
        int segmentNum = (int) (hash & (builder.segments() - 1));
        int hash2 = (int) (hash / builder.segments());
        return segments[segmentNum].get((K) key, hash2);
    }

    private long longHashCode(DirectBytes bytes) {
        long hashCode = 0;
        int i = 0;
        for (; i < bytes.position() - 7; i += 8)
            hashCode *= 10191 * hashCode + bytes.readLong(i);
        for (; i < bytes.position(); i++)
            hashCode *= 57 + bytes.readLong(i);
        return hashCode;
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

    class Segment<K, V> {
        private final NativeBytes bytes;
        private final IntIntMultiMap hashLookup;
        private final ATSDirectBitSet freeList;
        private final long entriesStart;

        public Segment(NativeBytes bytes) {
            this.bytes = bytes;
            long start = bytes.startAddr() + SharedHashMapBuilder.SEGMENT_HEADER;
            int size = builder.entriesPerSegment * 2 * 8;
            hashLookup = new IntIntMultiMap(new NativeBytes(start, start, start + size));
            start += size;
            size = (builder.entriesPerSegment + 63) / 64 * 8;
            freeList = new ATSDirectBitSet(new NativeBytes(start, start, start + size));
            start += size * (1 + builder.replicas());
            entriesStart = start;
        }

        public V get(K key, int hash2) {
            return null;
        }
    }
}
