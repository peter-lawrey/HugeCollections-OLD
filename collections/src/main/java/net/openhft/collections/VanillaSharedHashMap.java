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

import net.openhft.lang.io.*;
import net.openhft.lang.sandbox.collection.ATSDirectBitSet;

import java.io.File;
import java.util.AbstractMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class VanillaSharedHashMap<K, V> extends AbstractMap<K, V> implements SharedHashMap<K, V> {
    final ThreadLocal<DirectBytes> localBytes = new ThreadLocal<DirectBytes>();
    private final SharedHashMapBuilder builder;
    private final File file;
    private final MappedStore ms;
    private final Class<K> kClass;
    private final Class<V> vClass;
    private final Segment<K, V>[] segments;
    private final long lockTimeOutNS;

    public VanillaSharedHashMap(SharedHashMapBuilder builder, File file, MappedStore ms, Class<K> kClass, Class<V> vClass) {
        this.builder = builder;
        lockTimeOutNS = builder.lockTimeOutMS() * 1000000;
        this.file = file;
        this.ms = ms;
        this.kClass = kClass;
        this.vClass = vClass;
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
        if (!kClass.isInstance(key)) return null;
        DirectBytes bytes = acquireBytes();
        bytes.clear().writeObject(key);
        long hash = longHashCode(bytes);
        int segmentNum = (int) (hash & (builder.segments() - 1));
        int hash2 = (int) (hash / builder.segments());
        return segments[segmentNum].get((K) key, bytes, hash2);
    }

    @Override
    public V get(Object key, V value) {
        if (!kClass.isInstance(key)) return null;
        DirectBytes bytes = acquireBytes();
        bytes.clear().writeObject(key);
        long hash = longHashCode(bytes);
        int segmentNum = (int) (hash & (builder.segments() - 1));
        int hash2 = (int) (hash / builder.segments());
        return segments[segmentNum].get((K) key, value, hash2);
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
        /*
        The entry format is
        - stop-bit encoded length for key
        - bytes for the key
        - stop-bit encoded length of the value
        - bytes for the value.
         */
        static final int LOCK = 0;

        private final NativeBytes bytes;
        private final MultiStoreBytes tmpBytes = new MultiStoreBytes();
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

        public void lock() throws IllegalStateException {
            while (true) {
                boolean success = bytes.tryLockNanosInt(LOCK, lockTimeOutNS);
                if (success) return;
                if (Thread.currentThread().isInterrupted()) {
                    throw new IllegalStateException(new InterruptedException("Unable to obtain lock, interrupted"));
                } else {
                    builder.errorListener().onLockTimeout(bytes.threadIdForLockInt(LOCK));
                    bytes.resetLockInt(LOCK);
                }
            }
        }

        public void unlock() {
            try {
                bytes.unlockInt(LOCK);
            } catch (IllegalMonitorStateException e) {
                builder.errorListener().errorOnUnlock(e);
            }
        }

        public V get(K key, DirectBytes keyBytes, int hash2) {
            lock();
            try {
                hashLookup.startSearch(hash2);
                while (true) {
                    int pos = hashLookup.nextInt();
                    if (pos == IntIntMultiMap.UNSET) {
                        return null;

                    } else {
                        tmpBytes.storePositionAndSize(bytes, pos * builder.entrySize(), builder.entrySize());
                        if (keyEquals(keyBytes, tmpBytes))
                            break;
                    }
                }
                return null;
            } finally {
                unlock();
            }
        }

        private boolean keyEquals(DirectBytes keyBytes, MultiStoreBytes tmpBytes) {
            // check the length is the same.
            long keyLength = keyBytes.readStopBit();
            if (keyLength != tmpBytes.position())
                return false;
            return tmpBytes.startsWith(keyBytes);
        }

        public V get(K key, V value, int hash2) {
            return null;
        }
    }
}
