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
import net.openhft.lang.io.*;
import net.openhft.lang.model.Byteable;
import net.openhft.lang.sandbox.collection.ATSDirectBitSet;
import net.openhft.lang.sandbox.collection.DirectBitSet;

import java.util.AbstractMap;
import java.util.Set;

public class VanillaSharedHashMap<K, V> extends AbstractMap<K, V> implements SharedHashMap<K, V> {
    final ThreadLocal<DirectBytes> localBytes = new ThreadLocal<DirectBytes>();
    private final SharedHashMapBuilder builder;
    private final Class<K> kClass;
    private final Class<V> vClass;
    private final long lockTimeOutNS;
    private Segment[] segments;
    private MappedStore ms;

    public VanillaSharedHashMap(SharedHashMapBuilder builder, MappedStore ms, Class<K> kClass, Class<V> vClass) {
        this.builder = builder;
        lockTimeOutNS = builder.lockTimeOutMS() * 1000000;
        this.ms = ms;
        this.kClass = kClass;
        this.vClass = vClass;

        @SuppressWarnings("unchecked")
        Segment[] segments = (VanillaSharedHashMap<K, V>.Segment[]) new VanillaSharedHashMap.Segment[builder.segments()];
        this.segments = segments;

        long offset = SharedHashMapBuilder.HEADER_SIZE;
        long segmentSize = builder.segmentSize();
        for (int i = 0; i < this.segments.length; i++) {
            this.segments[i] = new Segment(ms.createSlice(offset, segmentSize));
            offset += segmentSize;
        }
    }

    @Override
    public void close() {
        if (ms == null)
            return;
        ms.free();
        segments = null;
        ms = null;
    }

    protected DirectBytes acquireBytes() {
        DirectBytes bytes = localBytes.get();
        if (bytes == null) {
            localBytes.set(bytes = new DirectStore(ms.bytesMarshallerFactory(), builder.entrySize() * 2, false).createSlice());
        } else {
            bytes.clear();
        }
        return bytes;
    }

    @Override
    public V get(Object key) {
        return using(key, null, false);
    }

    @Override
    public V getUsing(Object key, V value) {
        return using(key, value, false);
    }

    @Override
    public V acquireUsing(Object key, V value) {
        return using(key, value, true);
    }

    private V using(Object key, V value, boolean create) {
        if (!kClass.isInstance(key)) return null;
        DirectBytes bytes = acquireBytes();
        bytes.writeInstance(kClass, (K) key);
        bytes.flip();
        long hash = longHashCode(bytes);
        int segmentNum = (int) (hash & (builder.segments() - 1));
        int hash2 = (int) (hash / builder.segments());
//        System.out.println("[" + key + "] s: " + segmentNum + " h2: " + hash2);
        return segments[segmentNum].acquire(bytes, value, hash2, create);
    }

    private long longHashCode(DirectBytes bytes) {
        long h = 0;
        int i = 0;
        for (; i < bytes.limit() - 7; i += 8)
            h = 10191 * h + bytes.readLong(i);
//        for (; i < bytes.limit() - 3; i += 2)
//            h = 10191 * h + bytes.readInt(i);
        for (; i < bytes.limit(); i++)
            h = 57 * h + bytes.readByte(i);
        h ^= (h >>> 31) + (h << 31);
        h += (h >>> 21) + (h >>> 11);
        return h;
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

    class Segment {
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
        private final long entriesOffset;
        private int nextSet = 0;

        public Segment(NativeBytes bytes) {
            this.bytes = bytes;
            long start = bytes.startAddr() + SharedHashMapBuilder.SEGMENT_HEADER;
            long size = Maths.nextPower2(builder.entriesPerSegment() * 12, 16 * 8);
            hashLookup = new IntIntMultiMap(new NativeBytes(tmpBytes.bytesMarshallerFactory(), start, start + size, null));
            start += size;
            size = (builder.entriesPerSegment() + 63) / 64 * 8;
            freeList = new ATSDirectBitSet(new NativeBytes(tmpBytes.bytesMarshallerFactory(), start, start + size, null));
            start += size * (1 + builder.replicas());
            entriesOffset = start - bytes.startAddr();
            assert bytes.capacity() >= entriesOffset + builder.entriesPerSegment() * builder.entrySize();
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

        public V acquire(DirectBytes keyBytes, V value, int hash2, boolean create) {
            if (hash2 == hashLookup.unsetKey())
                hash2 = ~hash2;
            lock();
            try {
                hashLookup.startSearch(hash2);
                while (true) {
                    int pos = hashLookup.nextInt();
                    if (pos == hashLookup.unsetValue()) {
                        return createOrNull(keyBytes, value, hash2, create);

                    } else {
                        long offset = entriesOffset + pos * builder.entrySize();
                        tmpBytes.storePositionAndSize(bytes, offset, builder.entrySize());
                        if (!keyEquals(keyBytes, tmpBytes))
                            continue;
                        long keyLength = align(keyBytes.remaining() + tmpBytes.position()); // includes the stop bit length.
                        tmpBytes.skip(keyLength);
                        return readObjectUsing(value, offset + keyLength);
                    }
                }
            } finally {
                unlock();
            }
        }

        private long align(long num) {
            return (num + 3) & ~3;
        }

        private V createOrNull(DirectBytes keyBytes, V value, int hash2, boolean create) {
            int pos;
            if (create) {
                pos = nextFree();
                long offset = entriesOffset + pos * builder.entrySize();
                tmpBytes.storePositionAndSize(bytes, offset, builder.entrySize());
                long keyLength = keyBytes.remaining();
                tmpBytes.writeStopBit(keyLength);
                tmpBytes.write(keyBytes);
                V v = readObjectUsing(value, align(offset + tmpBytes.position()));
                // add to index if successful.
                hashLookup.put(hash2, pos);
                return v;
            }
            return null;
        }

        private int nextFree() {
            int ret = (int) freeList.setOne(nextSet);
            if (ret == DirectBitSet.NOT_FOUND) {
                ret = (int) freeList.setOne(0);
                if (ret == DirectBitSet.NOT_FOUND)
                    throw new IllegalStateException("Segment is full, no free entries found");
            }
            nextSet = ret + 1;
            return ret;
        }

        @SuppressWarnings("unchecked")
        private V readObjectUsing(V value, long offset) {
            if (value instanceof Byteable) {
                ((Byteable) value).bytes(bytes, offset);
                return value;
            }
            return tmpBytes.readInstance(vClass, value);
        }

        private boolean keyEquals(DirectBytes keyBytes, MultiStoreBytes tmpBytes) {
            // check the length is the same.
            long keyLength = tmpBytes.readStopBit();
            if (keyLength != keyBytes.remaining())
                return false;
            return tmpBytes.startsWith(keyBytes);
        }
    }
}
