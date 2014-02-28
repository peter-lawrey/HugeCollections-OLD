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
import net.openhft.lang.collection.DirectBitSet;
import net.openhft.lang.collection.SingleThreadedDirectBitSet;
import net.openhft.lang.io.*;
import net.openhft.lang.io.serialization.BytesMarshallable;
import net.openhft.lang.model.Byteable;
import net.openhft.lang.model.DataValueClasses;

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
    public V put(K key, V value) {
        return put0(key, value, true);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        return put0(key, value, false);
    }

    private V put0(K key, V value, boolean replaceIfPresent) {
        if (!kClass.isInstance(key)) return null;
        DirectBytes bytes = getKeyAsBytes(key);
        long hash = longHashCode(bytes);
        int segmentNum = (int) (hash & (builder.segments() - 1));
        int hash2 = (int) (hash / builder.segments());
//        System.out.println("[" + key + "] s: " + segmentNum + " h2: " + hash2);
        return segments[segmentNum].put(bytes, value, hash2, replaceIfPresent);
    }

    private DirectBytes getKeyAsBytes(K key) {
        DirectBytes bytes = acquireBytes();
        if (builder.generatedKeyType())
            ((BytesMarshallable) key).writeMarshallable(bytes);
        else
            bytes.writeInstance(kClass, key);
        bytes.flip();
        return bytes;
    }

    @Override
    public V get(Object key) {
        return lookupUsing(key, null, false);
    }

    @Override
    public V getUsing(Object key, V value) {
        return lookupUsing(key, value, false);
    }

    @Override
    public V acquireUsing(Object key, V value) {
        return lookupUsing(key, value, true);
    }

    private V lookupUsing(Object key, V value, boolean create) {
        if (!kClass.isInstance(key)) return null;
        DirectBytes bytes = getKeyAsBytes((K) key);
        long hash = longHashCode(bytes);
        int segmentNum = (int) (hash & (builder.segments() - 1));
        int hash2 = (int) (hash / builder.segments());
//        System.out.println("[" + key + "] s: " + segmentNum + " h2: " + hash2);
        return segments[segmentNum].acquire(bytes, value, hash2, create);
    }

    @Override
    public V remove(Object key) {
        return removeUsing(key, null);
    }

    private V removeUsing(Object key, V value) {
        if (!kClass.isInstance(key)) return null;
        DirectBytes bytes = getKeyAsBytes((K) key);
        long hash = longHashCode(bytes);
        int segmentNum = (int) (hash & (builder.segments() - 1));
        int hash2 = (int) (hash / builder.segments());
//        System.out.println("[" + key + "] s: " + segmentNum + " h2: " + hash2);
        return segments[segmentNum].remove(bytes, value, hash2);
    }

    private long longHashCode(DirectBytes bytes) {
        long h = 0;
        int i = 0;
        long limit = bytes.limit(); // clustering.
        for (; i < limit - 7; i += 8)
            h = 10191 * h + bytes.readLong(i);
//        for (; i < bytes.limit() - 3; i += 2)
//            h = 10191 * h + bytes.readInt(i);
        for (; i < limit; i++)
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
        private final SingleThreadedDirectBitSet freeList;
        private final long entriesOffset;
        private int nextSet = 0;

        public Segment(NativeBytes bytes) {
            this.bytes = bytes;
            long start = bytes.startAddr() + SharedHashMapBuilder.SEGMENT_HEADER;
            long size = Maths.nextPower2(builder.entriesPerSegment() * 12, 16 * 8);
            NativeBytes iimmapBytes = new NativeBytes(tmpBytes.bytesMarshallerFactory(), start, start + size, null);
            iimmapBytes.load();
            hashLookup = new IntIntMultiMap(iimmapBytes);
            start += size;
            long bsSize = (builder.entriesPerSegment() + 63) / 64 * 8;
            NativeBytes bsBytes = new NativeBytes(tmpBytes.bytesMarshallerFactory(), start, start + bsSize, null);
//            bsBytes.load();
            freeList = new SingleThreadedDirectBitSet(bsBytes);
            start += bsSize * (1 + builder.replicas());
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
                        return create ? acquireEntry(keyBytes, value, hash2) : null;

                    } else {
                        long offset = entriesOffset + pos * builder.entrySize();
                        tmpBytes.storePositionAndSize(bytes, offset, builder.entrySize());
                        long start0 = System.nanoTime();
                        boolean miss = !keyEquals(keyBytes, tmpBytes);
                        long time0 = System.nanoTime() - start0;
                        if (time0 > 1e6)
                            System.out.println("startsWith took " + time0 / 100000 / 10.0 + " ms.");
                        if (miss)
                            continue;
                        long keyLength = align(keyBytes.remaining() + tmpBytes.position()); // includes the stop bit length.
                        tmpBytes.position(keyLength);
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

        private V acquireEntry(DirectBytes keyBytes, V value, int hash2) {
            int pos = nextFree();
            long offset = entriesOffset + pos * builder.entrySize();
            tmpBytes.storePositionAndSize(bytes, offset, builder.entrySize());
            long keyLength = keyBytes.remaining();
            tmpBytes.writeStopBit(keyLength);
            tmpBytes.write(keyBytes);
            tmpBytes.position(align(tmpBytes.position()));
            tmpBytes.zeroOut(tmpBytes.position(), tmpBytes.limit());
            V v = readObjectUsing(value, offset + tmpBytes.position());
            // add to index if successful.
            hashLookup.put(hash2, pos);
            return v;
        }

        private void putEntry(DirectBytes keyBytes, V value, int hash2) {
            int pos = nextFree();
            long offset = entriesOffset + pos * builder.entrySize();
            tmpBytes.storePositionAndSize(bytes, offset, builder.entrySize());
            long keyLength = keyBytes.remaining();
            tmpBytes.writeStopBit(keyLength);
            tmpBytes.write(keyBytes);
            tmpBytes.position(align(tmpBytes.position()));
            appendInstance(keyBytes, value);
            // add to index if successful.
            hashLookup.put(hash2, pos);
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
            if (builder.generatedValueType()) {
                if (value == null)
                    value = DataValueClasses.newInstance(vClass);
                ((BytesMarshallable) value).readMarshallable(tmpBytes);
                return value;
            }
            return tmpBytes.readInstance(vClass, value);
        }

        private boolean keyEquals(DirectBytes keyBytes, MultiStoreBytes tmpBytes) {
            // check the length is the same.
            long keyLength = tmpBytes.readStopBit();
            return keyLength == keyBytes.remaining()
                    && tmpBytes.startsWith(keyBytes);
        }

        public V remove(DirectBytes keyBytes, V value, int hash2) {
            if (hash2 == hashLookup.unsetKey())
                hash2 = ~hash2;
            lock();
            try {
                hashLookup.startSearch(hash2);
                while (true) {
                    int pos = hashLookup.nextInt();
                    if (pos == hashLookup.unsetValue()) {
                        return null;

                    } else {
                        long offset = entriesOffset + pos * builder.entrySize();
                        tmpBytes.storePositionAndSize(bytes, offset, builder.entrySize());
                        if (!keyEquals(keyBytes, tmpBytes))
                            continue;
                        long keyLength = align(keyBytes.remaining());
                        tmpBytes.skip(keyLength);
                        V v = value == null && builder.removeReturnsNull() ? null : readObjectUsing(value, offset + keyLength);
                        hashLookup.remove(hash2, pos);
                        freeList.clear(pos);
                        if (pos < nextSet)
                            nextSet = pos;
                        return v;
                    }
                }
            } finally {
                unlock();
            }
        }

        public V put(DirectBytes keyBytes, V value, int hash2, boolean replaceIfPresent) {
            if (hash2 == hashLookup.unsetKey())
                hash2 = ~hash2;
            lock();
            try {
                hashLookup.startSearch(hash2);
                while (true) {
                    int pos = hashLookup.nextInt();
                    if (pos == hashLookup.unsetValue()) {
                        putEntry(keyBytes, value, hash2);
                        return null;

                    } else {
                        long offset = entriesOffset + pos * builder.entrySize();
                        tmpBytes.storePositionAndSize(bytes, offset, builder.entrySize());
                        if (!keyEquals(keyBytes, tmpBytes))
                            continue;
                        long keyLength = keyBytes.remaining();
                        tmpBytes.skip(keyLength);
                        long alignPosition = align(tmpBytes.position());
                        tmpBytes.position(alignPosition);
                        if (replaceIfPresent) {
                            if (builder.putReturnsNull()) {
                                appendInstance(keyBytes, value);
                                return null;
                            }
                            V v = readObjectUsing(null, offset + alignPosition);
                            tmpBytes.position(alignPosition);
                            appendInstance(keyBytes, value);
                            return v;

                        } else {
                            if (builder.putReturnsNull()) {
                                return null;
                            }
                            return readObjectUsing(null, offset + keyLength);
                        }
                    }
                }
            } finally {
                unlock();
            }
        }

        private void appendInstance(DirectBytes bytes, V value) {
            bytes.clear();
            if (builder.generatedValueType())
                ((BytesMarshallable) value).writeMarshallable(bytes);
            else
                bytes.writeInstance(vClass, value);
            bytes.flip();
            if (bytes.remaining() > tmpBytes.remaining())
                throw new IllegalArgumentException("Value too large for entry was " + bytes.remaining() + ", remaining: " + tmpBytes.remaining());
            tmpBytes.write(bytes);
        }
    }
}
