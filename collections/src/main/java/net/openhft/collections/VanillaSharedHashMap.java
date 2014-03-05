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
import net.openhft.lang.io.DirectBytes;
import net.openhft.lang.io.DirectStore;
import net.openhft.lang.io.MappedStore;
import net.openhft.lang.io.MultiStoreBytes;
import net.openhft.lang.io.NativeBytes;
import net.openhft.lang.io.serialization.BytesMarshallable;
import net.openhft.lang.model.Byteable;
import net.openhft.lang.model.DataValueClasses;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.AbstractMap;
import java.util.Set;

public class VanillaSharedHashMap<K, V> extends AbstractMap<K, V> implements SharedHashMap<K, V> {
    final ThreadLocal<DirectBytes> localBytes = new ThreadLocal<DirectBytes>();
    private final Class<K> kClass;
    private final Class<V> vClass;
    private final long lockTimeOutNS;
    private Segment[] segments;
    private MappedStore ms;
    
    private final int replicas;
    private final int entrySize;
    private final long entriesPerSegment;
    private final int bitSetSizeInBytes;

    private final SharedMapErrorListener errorListener;
    private final boolean generatedKeyType;
    private final boolean generatedValueType;
    private final boolean putReturnsNull;
    private final boolean removeReturnsNull;

    public VanillaSharedHashMap(SharedHashMapBuilder builder, File file,
            Class<K> kClass, Class<V> vClass) throws IOException {
        this.kClass = kClass;
        this.vClass = vClass;

        lockTimeOutNS = builder.lockTimeOutMS() * 1000000;

        this.replicas = builder.replicas();
        this.entrySize = builder.entrySize();

        this.errorListener = builder.errorListener();
        this.generatedKeyType = builder.generatedKeyType();
        this.generatedValueType = builder.generatedValueType();
        this.putReturnsNull = builder.putReturnsNull();
        this.removeReturnsNull = builder.removeReturnsNull();

        long entries = builder.entries();
        /**
         * As the entries will not be spaced evenly across segments,
         * one segment can fill while others are not.
         * The simple work around for now is to give the SHM
         * a capacity of entries * 3 / 2.
         */
        long entriesForReliability = (long) (entries * 1.5);

        long segments = builder.minSegments();
        if (entriesPerSegment(entriesForReliability, segments) >
                Integer.MAX_VALUE) {
            segments = (entriesForReliability + Integer.MAX_VALUE - 1) /
                    Integer.MAX_VALUE;
            while (entriesPerSegment(entriesForReliability, segments) >
                    Integer.MAX_VALUE) {
                segments++;
            }
        }
        if (segments > Integer.MAX_VALUE)
            throw new IllegalStateException();
        
        entriesPerSegment = entriesPerSegment(entriesForReliability, segments);
        bitSetSizeInBytes = (int) (entriesPerSegment / 8);

        @SuppressWarnings("unchecked")
        Segment[] ss = (VanillaSharedHashMap.Segment[])
                new VanillaSharedHashMap.Segment[(int) segments];
        this.segments = ss;

        this.ms = new MappedStore(file, FileChannel.MapMode.READ_WRITE,
                sizeInBytes());

        long offset = SharedHashMapBuilder.HEADER_SIZE;
        long segmentSize = segmentSize();
        for (int i = 0; i < this.segments.length; i++) {
            this.segments[i] = new Segment(ms.createSlice(offset, segmentSize));
            offset += segmentSize;
        }
    }
    
    private static long entriesPerSegment(long entries, long segments) {
        long minEPS = (entries + segments - 1) / segments;
        // for bit set
        return (minEPS + 63) / 64 * 64;
    }

    long sizeInBytes() {
        return SharedHashMapBuilder.HEADER_SIZE +
                segments.length * segmentSize();
    }

    long segmentSize() {
        return (SharedHashMapBuilder.SEGMENT_HEADER
                + Maths.nextPower2(entriesPerSegment * 12, 16 * 8) // the IntIntMultiMap
                + (1 + replicas) * bitSetSizeInBytes // the free list and 0+ dirty lists.
                + entriesPerSegment * entrySize); // the actual entries used.
    }

    /**
     * {@inheritDoc}
     */
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
            localBytes.set(bytes = new DirectStore(ms.bytesMarshallerFactory(), entrySize * 2, false).createSlice());
        } else {
            bytes.clear();
        }
        return bytes;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V put(K key, V value) {
        return put0(key, value, true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V putIfAbsent(K key, V value) {
        return put0(key, value, false);
    }

    private V put0(K key, V value, boolean replaceIfPresent) {
        if (!kClass.isInstance(key)) return null;
        DirectBytes bytes = getKeyAsBytes(key);
        long hash = longHashCode(bytes);
        int segmentNum = (int) (hash & (segments.length - 1));
        int hash2 = (int) (hash / segments.length);
//        System.out.println("[" + key + "] s: " + segmentNum + " h2: " + hash2);
        return segments[segmentNum].put(bytes, value, hash2, replaceIfPresent);
    }

    private DirectBytes getKeyAsBytes(K key) {
        DirectBytes bytes = acquireBytes();
        if (generatedKeyType)
            ((BytesMarshallable) key).writeMarshallable(bytes);
        else
            bytes.writeInstance(kClass, key);
        bytes.flip();
        return bytes;
    }

    /**
     * {@inheritDoc}
     */
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
        int segmentNum = (int) (hash & (segments.length - 1));
        int hash2 = (int) (hash / segments.length);
//        System.out.println("[" + key + "] s: " + segmentNum + " h2: " + hash2);
        return segments[segmentNum].acquire(bytes, value, hash2, create);
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public V remove(Object key) {
        return removeWith(key, null);
    }

    /**
     *
     * @param key the key of the entry to remove
     * @param expectedValue null if not required
     * @return
     */
    private V removeWith(Object key, V expectedValue) {
        if (!kClass.isInstance(key)) return null;
        DirectBytes bytes = getKeyAsBytes((K) key);
        long hash = longHashCode(bytes);
        int segmentNum = (int) (hash & (segments.length - 1));
        int hash2 = (int) (hash / segments.length);

        return segments[segmentNum].remove(bytes, expectedValue, hash2);
    }

    /**
     * replace the value in a map, only if the existing entry equals {@param existingValue}
     *
     * @param key           the key into the map
     * @param existingValue the expected existing value in the map ( could be null when we don't wish to do this check )
     * @param newValue      the new value you wish to store in the map
     * @return the value that was replaced
     */
    private V replaceUsing(final Object key, final V existingValue, final V newValue) {
        if (!kClass.isInstance(key)) return null;
        final DirectBytes bytes = getKeyAsBytes((K) key);
        final long hash = longHashCode(bytes);
        final int segmentNum = (int) (hash & (segments.length - 1));
        final int hash2 = (int) (hash / segments.length);
//        System.out.println("[" + key + "] s: " + segmentNum + " h2: " + hash2);
        return segments[segmentNum].replace(bytes, existingValue, newValue, hash2);
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


    /**
     * {@inheritDoc}
     *
     * @throws NullPointerException if the specified key is null
     */
    @Override
    public boolean remove(Object key, Object value) {
        final V v = removeWith(key, (V) value);
        return v != null;
    }

    /**
     * {@inheritDoc}
     *
     * @throws NullPointerException if any of the arguments are null
     */
    @Override
    public boolean replace(K key, V oldValue, V newValue) {

        if (key == null)
            throw new NullPointerException("'key' can not be null");

        if (oldValue == null)
            throw new NullPointerException("'oldValue' can not be null");

        if (newValue == null)
            throw new NullPointerException("'newValue' can not be null");

        return oldValue.equals(replaceUsing(key, oldValue, newValue));
    }


    /**
     * {@inheritDoc}
     *
     * @return the previous value associated with the specified key,
     * or <tt>null</tt> if there was no mapping for the key
     * @throws NullPointerException if the specified key or value is null
     */
    @Override
    public V replace(K key, V value) {

        if (key == null)
            throw new NullPointerException("'key' can not be null");

        if (value == null)
            throw new NullPointerException("'value' can not be null");

        return replaceUsing(key, (V) null, value);
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
        private final HashPosMultiMap hashLookup;
        private final SingleThreadedDirectBitSet freeList;
        private final long entriesOffset;
        private int nextSet = 0;

        public Segment(NativeBytes bytes) {
            this.bytes = bytes;
            long start = bytes.startAddr() + SharedHashMapBuilder.SEGMENT_HEADER;
            long size = Maths.nextPower2(entriesPerSegment * 12, 16 * 8);
            NativeBytes iimmapBytes = new NativeBytes(tmpBytes.bytesMarshallerFactory(), start, start + size, null);
            iimmapBytes.load();
            hashLookup = new IntIntMultiMap(iimmapBytes);
            start += size;
            NativeBytes bsBytes = new NativeBytes(tmpBytes.bytesMarshallerFactory(), start, start + bitSetSizeInBytes, null);
//            bsBytes.load();
            freeList = new SingleThreadedDirectBitSet(bsBytes);
            start += bitSetSizeInBytes * (1 + replicas);
            entriesOffset = start - bytes.startAddr();
            assert bytes.capacity() >= entriesOffset + entriesPerSegment * entrySize;
        }

        public void lock() throws IllegalStateException {
            while (true) {
                boolean success = bytes.tryLockNanosInt(LOCK, lockTimeOutNS);
                if (success) return;
                if (Thread.currentThread().isInterrupted()) {
                    throw new IllegalStateException(new InterruptedException("Unable to obtain lock, interrupted"));
                } else {
                    errorListener.onLockTimeout(bytes.threadIdForLockInt(LOCK));
                    bytes.resetLockInt(LOCK);
                }
            }
        }

        public void unlock() {
            try {
                bytes.unlockInt(LOCK);
            } catch (IllegalMonitorStateException e) {
                errorListener.errorOnUnlock(e);
            }
        }


        /**
         * used to acquire and object of type V from the map
         *
         * @param keyBytes
         * @param value
         * @param hash2    the hash code of the object to acquire
         * @param create
         * @return
         */
        public V acquire(DirectBytes keyBytes, V value, int hash2, boolean create) {
            lock();
            try {
                hash2 = hashLookup.startSearch(hash2);
                while (true) {
                    int pos = hashLookup.nextPos();
                    if (pos < 0) {
                        return create ? acquireEntry(keyBytes, value, hash2) : null;

                    } else {
                        long offset = entriesOffset + pos * entrySize;
                        tmpBytes.storePositionAndSize(bytes, offset, entrySize);
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
            long offset = entriesOffset + pos * entrySize;
            tmpBytes.storePositionAndSize(bytes, offset, entrySize);
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
            long offset = entriesOffset + pos * entrySize;
            tmpBytes.storePositionAndSize(bytes, offset, entrySize);
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

        /**
         * readObjectUsing - the "using" part means, reuse this object if possible.
         *
         * @param value  null - creates an object on demand.  It shouldn't be null in most examples, otherwise it will reuse this object
         * @param offset
         * @return
         */
        @SuppressWarnings("unchecked")
        private V readObjectUsing(V value, long offset) {
            if (value instanceof Byteable) {
                ((Byteable) value).bytes(bytes, offset);
                return value;
            }
            if (generatedValueType) {
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

        /**
         * @param keyBytes
         * @param expectedValue if null no check if performed, otherwise, the remove will only occur if the value to be removed equals the expected value
         * @param hash2
         * @return
         */
        public V remove(DirectBytes keyBytes, V expectedValue, int hash2) {
            lock();
            try {
                hash2 = hashLookup.startSearch(hash2);
                while (true) {
                    int pos = hashLookup.nextPos();
                    if (pos < 0) {
                        return null;

                    } else {
                        long offset = entriesOffset + pos * entrySize;
                        tmpBytes.storePositionAndSize(bytes, offset, entrySize);
                        if (!keyEquals(keyBytes, tmpBytes))
                            continue;
                        long keyLength = align(keyBytes.remaining() + tmpBytes.position()); // includes the stop bit length.
                        tmpBytes.position(keyLength);
                        V valueRemoved = expectedValue == null && removeReturnsNull ? null : readObjectUsing(null, offset + keyLength);

                        if (expectedValue != null && !expectedValue.equals(valueRemoved))
                            return null;

                        hashLookup.remove(hash2, pos);
                        freeList.clear(pos);
                        if (pos < nextSet)
                            nextSet = pos;
                        return valueRemoved;
                    }
                }
            } finally {
                unlock();
            }
        }


        /**
         * implementation for map.replace(Key,Value) and map.replace(Key,Old,New)
         *
         * @param keyBytes      the key of the entry to be replaced
         * @param expectedValue the expected value to replaced
         * @param newValue      the new value that will only be set if the existing value in the map equals the {@param expectedValue} or  {@param expectedValue} is null
         * @param hash2         the hash code
         * @return null if the value was not replaced, else the value that is replaced is returned
         */
        private V replace(DirectBytes keyBytes, V expectedValue, V newValue, int hash2) {
            lock();
            try {

                hashLookup.startSearch(hash2);
                while (true) {

                    final int pos = hashLookup.nextPos();

                    if (pos < 0) {
                        return null;

                    } else {

                        final long offset = entriesOffset + pos * entrySize;
                        tmpBytes.storePositionAndSize(bytes, offset, entrySize);

                        if (!keyEquals(keyBytes, tmpBytes))
                            continue;
                        final long keyLength = keyBytes.remaining();
                        tmpBytes.skip(keyLength);
                        final long alignPosition = align(tmpBytes.position());
                        tmpBytes.position(alignPosition);
                        final V valueRead = readObjectUsing(null, offset + keyLength);

                        if (valueRead == null)
                            return null;

                        if (expectedValue == null || expectedValue.equals(valueRead)) {
                            tmpBytes.position(alignPosition);
                            appendInstance(keyBytes, newValue);
                        }

                        return valueRead;
                    }
                }
            } finally {
                unlock();
            }
        }


        public V put(DirectBytes keyBytes, V value, int hash2, boolean replaceIfPresent) {
            lock();
            try {
                hash2 = hashLookup.startSearch(hash2);
                while (true) {
                    final int pos = hashLookup.nextPos();
                    if (pos < 0) {
                        putEntry(keyBytes, value, hash2);
                        return null;

                    } else {
                        final long offset = entriesOffset + pos * entrySize;
                        tmpBytes.storePositionAndSize(bytes, offset, entrySize);
                        if (!keyEquals(keyBytes, tmpBytes))
                            continue;
                        final long keyLength = keyBytes.remaining();
                        tmpBytes.skip(keyLength);
                        final long alignPosition = align(tmpBytes.position());
                        tmpBytes.position(alignPosition);
                        if (replaceIfPresent) {
                            if (putReturnsNull) {
                                appendInstance(keyBytes, value);
                                return null;
                            }
                            final V v = readObjectUsing(null, offset + alignPosition);
                            tmpBytes.position(alignPosition);
                            appendInstance(keyBytes, value);
                            return v;

                        } else {
                            if (putReturnsNull) {
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
            if (generatedValueType)
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
