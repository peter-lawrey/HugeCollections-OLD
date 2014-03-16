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
import net.openhft.lang.model.constraints.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.lang.Thread.currentThread;

public class VanillaSharedHashMap<K, V> extends AbstractMap<K, V> implements SharedHashMap<K, V>, DirectMap {
    private static final Logger LOGGER = Logger.getLogger(VanillaSharedHashMap.class.getName());
    private final ThreadLocal<DirectBytes> localBytes = new ThreadLocal<DirectBytes>();
    private final Class<K> kClass;
    private final Class<V> vClass;
    private final long lockTimeOutNS;
    private final int metaDataBytes;
    private Segment[] segments; // non-final for close()
    private MappedStore ms;     // non-final for close()
    private final Hasher hasher;

    private final int replicas;
    private final int entrySize;
    private final int entriesPerSegment;
    private final int hashMask;

    private final SharedMapErrorListener errorListener;
    private final SharedMapEventListener<K, V> eventListener;
    private final boolean generatedKeyType;
    private final boolean generatedValueType;
    private final boolean putReturnsNull;
    private final boolean removeReturnsNull;

    transient Set<Map.Entry<K, V>> entrySet;


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

        int segments = builder.actualSegments();
        int entriesPerSegment = builder.actualEntriesPerSegment();
        this.entriesPerSegment = entriesPerSegment;
        this.metaDataBytes = builder.metaDataBytes();
        this.eventListener = builder.eventListener();
        this.hashMask = entriesPerSegment > (1 << 16) ? ~0 : 0xFFFF;

        this.hasher = new Hasher(segments, hashMask);

        @SuppressWarnings("unchecked")
        Segment[] ss = (VanillaSharedHashMap.Segment[])
                new VanillaSharedHashMap.Segment[segments];
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

    @Override
    public File file() {
        return ms.file();
    }

    @Override
    public SharedHashMapBuilder builder() {
        return new SharedHashMapBuilder()
                .actualSegments(segments.length)
                .actualEntriesPerSegment(entriesPerSegment)
                .entries((long) segments.length * entriesPerSegment / 2)
                .entrySize(entrySize)
                .errorListener(errorListener)
                .generatedKeyType(generatedKeyType)
                .generatedValueType(generatedValueType)
                .lockTimeOutMS(lockTimeOutNS / 1000000)
                .minSegments(segments.length)
                .putReturnsNull(putReturnsNull)
                .removeReturnsNull(removeReturnsNull)
                .replicas(replicas)
                .transactional(false)
                .metaDataBytes(metaDataBytes)
                .eventListener(eventListener);
    }

    long sizeInBytes() {
        return SharedHashMapBuilder.HEADER_SIZE +
                segments.length * segmentSize();
    }

    long sizeOfMultiMap() {
        int np2 = Maths.nextPower2(entriesPerSegment, 8);
        return align64(np2 * (entriesPerSegment > (1 << 16) ? 8L : 4L));
    }

    long sizeOfBitSets() {
        return align64(entriesPerSegment / 8);
    }

    int numberOfBitSets() {
        return 1 // for free list
                + (replicas > 0 ? 1 : 0) // deleted set
                + replicas; // to notify each replica of a change.
    }

    long segmentSize() {
        long ss = SharedHashMapBuilder.SEGMENT_HEADER
                + sizeOfMultiMap() // the VanillaIntIntMultiMap
                + numberOfBitSets() * sizeOfBitSets() // the free list and 0+ dirty lists.
                + sizeOfEntriesInSegment();
        assert (ss & 63) == 0;
        return ss; // the actual entries used.
    }

    private long sizeOfEntriesInSegment() {
        return align64((long) entriesPerSegment * entrySize);
    }

    /**
     * Cache line alignment, assuming 64-byte cache lines.
     */
    private long align64(long l) {
        // 64-byte alignment.
        return (l + 63) & ~63;
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

    DirectBytes acquireBytes() {
        DirectBytes bytes = localBytes.get();
        if (bytes == null) {
            localBytes.set(bytes = new DirectStore(ms.bytesMarshallerFactory(), entrySize * 2, false).createSlice());
        } else {
            bytes.clear();
        }
        return bytes;
    }

    private void checkKey(Object key) {
        if (!kClass.isInstance(key)) {
            // key.getClass will cause NPE exactly as needed
            throw new ClassCastException("Key must be a " + kClass.getName() +
                    " but was a " + key.getClass());
        }
    }

    private void checkValue(Object value) {
        if (!vClass.isInstance(value)) {
            throw new ClassCastException("Value must be a " + vClass.getName() +
                    " but was a " + value.getClass());
        }
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
        checkKey(key);
        checkValue(value);
        DirectBytes bytes = getKeyAsBytes(key);
        long hash = hasher.hash(bytes);
        int segmentNum = hasher.getSegment(hash);
        int segmentHash = hasher.segmentHash(hash);
        return segments[segmentNum].put(bytes, key, value, segmentHash, replaceIfPresent);
    }

    @Override
    public void put(Bytes key, Bytes value) {
        long hash = hasher.hash(value);
        int segmentNum = hasher.getSegment(hash);
        int segmentHash = hasher.segmentHash(hash);
        segments[segmentNum].directPut(key, value, segmentHash, null, null);
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
        return lookupUsing((K) key, null, false);
    }

    @Override
    public V getUsing(K key, V value) {
        return lookupUsing(key, value, false);
    }

    @Override
    public V acquireUsing(K key, V value) {
        return lookupUsing(key, value, true);
    }

    private V lookupUsing(K key, V value, boolean create) {
        checkKey(key);
        DirectBytes bytes = getKeyAsBytes(key);
        long hash = hasher.hash(bytes);
        int segmentNum = hasher.getSegment(hash);
        int segmentHash = hasher.segmentHash(hash);
        return segments[segmentNum].acquire(bytes, key, value, segmentHash, create);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsKey(final Object key) {
        checkKey(key);
        final DirectBytes bytes = getKeyAsBytes((K) key);
        long hash = hasher.hash(bytes);
        int segmentNum = hasher.getSegment(hash);
        int segmentHash = hasher.segmentHash(hash);

        return segments[segmentNum].containsKey(bytes, segmentHash);
    }

    @Override
    public void clear() {
        for (Segment segment : segments)
            segment.clear();
    }

    /**
     * {@inheritDoc}
     */
    @NotNull
    @Override
    public Set<Entry<K, V>> entrySet() {
        return (entrySet != null) ? entrySet : (entrySet = new EntrySet());
    }


    /**
     * {@inheritDoc}
     *
     * @throws NullPointerException if the specified key is null
     */
    @Override
    public V remove(final Object key) {
        return removeIfValueIs(key, null);
    }

    /**
     * {@inheritDoc}
     *
     * @throws NullPointerException if the specified key is null
     */
    @Override
    public boolean remove(final Object key, final Object value) {
        if (value == null)
            return false; // CHM compatibility; I would throw NPE
        return removeIfValueIs(key, (V) value) != null;
    }


    /**
     * removes ( if there exists ) an entry from the map, if the {@param key} and {@param expectedValue} match that of a maps.entry.
     * If the {@param expectedValue} equals null then ( if there exists ) an entry whose key equals {@param key} this is removed.
     *
     * @param key           the key of the entry to remove
     * @param expectedValue null if not required
     * @return true if and entry was removed
     */
    private V removeIfValueIs(final Object key, final V expectedValue) {
        checkKey(key);
        final DirectBytes bytes = getKeyAsBytes((K) key);
        long hash = hasher.hash(bytes);
        int segmentNum = hasher.getSegment(hash);
        int segmentHash = hasher.segmentHash(hash);
        return segments[segmentNum].remove(bytes, (K) key, expectedValue, segmentHash);
    }

    @Override
    public void remove(Bytes keyBytes) {
        long hash = hasher.hash(keyBytes);
        int segmentNum = hasher.getSegment(hash);
        int segmentHash = hasher.segmentHash(hash);
        segments[segmentNum].directRemove(keyBytes, segmentHash);
    }

    /**
     * {@inheritDoc}
     *
     * @throws NullPointerException if any of the arguments are null
     */
    @Override
    public boolean replace(final K key, final V oldValue, final V newValue) {
        checkValue(oldValue);
        return oldValue.equals(replaceIfValueIs(key, oldValue, newValue));
    }


    /**
     * {@inheritDoc}
     *
     * @return the previous value associated with the specified key,
     * or <tt>null</tt> if there was no mapping for the key
     * @throws NullPointerException if the specified key or value is null
     */
    @Override
    public V replace(final K key, final V value) {
        return replaceIfValueIs(key, null, value);
    }


    // TODO uncomment once tested -  HCOLL-16  implement map.size()

    /**
     * {@inheritDoc}
     */

    public int size() {
        long result = 0;

        for (final Segment segment : this.segments) {
            result += segment.getSize();
        }

        return (int) result;

    }

    /**
     * replace the value in a map, only if the existing entry equals {@param existingValue}
     *
     * @param key           the key into the map
     * @param existingValue the expected existing value in the map ( could be null when we don't wish to do this check )
     * @param newValue      the new value you wish to store in the map
     * @return the value that was replaced
     */
    private V replaceIfValueIs(@NotNull final K key, final V existingValue, final V newValue) {
        checkKey(key);
        checkValue(newValue);
        final DirectBytes bytes = getKeyAsBytes(key);
        long hash = hasher.hash(bytes);
        int segmentNum = hasher.getSegment(hash);
        int segmentHash = hasher.segmentHash(hash);
        return segments[segmentNum].replace(bytes, key, existingValue, newValue, segmentHash);
    }


    static final class Hasher<K> {

        private final int segments;

        private final int bits;

        private final int mask;

        Hasher(int segments, int mask) {
            this.segments = segments;
            this.bits = Maths.intLog2(segments);
            this.mask = mask;
        }

        final long hash(Bytes bytes) {
            long h = 0;
            int i = 0;
            long limit = bytes.limit(); // clustering.
            for (; i < limit - 7; i += 8)
                h = 1011001110001111L * h + bytes.readLong(i);
            for (; i < limit - 1; i += 2)
                h = 101111 * h + bytes.readShort(i);
            if (i < limit)
                h = 2111 * h + bytes.readByte(i);
            h *= 11018881818881011L;
            h ^= (h >>> 41) ^ (h >>> 21);
            //System.out.println(bytes + " => " + Long.toHexString(h));
            return h;
        }

        final int segmentHash(long hash) {
            return (int) (hash >>> bits) & mask;
        }

        public final int getSegment(long hash) {
            return (int) (hash & (segments - 1));
        }
    }

    // these methods should be package local, not public or private.
    class Segment {
        /*
        The entry format is
        - stop-bit encoded length for key
        - bytes for the key
        - stop-bit encoded length of the value
        - bytes for the value.
         */
        static final int LOCK_OFFSET = 0; // 64-bit
        static final int SIZE_OFFSET = LOCK_OFFSET + 8; // 32-bit
        static final int PAD1_OFFSET = SIZE_OFFSET + 4; // 32-bit
        static final int REPLICA_OFFSET = PAD1_OFFSET + 4; // 64-bit

        private final NativeBytes bytes;
        private final MultiStoreBytes tmpBytes = new MultiStoreBytes();
        private final IntIntMultiMap hashLookup;
        private final SingleThreadedDirectBitSet freeList;
        private final long entriesOffset;
        private int nextSet = 0;

        Segment(NativeBytes bytes) {
            this.bytes = bytes;

            long start = bytes.startAddr() + SharedHashMapBuilder.SEGMENT_HEADER;
            final NativeBytes iimmapBytes = new NativeBytes(null, start, start + sizeOfMultiMap(), null);
            iimmapBytes.load();
            hashLookup = hashMask == ~0 ? new VanillaIntIntMultiMap(iimmapBytes) : new VanillaShortShortMultiMap(iimmapBytes);
            start += sizeOfMultiMap();
            final NativeBytes bsBytes = new NativeBytes(tmpBytes.bytesMarshallerFactory(), start, start + sizeOfBitSets(), null);
            freeList = new SingleThreadedDirectBitSet(bsBytes);
            start += numberOfBitSets() * sizeOfBitSets();
            entriesOffset = start - bytes.startAddr();
            assert bytes.capacity() >= entriesOffset + entriesPerSegment * entrySize;
        }

        /**
         * increments the size by one
         */
        private void incrementSize() {
            this.bytes.addInt(SIZE_OFFSET, 1);
        }

        public void resetSize() {
            this.bytes.writeInt(SIZE_OFFSET, 0);
        }

        /**
         * decrements the size by one
         */
        private void decrementSize() {
            this.bytes.addInt(SIZE_OFFSET, -1);
        }

        /**
         * reads the the number of entries in this segment
         */
        int getSize() {
            // any negative value is in error state.
            return Math.max(0, this.bytes.readVolatileInt(SIZE_OFFSET));
        }


        void lock() throws IllegalStateException {
            while (true) {
                final boolean success = bytes.tryLockNanosLong(LOCK_OFFSET, lockTimeOutNS);
                if (success) return;
                if (currentThread().isInterrupted()) {
                    throw new IllegalStateException(new InterruptedException("Unable to obtain lock, interrupted"));
                } else {
                    errorListener.onLockTimeout(bytes.threadIdForLockLong(LOCK_OFFSET));
                    bytes.resetLockLong(LOCK_OFFSET);
                }
            }
        }

        void unlock() {
            try {
                bytes.unlockLong(LOCK_OFFSET);
            } catch (IllegalMonitorStateException e) {
                errorListener.errorOnUnlock(e);
            }
        }


        /**
         * used to acquire and object of type V from the map,
         * <p/>
         * when {@param create }== true, this method is equivalent to :
         * <pre>
         * Object value = map.get("Key");
         *
         * if ( counter == null ) {
         *    value = new Object();
         *    map.put("Key", value);
         * }
         *
         * return value;
         * </pre>
         *
         * @param keyBytes   the key of the entry
         * @param usingValue an object to be reused, null creates a new object.
         * @param hash2      a hash code relating to the @keyBytes ( not the natural hash of {@keyBytes}  )
         * @param create     false - if the  {@keyBytes} can not be found null will be returned, true - if the  {@keyBytes} can not be found an value will be acquired
         * @return an entry.value whose entry.key equals {@param keyBytes}
         */
        V acquire(DirectBytes keyBytes, K key, V usingValue, int hash2, boolean create) {
            lock();
            try {
                hash2 = hashLookup.startSearch(hash2);
                while (true) {
                    int pos = hashLookup.nextPos();
                    if (pos < 0) {
                        return create ? acquireEntry(keyBytes, key, usingValue, hash2) : notifyMissed(keyBytes, key, usingValue, hash2);

                    } else {
                        final long offset = entriesOffset + pos * entrySize + metaDataBytes;
                        tmpBytes.storePositionAndSize(bytes, offset, entrySize - metaDataBytes);
                        final boolean miss;
                        if (LOGGER.isLoggable(Level.FINE)) {
                            final long start0 = System.nanoTime();
                            miss = !keyEquals(keyBytes, tmpBytes);
                            final long time0 = System.nanoTime() - start0;
                            if (time0 > 1e6)
                                LOGGER.fine("startsWith took " + time0 / 100000 / 10.0 + " ms.");
                        } else {
                            miss = !keyEquals(keyBytes, tmpBytes);
                        }
                        if (miss)
                            continue;
                        long valueLengthOffset = keyBytes.remaining() + tmpBytes.position();
                        tmpBytes.position(valueLengthOffset);
                        // skip the value length
                        // todo use the value length to limit reading below
                        long valueLength = tmpBytes.readStopBit();
                        final long valueOffset = align(tmpBytes.position()); // includes the stop bit length.
                        tmpBytes.position(valueOffset);
                        V v = readObjectUsing(usingValue, offset + valueOffset);
                        notifyGet(offset - metaDataBytes, key, v);
                        return v;
                    }
                }
            } finally {
                unlock();
            }
        }

        long align(long num) {
            return (num + 3) & ~3;
        }

        /**
         * @param keyBytes the key of the entry
         * @param value    to reuse if not null.
         * @param hash2    a hash code relating to the {@keyBytes} ( not the natural hash of {@keyBytes}  )
         * @return
         */

        V acquireEntry(DirectBytes keyBytes, K key, V value, int hash2) {
            value = createValueIfNull(value);

            final int pos = nextFree();
            final long offset = entriesOffset + pos * entrySize + metaDataBytes;
            tmpBytes.storePositionAndSize(bytes, offset, entrySize - metaDataBytes);
            final long keyLength = keyBytes.remaining();
            tmpBytes.writeStopBit(keyLength);
            tmpBytes.write(keyBytes);
            if (value instanceof Byteable) {
                Byteable byteable = (Byteable) value;
                int length = byteable.maxSize();
                tmpBytes.writeStopBit(length);
                tmpBytes.position(align(tmpBytes.position()));
                if (length > tmpBytes.remaining())
                    throw new IllegalStateException("Not enough space left in entry for value, needs " + length + " but only " + tmpBytes.remaining() + " left");
                tmpBytes.zeroOut(tmpBytes.position(), tmpBytes.position() + length);
                byteable.bytes(bytes, offset + tmpBytes.position());
            } else {
                appendInstance(keyBytes, value);
            }
            // add to index if successful.
            hashLookup.put(hash2, pos);
            incrementSize();
            notifyPut(offset, true, key, value);
            return value;
        }

        private V createValueIfNull(V value) {
            if (value == null) {
                if (generatedValueType)
                    value = DataValueClasses.newDirectReference(vClass);
                else
                    try {
                        value = vClass.newInstance();
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
            }
            return value;
        }

        void putEntry(Bytes keyBytes, V value, int hash2) {
            final int pos = nextFree();
            final long offset = entriesOffset + pos * entrySize;
            // clear any previous meta data.
            clearMetaData(offset);
            writeKey(keyBytes, offset + metaDataBytes);
            appendInstance(keyBytes, value);
            // add to index if successful.
            hashLookup.put(hash2, pos);
            incrementSize();
        }

        void directPutEntry(Bytes keyBytes, Bytes valueBytes, int hash2, K key, V value) {
            final int pos = nextFree();
            final long offset = entriesOffset + pos * entrySize;
            // clear any previous meta data.
            clearMetaData(offset);
            writeKey(keyBytes, offset + metaDataBytes);
            appendValue(valueBytes);
            // add to index if successful.
            hashLookup.put(hash2, pos);
            incrementSize();
            notifyPut(offset, false, key, value);
        }

        private void clearMetaData(long offset) {
            if (metaDataBytes > 0) {
                tmpBytes.storePositionAndSize(bytes, offset, metaDataBytes);
                tmpBytes.zeroOut();
            }
        }

        private void writeKey(Bytes keyBytes, long offset) {
            tmpBytes.storePositionAndSize(bytes, offset, entrySize - metaDataBytes);
            long keyLength = keyBytes.remaining();
            tmpBytes.writeStopBit(keyLength);
            tmpBytes.write(keyBytes);
        }

        int nextFree() {
            int ret = (int) freeList.setNextClearBit(nextSet);
            if (ret == DirectBitSet.NOT_FOUND) {
                ret = (int) freeList.setNextClearBit(0);
                if (ret == DirectBitSet.NOT_FOUND)
                    throw new IllegalStateException("Segment is full, no free entries found");
            }
            nextSet = ret + 1;
            return ret;
        }

        /**
         * Reads from {@link this.tmpBytes} an object at {@param offset}, will reuse {@param value} if possible, to reduce object creation.
         *
         * @param value  the object to reuse ( if possible ), if null a new object will be created an object and no reuse will occur.
         * @param offset the offset to read the data from
         */
        @SuppressWarnings("unchecked")
        V readObjectUsing(V value, final long offset) {
            if (generatedValueType)
                if (value == null)
                    value = DataValueClasses.newDirectReference(vClass);
                else
                    assert value instanceof Byteable;
            if (value instanceof Byteable) {
                ((Byteable) value).bytes(bytes, offset);
                return value;
            }
            return tmpBytes.readInstance(vClass, value);
        }

        boolean keyEquals(Bytes keyBytes, MultiStoreBytes tmpBytes) {
            // check the length is the same.
            long keyLength = tmpBytes.readStopBit();
            return keyLength == keyBytes.remaining()
                    && tmpBytes.startsWith(keyBytes);
        }

        /**
         * implementation for map.remove(Key,Value)
         *
         * @param keyBytes      the key of the entry to remove
         * @param expectedValue the entry will only be removed if the {@param existingValue} equals null or the {@param existingValue} equals that of the entry.value
         * @param hash2         a hash code relating to the {@keyBytes} ( not the natural hash of {@keyBytes}  )
         * @return if the entry corresponding to the {@param keyBytes} exists and removeReturnsNull==false, returns the value of the entry that was removed, otherwise null is returned
         */
        V remove(final DirectBytes keyBytes, final K key, final V expectedValue, int hash2) {
            lock();
            try {
                hash2 = hashLookup.startSearch(hash2);
                while (true) {

                    final int pos = hashLookup.nextPos();
                    if (pos < 0) {
                        return null;

                    } else {
                        final long offset = entriesOffset + pos * entrySize + metaDataBytes;
                        tmpBytes.storePositionAndSize(bytes, offset, entrySize - metaDataBytes);
                        if (!keyEquals(keyBytes, tmpBytes))
                            continue;
                        final long keyLength = keyBytes.remaining() + tmpBytes.position(); // includes the stop bit length.
                        tmpBytes.position(keyLength);
                        tmpBytes.readStopBit(); // read the length of the value.
                        tmpBytes.alignPositionAddr(4);
                        V valueRemoved = expectedValue == null && removeReturnsNull ? null : readObjectUsing(null, offset + keyLength);

                        if (expectedValue != null && !expectedValue.equals(valueRemoved))
                            return null;

                        hashLookup.remove(hash2, pos);
                        decrementSize();
                        notifyRemoved(offset - metaDataBytes, key, valueRemoved);

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


        void directRemove(final Bytes keyBytes, int hash2) {
            lock();
            try {
                hash2 = hashLookup.startSearch(hash2);
                while (true) {

                    final int pos = hashLookup.nextPos();
                    if (pos < 0) {
                        return;

                    } else {
                        final long offset = entriesOffset + pos * entrySize + metaDataBytes;
                        tmpBytes.storePositionAndSize(bytes, offset, entrySize - metaDataBytes);
                        if (!keyEquals(keyBytes, tmpBytes))
                            continue;
                        final long keyLength = align(keyBytes.remaining() + tmpBytes.position()); // includes the stop bit length.
                        tmpBytes.position(keyLength);

                        hashLookup.remove(hash2, pos);
                        decrementSize();

                        freeList.clear(pos);
                        if (pos < nextSet)
                            nextSet = pos;

                        return;
                    }
                }
            } finally {
                unlock();
            }
        }

        /**
         * implementation for map.containsKey(Key)
         *
         * @param keyBytes the key of the entry
         * @param hash2    a hash code relating to the {@keyBytes} ( not the natural hash of {@keyBytes}  )
         * @return true if and entry for this key exists
         */
        boolean containsKey(final DirectBytes keyBytes, final int hash2) {
            lock();
            try {

                hashLookup.startSearch(hash2);
                while (true) {

                    final int pos = hashLookup.nextPos();

                    if (pos < 0) {
                        return false;

                    } else {

                        final long offset = entriesOffset + pos * entrySize + metaDataBytes;
                        tmpBytes.storePositionAndSize(bytes, offset, entrySize - metaDataBytes);

                        if (!keyEquals(keyBytes, tmpBytes))
                            continue;

                        return true;

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
         * @param hash2         a hash code relating to the {keyBytes} ( not the natural hash of {keyBytes}  )
         * @return null if the value was not replaced, else the value that is replaced is returned
         */
        V replace(final DirectBytes keyBytes, final K key, final V expectedValue, final V newValue, final int hash2) {
            lock();
            try {

                hashLookup.startSearch(hash2);
                while (true) {

                    final int pos = hashLookup.nextPos();

                    if (pos < 0) {
                        return null;

                    } else {

                        final long offset = entriesOffset + pos * entrySize + metaDataBytes;
                        tmpBytes.storePositionAndSize(bytes, offset, entrySize - metaDataBytes);

                        if (!keyEquals(keyBytes, tmpBytes))
                            continue;

                        final long keyLength = keyBytes.remaining();
                        tmpBytes.skip(keyLength);
                        long valuePosition = tmpBytes.position();
                        tmpBytes.readStopBit();
                        final long alignPosition = align(tmpBytes.position());
                        tmpBytes.position(alignPosition);

                        final V valueRead = readObjectUsing(null, offset + keyLength);

                        if (valueRead == null)
                            return null;

                        if (expectedValue == null || expectedValue.equals(valueRead)) {
                            tmpBytes.position(valuePosition);
                            appendInstance(keyBytes, newValue);
                        }
                        notifyPut(offset, false, key, valueRead);
                        return valueRead;
                    }
                }
            } finally {
                unlock();
            }
        }


        /**
         * implementation for map.put(Key,Value)
         *
         * @param keyBytes
         * @param value
         * @param hash2            a hash code relating to the {@keyBytes} ( not the natural hash of {@keyBytes}  )
         * @param replaceIfPresent
         * @return
         */
        V put(final DirectBytes keyBytes, final K key, final V value, int hash2, boolean replaceIfPresent) {
            lock();
            try {
                hash2 = hashLookup.startSearch(hash2);
                while (true) {
                    final int pos = hashLookup.nextPos();
                    if (pos < 0) {
                        putEntry(keyBytes, value, hash2);
                        final long offset = entriesOffset + pos * entrySize;
                        notifyPut(offset, false, key, value);
                        return null;

                    } else {
                        final long offset = entriesOffset + pos * entrySize + metaDataBytes;
                        tmpBytes.storePositionAndSize(bytes, offset, entrySize - metaDataBytes);
                        if (!keyEquals(keyBytes, tmpBytes))
                            continue;
                        V v = addForPut(keyBytes, value, replaceIfPresent, offset);
                        notifyPut(offset - metaDataBytes, false, key, v);
                        return v;
                    }
                }
            } finally {
                unlock();
            }
        }

        private void notifyPut(long offset, boolean added, K key, V value) {
            if (eventListener != SharedMapEventListeners.NOP) {
                tmpBytes.storePositionAndSize(bytes, offset, entrySize);
                eventListener.onPut(VanillaSharedHashMap.this, tmpBytes, metaDataBytes, added, key, value);
            }
        }

        private void notifyGet(long offset, K key, V value) {
            if (eventListener != SharedMapEventListeners.NOP) {
                tmpBytes.storePositionAndSize(bytes, offset, entrySize);
                eventListener.onGetFound(VanillaSharedHashMap.this, tmpBytes, metaDataBytes, key, value);
            }
        }

        private V notifyMissed(DirectBytes keyBytes, K key, V usingValue, int hash2) {
            if (usingValue instanceof Byteable)
                ((Byteable) usingValue).bytes(null, 0);
            if (eventListener != SharedMapEventListeners.NOP) {
                V value2 = eventListener.onGetMissing(VanillaSharedHashMap.this, keyBytes, key, usingValue);
                if (value2 != null)
                    put(keyBytes, key, value2, hash2, false);
                return value2;
            }
            return null;
        }

        private void notifyRemoved(long offset, K key, V value) {
            if (eventListener != SharedMapEventListeners.NOP) {
                tmpBytes.storePositionAndSize(bytes, offset, entrySize);
                eventListener.onRemove(VanillaSharedHashMap.this, tmpBytes, metaDataBytes, key, value);
            }

        }

        private V addForPut(DirectBytes keyBytes, V value, boolean replaceIfPresent, long offset) {
            final long keyLength = keyBytes.remaining();
            tmpBytes.skip(keyLength);
            if (replaceIfPresent) {
                if (putReturnsNull) {
                    appendInstance(keyBytes, value);
                    return null;
                }
                long valuePosition = tmpBytes.position();
                tmpBytes.readStopBit();
                tmpBytes.alignPositionAddr(4);
                final V v = readObjectUsing(null, offset + tmpBytes.position());
                tmpBytes.position(valuePosition);
                appendInstance(keyBytes, value);

                return v;

            } else {
                if (putReturnsNull) {
                    return null;
                }

                tmpBytes.readStopBit();
                tmpBytes.alignPositionAddr(4);
                return readObjectUsing(null, offset + tmpBytes.position());
            }
        }

        void directPut(final Bytes keyBytes, final Bytes valueBytes, final int hash2, final K key, final V value) {
            lock();
            try {
                for (int pos = hashLookup.startSearch(hash2); ; pos = hashLookup.nextPos()) {
                    if (pos < 0) {
                        directPutEntry(keyBytes, valueBytes, hash2, key, value);

                        return;

                    } else {
                        final long offset = entriesOffset + pos * entrySize + metaDataBytes;
                        tmpBytes.storePositionAndSize(bytes, offset, entrySize - metaDataBytes);
                        if (!keyEquals(keyBytes, tmpBytes))
                            continue;
                        final long keyLength = keyBytes.remaining();
                        tmpBytes.skip(keyLength);
                        appendValue(valueBytes);
                        return;
                    }
                }
            } finally {
                unlock();
            }
        }

        void appendInstance(final Bytes bytes, final V value) {
            bytes.clear();
            if (generatedValueType)
                ((BytesMarshallable) value).writeMarshallable(bytes);
            else
                bytes.writeInstance(vClass, value);
            bytes.flip();
            appendValue(bytes);
        }

        void appendValue(final Bytes value) {
            if (value.remaining() + 4 > tmpBytes.remaining())
                throw new IllegalArgumentException("Value too large for entry was " + (value.remaining() + 4) + ", remaining: " + tmpBytes.remaining());
            tmpBytes.writeStopBit(value.remaining());
            tmpBytes.position(align(tmpBytes.position()));
            tmpBytes.write(value);
        }

        public void clear() {
            lock();
            try {
                hashLookup.clear();
                freeList.clear();
                resetSize();
            } finally {
                unlock();
            }

        }

        Entry<K, V> getNextEntry(K prevKey) {
            int pos;
            if (prevKey == null) {
                pos = hashLookup.firstPos();
            } else {
                int hash = hasher.segmentHash(hasher.hash(getKeyAsBytes(prevKey)));
                pos = hashLookup.nextKeyAfter(hash);
            }

            if (pos >= 0) {
                final long offset = entriesOffset + pos * entrySize + metaDataBytes;
                int length = entrySize - metaDataBytes;
                tmpBytes.storePositionAndSize(bytes, offset, length);
                tmpBytes.readStopBit();
                K key = tmpBytes.readInstance(kClass, null); //todo: readUsing?

                tmpBytes.readStopBit();
                final long valueOffset = align(tmpBytes.position()); // includes the stop bit length.
                tmpBytes.position(valueOffset);
                V value = readObjectUsing(null, offset + valueOffset); //todo: reusable container

                //notifyGet(offset - metaDataBytes, key, value); //todo: should we call this?

                return new SimpleEntry<K, V>(key, value);
            } else {
                return null;
            }
        }
    }

    final class EntryIterator implements Iterator<Entry<K, V>> {

        int segmentIndex = segments.length - 1;

        Entry<K, V> nextEntry, lastReturned;

        K lastSegmentKey;

        EntryIterator() {
            nextEntry = nextSegmentEntry();
        }

        public boolean hasNext() {
            return nextEntry != null;
        }

        public void remove() {
            if (lastReturned == null) throw new IllegalStateException();
            VanillaSharedHashMap.this.remove(lastReturned.getKey());
            lastReturned = null;
        }

        public Map.Entry<K, V> next() {
            Entry<K, V> e = nextEntry;
            if (e == null)
                throw new NoSuchElementException();
            lastReturned = e; // cannot assign until after null check
            nextEntry = nextSegmentEntry();
            return e;
        }

        Entry<K, V> nextSegmentEntry() {
            while (segmentIndex >= 0) {
                Segment segment = segments[segmentIndex];
                Entry<K, V> entry = segment.getNextEntry(lastSegmentKey);
                if (entry == null) {
                    segmentIndex--;
                    lastSegmentKey = null;
                } else {
                    lastSegmentKey = entry.getKey();
                    return entry;
                }
            }
            return null;
        }

    }

    final class EntrySet extends AbstractSet<Map.Entry<K, V>> {
        public Iterator<Map.Entry<K, V>> iterator() {
            return new EntryIterator();
        }

        public boolean contains(Object o) {
            if (!(o instanceof Map.Entry))
                return false;
            Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
            try {
                V v = VanillaSharedHashMap.this.get(e.getKey());
                return v != null && v.equals(e.getValue());
            } catch (ClassCastException ex) {
                return false;
            } catch (NullPointerException ex) {
                return false;
            }
        }

        public boolean remove(Object o) {
            if (!(o instanceof Map.Entry))
                return false;
            Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
            try {
                Object key = e.getKey();
                Object value = e.getValue();
                return VanillaSharedHashMap.this.remove(key, value);
            } catch (ClassCastException ex) {
                return false;
            } catch (NullPointerException ex) {
                return false;
            }
        }

        public int size() {
            return VanillaSharedHashMap.this.size();
        }

        public boolean isEmpty() {
            return VanillaSharedHashMap.this.isEmpty();
        }

        public void clear() {
            VanillaSharedHashMap.this.clear();
        }
    }
}
