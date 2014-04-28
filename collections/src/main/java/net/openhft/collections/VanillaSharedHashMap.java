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

public class VanillaSharedHashMap<K, V> extends AbstractMap<K, V> implements SharedHashMap<K, V>, SegmentInfoProvider {
    private static final Logger LOGGER = Logger.getLogger(VanillaSharedHashMap.class.getName());

    /**
     * @param size positive number
     * @return number of bytes taken by
     * {@link net.openhft.lang.io.AbstractBytes#writeStopBit(long)}
     * applied to {@code size}
     */
    private static int expectedStopBits(long size) {
        if (size <= 127)
            return 1;
        // numberOfLeadingZeros is cheap intrinsic on modern CPUs
        // integral division is not... but there is no choice
        return ((70 - Long.numberOfLeadingZeros(size)) / 7);
    }

    /**
     * Because DirectBitSet implementations couldn't find more
     * than 64 continuous clear or set bits.
     */
    private static final int MAX_ENTRY_OVERSIZE_FACTOR = 64;

    private static int figureBufferAllocationFactor(SharedHashMapBuilder builder) {
        // if expected map size is about 1000, seems rather wasteful to allocate
        // key and value serialization buffers each x64 of expected entry size..
        return (int) Math.min(Math.max(2L, builder.entries() >> 10),
                MAX_ENTRY_OVERSIZE_FACTOR);
    }

    private final int bufferAllocationFactor;

    private final ThreadLocal<DirectBytes> localBufferForKeys =
            new ThreadLocal<DirectBytes>();
    private final ThreadLocal<DirectBytes> localBufferForValues =
            new ThreadLocal<DirectBytes>();

    private final Class<K> kClass;
    private final Class<V> vClass;
    private final long lockTimeOutNS;
    private final int metaDataBytes;
    private Segment[] segments; // non-final for close()
    private MappedStore ms;     // non-final for close()
    private final Hasher hasher;

    private final int replicas;
    private final int entrySize;
    private final Alignment alignment;
    private final int entriesPerSegment;
    private final int hashMask;

    private final SharedMapErrorListener errorListener;
    private final SharedMapEventListener<K, V> eventListener;
    private final boolean generatedKeyType;
    private final boolean generatedValueType;


    // if set the ReturnsNull fields will cause some functions to return NULL
    // rather than as returning the Object can be expensive for something you probably don't use.
    private final boolean putReturnsNull;
    private final boolean removeReturnsNull;

    transient Set<Map.Entry<K, V>> entrySet;


    public VanillaSharedHashMap(SharedHashMapBuilder builder, File file,
                                Class<K> kClass, Class<V> vClass) throws IOException {
        bufferAllocationFactor = figureBufferAllocationFactor(builder);

        this.kClass = kClass;
        this.vClass = vClass;

        lockTimeOutNS = builder.lockTimeOutMS() * 1000000;

        this.replicas = builder.replicas();
        this.entrySize = builder.alignedEntrySize();
        this.alignment = builder.entryAndValueAlignment();

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
            this.segments[i] = new Segment(ms.createSlice(offset, segmentSize), i);
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
                .actualSegments(segments.length)
                .actualEntriesPerSegment(entriesPerSegment)
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

    public int getEntriesPerSegment() {
        return entriesPerSegment;
    }

    /**
     * Cache line alignment, assuming 64-byte cache lines.
     */
    private static long align64(long l) {
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

    public VanillaSharedHashMap.Segment[] getSegments() {
        return segments;
    }

    private DirectBytes acquireBufferForKey() {
        DirectBytes buffer = localBufferForKeys.get();
        if (buffer == null) {
            buffer = new DirectStore(ms.bytesMarshallerFactory(),
                    entrySize * bufferAllocationFactor, false).createSlice();
            localBufferForKeys.set(buffer);
        } else {
            buffer.clear();
        }
        return buffer;
    }

    private DirectBytes acquireBufferForValue() {
        DirectBytes buffer = localBufferForValues.get();
        if (buffer == null) {
            buffer = new DirectStore(ms.bytesMarshallerFactory(),
                    entrySize * bufferAllocationFactor, false).createSlice();
            localBufferForValues.set(buffer);
        } else {
            buffer.clear();
        }
        return buffer;
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
        Bytes keyBytes = getKeyAsBytes(key);
        long hash = Hasher.hash(keyBytes);
        int segmentNum = hasher.getSegment(hash);
        int segmentHash = hasher.segmentHash(hash);
        return segments[segmentNum].put(keyBytes, key, value, segmentHash, replaceIfPresent);
    }

    private DirectBytes getKeyAsBytes(K key) {
        DirectBytes buffer = acquireBufferForKey();
        if (generatedKeyType)
            ((BytesMarshallable) key).writeMarshallable(buffer);
        else
            buffer.writeInstance(kClass, key);
        buffer.flip();
        return buffer;
    }

    private DirectBytes getValueAsBytes(V value) {
        DirectBytes buffer = acquireBufferForValue();
        buffer.clear();
        if (generatedValueType)
            ((BytesMarshallable) value).writeMarshallable(buffer);
        else
            buffer.writeInstance(vClass, value);
        buffer.flip();
        return buffer;
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
        Bytes keyBytes = getKeyAsBytes(key);
        long hash = Hasher.hash(keyBytes);
        int segmentNum = hasher.getSegment(hash);
        int segmentHash = hasher.segmentHash(hash);
        return segments[segmentNum].acquire(keyBytes, key, value, segmentHash, create);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsKey(final Object key) {
        checkKey(key);
        Bytes keyBytes = getKeyAsBytes((K) key);
        long hash = Hasher.hash(keyBytes);
        int segmentNum = hasher.getSegment(hash);
        int segmentHash = hasher.segmentHash(hash);

        return segments[segmentNum].containsKey(keyBytes, segmentHash);
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
        Bytes keyBytes = getKeyAsBytes((K) key);
        long hash = Hasher.hash(keyBytes);
        int segmentNum = hasher.getSegment(hash);
        int segmentHash = hasher.segmentHash(hash);
        return segments[segmentNum].remove(keyBytes, (K) key, expectedValue, segmentHash);
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

    @Override
    public long longSize() {
        long result = 0;

        for (final Segment segment : this.segments) {
            result += segment.getSize();
        }

        return result;
    }

    @Override
    public int size() {
        long size = longSize();
        return size > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) size;
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
        Bytes keyBytes = getKeyAsBytes(key);
        long hash = Hasher.hash(keyBytes);
        int segmentNum = hasher.getSegment(hash);
        int segmentHash = hasher.segmentHash(hash);
        return segments[segmentNum].replace(keyBytes, key, existingValue, newValue, segmentHash);
    }

    /**
     * For testing
     */
    void checkConsistency() {
        for (Segment segment : segments) {
            segment.checkConsistency();
        }
    }


    private static final class Hasher {

        static long hash(Bytes bytes) {
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

        private final int segments;
        private final int bits;
        private final int mask;

        Hasher(int segments, int mask) {
            this.segments = segments;
            this.bits = Maths.intLog2(segments);
            this.mask = mask;
        }

        int segmentHash(long hash) {
            return (int) (hash >>> bits) & mask;
        }

        int getSegment(long hash) {
            return (int) (hash & (segments - 1));
        }
    }

    // these methods should be package local, not public or private.
    class Segment implements SharedSegment {
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
        private final int index;
        private final MultiStoreBytes tmpBytes = new MultiStoreBytes();
        private final IntIntMultiMap hashLookup;
        private final SingleThreadedDirectBitSet freeList;
        private int nextPosToSearchFrom = 0;
        private final long entriesOffset;


        /**
         * @param bytes
         * @param index the index of this segment held by the map
         */
        Segment(NativeBytes bytes, int index) {
            this.bytes = bytes;
            this.index = index;

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

        public int getIndex() {
            return index;
        }


        /* Methods with private access modifier considered private to Segment
         * class, although Java allows to access them from outer class anyway.
         */

        /**
         * increments the size by one
         */
        private void incrementSize() {
            this.bytes.addInt(SIZE_OFFSET, 1);
        }

        private void resetSize() {
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


        public void lock() throws IllegalStateException {
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

        public void unlock() {
            try {
                bytes.unlockLong(LOCK_OFFSET);
            } catch (IllegalMonitorStateException e) {
                errorListener.errorOnUnlock(e);
            }
        }

        long offsetFromPos(int pos) {
            return entriesOffset + pos * entrySize;
        }

        long posFromOffset(long offset) {
            return (offset - entriesOffset) / entrySize;
        }


        public MultiStoreBytes entry(long offset) {
            offset += metaDataBytes;
            tmpBytes.storePositionAndSize(bytes, offset,
                    // "Infinity". Limit not used when treating entries as
                    // possibly oversized
                    bytes.limit() - offset);
            return tmpBytes;
        }

        private long entryStartAddr(long offset) {
            // entry.address() points to "needed" start addr + metaDataBytes
            return bytes.startAddr() + offset;
        }

        private long entrySize(long keyLen, long valueLen) {
            return alignment.alignAddr(metaDataBytes +
                    expectedStopBits(keyLen) + keyLen +
                    expectedStopBits(valueLen)) + valueLen;
        }

        private int inBlocks(long sizeInBytes) {
            if (sizeInBytes <= entrySize)
                return 1;
            // int division is MUCH faster than long on Intel CPUs
            sizeInBytes -= 1;
            if (sizeInBytes <= Integer.MAX_VALUE)
                return (((int) sizeInBytes) / entrySize) + 1;
            return (int) (sizeInBytes / entrySize) + 1;
        }

        /**
         * Used to acquire an object of type V from the Segment.
         * <p/>
         * {@code usingValue} is reused to read the value if key is present
         * in this Segment, if key is absent in this Segment:
         * <p/>
         * <ol><li>If {@code create == false}, just {@code null} is returned
         * (except when event listener provides a value "on get missing" - then
         * it is put into this Segment for the key).</li>
         * <p/>
         * <li>If {@code create == true}, {@code usingValue} or a newly
         * created instance of value class, if {@code usingValue == null},
         * is put into this Segment for the key.</li></ol>
         *
         * @param keyBytes serialized {@code key}
         * @param hash2    a hash code related to the {@code keyBytes}
         * @return the value which is finally associated with the given key in
         * this Segment after execution of this method, or {@code null}.
         */
        V acquire(Bytes keyBytes, K key, V usingValue, int hash2, boolean create) {
            lock();
            try {
                long keyLen = keyBytes.remaining();
                hashLookup.startSearch(hash2);
                for (int pos; (pos = hashLookup.nextPos()) >= 0;) {
                    long offset = offsetFromPos(pos);
                    NativeBytes entry = entry(offset);
                    if (!keyEqualsForAcquire(keyBytes, keyLen, entry))
                        continue;
                    // key is found
                    entry.skip(keyLen);
                    V v = readValue(entry, usingValue);
                    notifyGet(offset, key, v);
                    return v;
                }
                // key is not found
                if (create) {
                    usingValue = createValueIfNull(usingValue);
                } else {
                    if (usingValue instanceof Byteable)
                        ((Byteable) usingValue).bytes(null, 0);
                    usingValue = notifyMissed(keyBytes, key, usingValue);
                    if (usingValue == null)
                        return null;
                }
                long offset =
                        putEntryConsideringByteableValue(keyBytes, usingValue);
                incrementSize();
                notifyPut(offset, true, key, usingValue, posFromOffset(offset));
                return usingValue;
            } finally {
                unlock();
            }
        }

        /**
         * Who needs this? Why only in acquire()?
         */
        private boolean keyEqualsForAcquire(Bytes keyBytes, long keyLen, Bytes entry) {
            if (!LOGGER.isLoggable(Level.FINE))
                return keyEquals(keyBytes, keyLen, entry);
            final long start0 = System.nanoTime();
            boolean result = keyEquals(keyBytes, keyLen, entry);
            final long time0 = System.nanoTime() - start0;
            if (time0 > 1e6) // 1 million nanoseconds = 1 millisecond
                LOGGER.fine("startsWith took " + time0 / 100000 / 10.0 + " ms.");
            return result;
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

        private long putEntryConsideringByteableValue(Bytes keyBytes, V value) {
            return putEntry(keyBytes, value, true);
        }

        V put(Bytes keyBytes, K key, V value, int hash2, boolean replaceIfPresent) {
            lock();
            try {
                long keyLen = keyBytes.remaining();
                hashLookup.startSearch(hash2);
                for (int pos; (pos = hashLookup.nextPos()) >= 0;) {
                    long offset = offsetFromPos(pos);
                    NativeBytes entry = entry(offset);
                    if (!keyEquals(keyBytes, keyLen, entry))
                        continue;
                    // key is found
                    entry.skip(keyLen);
                    if (replaceIfPresent) {
                        long valueLenPos = entry.position();
                        long valueLen = readValueLen(entry);
                        long entryEndAddr = entry.positionAddr() + valueLen;
                        V prevValue = null;
                        if (!putReturnsNull)
                            prevValue = readValue(entry, null, valueLen);

                        // putValue may relocate entry and change offset
                        offset = putValue(pos, offset, entry, valueLenPos,
                                entryEndAddr, getValueAsBytes(value));
                        notifyPut(offset, false, key, value,
                                posFromOffset(offset));
                        return prevValue;
                    } else {
                        return putReturnsNull ? null : readValue(entry, null);
                    }
                }
                // key is not found
                long offset = putEntry(keyBytes, value);
                incrementSize();
                notifyPut(offset, true, key, value, posFromOffset(offset));
                return null;
            } finally {
                unlock();
            }
        }

        private long putEntry(Bytes keyBytes, V value) {
            return putEntry(keyBytes, value, false);
        }

        private long putEntry(Bytes keyBytes, V value, boolean considerByteableValue) {
            long keyLen = keyBytes.remaining();

            // "if-else polymorphism" is not very beautiful, but allows to
            // reuse the rest code of this method and doesn't hurt performance.
            boolean byteableValue =
                    considerByteableValue && value instanceof Byteable;
            long valueLen;
            Bytes valueBytes = null;
            Byteable valueAsByteable = null;
            if (!byteableValue) {
                valueBytes = getValueAsBytes(value);
                valueLen = valueBytes.remaining();
            } else {
                valueAsByteable = (Byteable) value;
                valueLen = valueAsByteable.maxSize();
            }

            long entrySize = entrySize(keyLen, valueLen);
            int pos = alloc(inBlocks(entrySize));
            long offset = offsetFromPos(pos);
            clearMetaData(offset);
            NativeBytes entry = entry(offset);

            entry.writeStopBit(keyLen);
            entry.write(keyBytes);

            entry.writeStopBit(valueLen);
            alignment.alignPositionAddr(entry);

            if (!byteableValue) {
                entry.write(valueBytes);
            } else {
                long valueOffset = entry.positionAddr() - bytes.address();
                bytes.zeroOut(valueOffset, valueOffset + valueLen);
                valueAsByteable.bytes(bytes, valueOffset);
            }

            hashLookup.putAfterFailedSearch(pos);
            return offset;
        }

        private void clearMetaData(long offset) {
            if (metaDataBytes > 0)
                bytes.zeroOut(offset, offset + metaDataBytes);
        }

        private int alloc(int blocks) {
            int ret = (int) freeList.setNextNContinuousClearBits(nextPosToSearchFrom,
                    blocks);
            if (ret == DirectBitSet.NOT_FOUND) {
                ret = (int) freeList.setNextNContinuousClearBits(0, blocks);
                if (ret == DirectBitSet.NOT_FOUND) {
                    if (blocks == 1) {
                        throw new IllegalArgumentException(
                                "Segment is full, no free entries found");
                    } else {
                        throw new IllegalArgumentException(
                                "Segment is full or has no ranges of " + blocks
                                        + " continuous free blocks"
                        );
                    }
                }
            }
            // if bit at nextPosToSearchFrom is clear, it was skipped because
            // more than 1 block was requested. Don't move nextPosToSearchFrom
            // in this case. blocks == 1 clause is just a fast path.
            if (blocks == 1 || freeList.isSet(nextPosToSearchFrom))
                nextPosToSearchFrom = ret + blocks;
            return ret;
        }

        private boolean realloc(int fromPos, int oldBlocks, int newBlocks) {
            if (freeList.allClear(fromPos + oldBlocks, fromPos + newBlocks)) {
                freeList.set(fromPos + oldBlocks, fromPos + newBlocks);
                return true;
            } else {
                return false;
            }
        }

        private void free(int fromPos, int blocks) {
            freeList.clear(fromPos, fromPos + blocks);
            if (fromPos < nextPosToSearchFrom)
                nextPosToSearchFrom = fromPos;
        }

        private V readValue(NativeBytes entry, V value) {
            return readValue(entry, value, readValueLen(entry));
        }

        private long readValueLen(Bytes entry) {
            long valueLen = entry.readStopBit();
            alignment.alignPositionAddr(entry);
            return valueLen;
        }

        /**
         * TODO use the value length to limit reading
         *
         * @param value the object to reuse (if possible),
         *              if {@code null} a new object is created
         */
        private V readValue(NativeBytes entry, V value, long valueLen) {
            if (generatedValueType)
                if (value == null)
                    value = DataValueClasses.newDirectReference(vClass);
                else
                    assert value instanceof Byteable;
            if (value instanceof Byteable) {
                long valueOffset = entry.positionAddr() - bytes.address();
                ((Byteable) value).bytes(bytes, valueOffset);
                return value;
            }
            return entry.readInstance(vClass, value);
        }

        private boolean keyEquals(Bytes keyBytes, long keyLen, Bytes entry) {
            return keyLen == entry.readStopBit() && entry.startsWith(keyBytes);
        }

        /**
         * Removes a key (or key-value pair) from the Segment.
         * <p/>
         * The entry will only be removed if {@code expectedValue} equals
         * to {@code null} or the value previously corresponding to the specified key.
         *
         * @param keyBytes bytes of the key to remove
         * @param hash2    a hash code related to the {@code keyBytes}
         * @return the value of the entry that was removed if the entry
         * corresponding to the {@code keyBytes} exists
         * and {@link #removeReturnsNull} is {@code false},
         * {@code null} otherwise
         */
        V remove(Bytes keyBytes, K key, V expectedValue, int hash2) {
            lock();
            try {
                long keyLen = keyBytes.remaining();
                hashLookup.startSearch(hash2);
                for (int pos; (pos = hashLookup.nextPos()) >= 0;) {
                    long offset = offsetFromPos(pos);
                    NativeBytes entry = entry(offset);
                    if (!keyEquals(keyBytes, keyLen, entry))
                        continue;
                    // key is found
                    entry.skip(keyLen);
                    long valueLen = readValueLen(entry);
                    long entryEndAddr = entry.positionAddr() + valueLen;
                    V valueRemoved = expectedValue != null || !removeReturnsNull
                            ? readValue(entry, null, valueLen) : null;
                    if (expectedValue != null && !expectedValue.equals(valueRemoved))
                        return null;
                    hashLookup.removePrevPos();
                    decrementSize();
                    free(pos, inBlocks(entryEndAddr - entryStartAddr(offset)));
                    notifyRemoved(offset, key, valueRemoved, pos);
                    return valueRemoved;
                }
                // key is not found
                return null;
            } finally {
                unlock();
            }
        }

        boolean containsKey(Bytes keyBytes, int hash2) {
            lock();
            try {
                long keyLen = keyBytes.remaining();
                hashLookup.startSearch(hash2);
                for (int pos; (pos = hashLookup.nextPos()) >= 0;) {
                    Bytes entry = entry(offsetFromPos(pos));
                    if (keyEquals(keyBytes, keyLen, entry))
                        return true;
                }
                return false;
            } finally {
                unlock();
            }

        }

        /**
         * Replaces the specified value for the key with the given value.
         * <p/>
         * {@code newValue} is set only if the existing value corresponding
         * to the specified key is equal to {@code expectedValue}
         * or {@code expectedValue == null}.
         *
         * @param hash2 a hash code related to the {@code keyBytes}
         * @return the replaced value or {@code null} if the value was not replaced
         */
        V replace(Bytes keyBytes, K key, V expectedValue, V newValue, int hash2) {
            lock();
            try {
                long keyLen = keyBytes.remaining();
                hashLookup.startSearch(hash2);
                for (int pos; (pos = hashLookup.nextPos()) >= 0;) {
                    long offset = offsetFromPos(pos);
                    NativeBytes entry = entry(offset);
                    if (!keyEquals(keyBytes, keyLen, entry))
                        continue;
                    // key is found
                    entry.skip(keyLen);
                    long valueLenPos = entry.position();
                    long valueLen = readValueLen(entry);
                    long entryEndAddr = entry.positionAddr() + valueLen;
                    V valueRead = readValue(entry, null, valueLen);
                    if (valueRead == null)
                        return null;
                    if (expectedValue == null || expectedValue.equals(valueRead)) {
                        // putValue may relocate entry and change offset
                        offset = putValue(pos, offset, entry, valueLenPos,
                                entryEndAddr, getValueAsBytes(newValue));
                        notifyPut(offset, false, key, newValue,
                                posFromOffset(offset));
                        return valueRead;
                    }
                    return null;
                }
                // key is not found
                return null;
            } finally {
                unlock();
            }
        }


        private void notifyPut(long offset, boolean added, K key, V value, final long pos) {
            if (eventListener != SharedMapEventListeners.NOP) {
                tmpBytes.storePositionAndSize(bytes, offset, entrySize);
                eventListener.onPut(VanillaSharedHashMap.this, tmpBytes, metaDataBytes, added, key, value, pos, this);
            }
        }

        private void notifyGet(long offset, K key, V value) {
            if (eventListener != SharedMapEventListeners.NOP) {
                tmpBytes.storePositionAndSize(bytes, offset, entrySize);
                eventListener.onGetFound(VanillaSharedHashMap.this, tmpBytes, metaDataBytes, key, value);
            }
        }

        private V notifyMissed(Bytes keyBytes, K key, V usingValue) {
            if (eventListener != SharedMapEventListeners.NOP) {
                return eventListener.onGetMissing(VanillaSharedHashMap.this, keyBytes, key, usingValue);
            }
            return null;
        }

        private void notifyRemoved(long offset, K key, V value, final int pos) {
            if (eventListener != SharedMapEventListeners.NOP) {
                tmpBytes.storePositionAndSize(bytes, offset, entrySize);
                eventListener.onRemove(VanillaSharedHashMap.this, tmpBytes, metaDataBytes, key, value, pos, this);
            }
        }

        /**
         * Replaces value in existing entry. May cause entry relocation, because
         * there may be not enough space for new value in location already
         * allocated for this entry.
         *
         * @param pos          index of the first block occupied by the entry
         * @param offset       relative offset of the entry in Segment bytes
         *                     (before, i. e. including metaData)
         * @param entry        relative pointer in Segment bytes
         * @param valueLenPos  relative position of value "stop bit" in entry
         * @param entryEndAddr absolute address of the entry end
         * @param valueBytes   serialized value
         * @return relative offset of the entry in Segment bytes after putting value
         * (that may cause entry relocation)
         */
        private long putValue(int pos, long offset,
                              NativeBytes entry, long valueLenPos,
                              long entryEndAddr, Bytes valueBytes) {
            long valueLenAddr = entry.address() + valueLenPos;
            long newValueLen = valueBytes.remaining();
            long newValueAddr = alignment.alignAddr(
                    valueLenAddr + expectedStopBits(newValueLen));
            long newEntryEndAddr = newValueAddr + newValueLen;
            // Fast check before counting "sizes in blocks" that include
            // integral division
            newValueDoesNotFit:
            if (newEntryEndAddr != entryEndAddr) {
                long entryStartAddr = entryStartAddr(offset);
                long oldEntrySize = entryEndAddr - entryStartAddr;
                int oldSizeInBlocks = inBlocks(oldEntrySize);
                int newSizeInBlocks = inBlocks(newEntryEndAddr - entryStartAddr);
                if (newSizeInBlocks > oldSizeInBlocks) {
                    if (realloc(pos, oldSizeInBlocks, newSizeInBlocks))
                        break newValueDoesNotFit;
                    if (newSizeInBlocks > MAX_ENTRY_OVERSIZE_FACTOR) {
                        throw new IllegalArgumentException("Value too large: " +
                                "entry takes " + newSizeInBlocks + " blocks, " +
                                MAX_ENTRY_OVERSIZE_FACTOR + " is maximum.");
                    }
                    // RELOCATION
                    free(pos, oldSizeInBlocks);
                    pos = alloc(newSizeInBlocks);
                    // putValue() is called from put() and replace()
                    // after successful search by key
                    hashLookup.replacePrevPos(pos);
                    offset = offsetFromPos(pos);
                    // Moving metadata, key stop bit and key.
                    // Don't want to fiddle with pseudo-buffers for this,
                    // since we already have all absolute addresses.
                    long newEntryStartAddr = entryStartAddr(offset);
                    NativeBytes.UNSAFE.copyMemory(entryStartAddr,
                            newEntryStartAddr, valueLenAddr - entryStartAddr);
                    entry = entry(offset);
                    // END OF RELOCATION
                } else if (newSizeInBlocks < oldSizeInBlocks) {
                    // Freeing extra blocks
                    freeList.clear(pos + newSizeInBlocks, pos + oldSizeInBlocks);
                    // Do NOT reset nextPosToSearchFrom, because if value
                    // once was larger it could easily became oversized again,
                    // But if these blocks will be taken by that time,
                    // this entry will need to be relocated.
                }
            }
            // Common code for all cases
            entry.position(valueLenPos);
            entry.writeStopBit(newValueLen);
            alignment.alignPositionAddr(entry);
            entry.write(valueBytes);
            return offset;
        }

        void clear() {
            lock();
            try {
                hashLookup.clear();
                freeList.clear();
                resetSize();
            } finally {
                unlock();
            }

        }

        void visit(IntIntMultiMap.EntryConsumer entryConsumer) {
            hashLookup.forEach(entryConsumer);
        }

        public Entry<K, V> getEntry(int pos) {
            long offset = offsetFromPos(pos);
            NativeBytes entry = entry(offset);
            entry.readStopBit();
            K key = entry.readInstance(kClass, null); //todo: readUsing?

            V value = readValue(entry, null); //todo: reusable container

            //notifyGet(offset - metaDataBytes, key, value); //todo: should we call this?

            return new WriteThroughEntry(key, value);
        }

        /**
         * Check there is no garbage in freeList.
         */
        void checkConsistency() {
            lock();
            try {
                for (int pos = 0; (pos = (int) freeList.nextSetBit(pos)) >= 0;) {
                    PosPresentOnce check = new PosPresentOnce(pos);
                    hashLookup.forEach(check);
                    if (check.count != 1)
                        throw new AssertionError();
                    long offset = offsetFromPos(pos);
                    Bytes entry = entry(offset);
                    long keyLen = entry.readStopBit();
                    entry.skip(keyLen);
                    long valueLen = entry.readStopBit();
                    long sizeInBytes = entrySize(keyLen, valueLen);
                    int entrySizeInBlocks = inBlocks(sizeInBytes);
                    if (!freeList.allSet(pos, pos + entrySizeInBlocks))
                        throw new AssertionError();
                    pos += entrySizeInBlocks;
                }
            } finally {
                unlock();
            }
        }

        private class PosPresentOnce implements IntIntMultiMap.EntryConsumer {
            int pos, count = 0;

            PosPresentOnce(int pos) {
                this.pos = pos;
            }

            @Override
            public void accept(int hash, int pos) {
                if (this.pos == pos) count++;
            }
        }
    }

    final class EntryIterator implements Iterator<Entry<K, V>>, IntIntMultiMap.EntryConsumer {

        int segmentIndex = segments.length;

        Entry<K, V> nextEntry, lastReturned;

        Deque<Integer> segmentPositions = new ArrayDeque<Integer>(); //todo: replace with a more efficient, auto resizing int[]

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
                if (segmentPositions.isEmpty()) {
                    switchToNextSegment();
                } else {
                    Segment segment = segments[segmentIndex];
                    while (!segmentPositions.isEmpty()) {
                        Entry<K, V> entry = segment.getEntry(segmentPositions.removeFirst());
                        if (entry != null) {
                            return entry;
                        }
                    }
                }
            }
            return null;
        }

        private void switchToNextSegment() {
            segmentPositions.clear();
            segmentIndex--;
            if (segmentIndex >= 0) {
                segments[segmentIndex].visit(this);
            }
        }

        @Override
        public void accept(int key, int value) {
            segmentPositions.add(value);
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

    final class WriteThroughEntry extends SimpleEntry<K, V> {

        WriteThroughEntry(K key, V value) {
            super(key, value);
        }

        @Override
        public V setValue(V value) {
            put(getKey(), value);
            return super.setValue(value);
        }
    }
}
