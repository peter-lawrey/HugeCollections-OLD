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
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.AbstractMap;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.lang.Thread.currentThread;

public class VanillaSharedHashMap<K, V> extends AbstractMap<K, V> implements SharedHashMap<K, V>, DirectMap {
    private static final Logger LOGGER = Logger.getLogger(VanillaSharedHashMap.class.getName());
    private final ThreadLocal<DirectBytes> localBytes = new ThreadLocal<DirectBytes>();
    private final Class<K> kClass;
    private final Class<V> vClass;
    private final long lockTimeOutNS;
    private final int segmentBits;
    private Segment[] segments; // non-final for close()
    private MappedStore ms;     // non-final for close()

    private final int replicas;
    private final int entrySize;
    private final int entriesPerSegment;

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

        int segments = builder.actualSegments();
        this.segmentBits = Maths.intLog2(segments);
        int entriesPerSegment = builder.actualEntriesPerSegment();
        this.entriesPerSegment = entriesPerSegment;

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
                .transactional(false);

    }

    long sizeInBytes() {
        return SharedHashMapBuilder.HEADER_SIZE +
                segments.length * segmentSize();
    }

    long sizeOfMultiMap() {
        return align64(Maths.nextPower2(entriesPerSegment * 8L, 64));
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
    public V putIfAbsent(@NotNull K key, V value) {
        return put0(key, value, false);
    }

    private V put0(K key, V value, boolean replaceIfPresent) {
        if (!kClass.isInstance(key)) throw new IllegalArgumentException("Key must be a " + kClass.getName());
        DirectBytes bytes = getKeyAsBytes(key);
        long hash = longHashCode(bytes);
        int segmentNum = (int) (hash & (segments.length - 1));
        int hash2 = (int) (hash >>> segmentBits);
        return segments[segmentNum].put(bytes, value, hash2, replaceIfPresent);
    }

    @Override
    public void put(Bytes key, Bytes value) {
        long hash = longHashCode(key);
        int segmentNum = (int) (hash & (segments.length - 1));
        int hash2 = (int) (hash >>> segmentBits);
        segments[segmentNum].directPut(key, value, hash2);
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
        if (!kClass.isInstance(key)) return null;
        DirectBytes bytes = getKeyAsBytes(key);
        long hash = longHashCode(bytes);
        int segmentNum = (int) (hash & (segments.length - 1));
        int hash2 = (int) (hash >>> segmentBits);
        return segments[segmentNum].acquire(bytes, value, hash2, create);
    }


    private long longHashCode(Bytes bytes) {
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

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsKey(final Object key) {
        if (!kClass.isInstance(key))
            return false;

        final DirectBytes bytes = getKeyAsBytes((K) key);
        final long hash = longHashCode(bytes);
        final int segmentNum = (int) (hash & (segments.length - 1));
        int hash2 = (int) (hash >>> segmentBits);

        return segments[segmentNum].containsKey(bytes, hash2);
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
        throw new UnsupportedOperationException();
    }


    /**
     * {@inheritDoc}
     *
     * @throws NullPointerException if the specified key is null
     */
    @Override
    public V remove(@NotNull final Object key) {
        if (key == null)
            throw new NullPointerException("'key' can not be null");

        return removeIfValueIs(key, null);
    }

    /**
     * {@inheritDoc}
     *
     * @throws NullPointerException if the specified key is null
     */
    @Override
    public boolean remove(@NotNull final Object key, final Object value) {
        if (key == null)
            throw new NullPointerException("'key' can not be null");

        final V v = removeIfValueIs(key, (V) value);
        return v != null;
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

        if (!kClass.isInstance(key))
            return null;

        final DirectBytes bytes = getKeyAsBytes((K) key);
        final long hash = longHashCode(bytes);
        final int segmentNum = (int) (hash & (segments.length - 1));
        int hash2 = (int) (hash >>> segmentBits);
        return segments[segmentNum].remove(bytes, expectedValue, hash2);
    }

    @Override
    public void remove(Bytes key) {
        final DirectBytes bytes = getKeyAsBytes((K) key);
        final long hash = longHashCode(bytes);
        final int segmentNum = (int) (hash & (segments.length - 1));
        int hash2 = (int) (hash >>> segmentBits);
        segments[segmentNum].directRemove(bytes, hash2);
    }

    /**
     * {@inheritDoc}
     *
     * @throws NullPointerException if any of the arguments are null
     */
    @Override
    public boolean replace(@NotNull final K key, @NotNull final V oldValue, @NotNull final V newValue) {

        if (key == null)
            throw new NullPointerException("'key' can not be null");

        if (oldValue == null)
            throw new NullPointerException("'oldValue' can not be null");

        if (newValue == null)
            throw new NullPointerException("'newValue' can not be null");

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
    public V replace(@NotNull final K key, @NotNull final V value) {

        if (key == null)
            throw new NullPointerException("'key' can not be null");

        if (value == null)
            throw new NullPointerException("'value' can not be null");

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

        if (!kClass.isInstance(key))
            return null;

        final DirectBytes bytes = getKeyAsBytes(key);
        final long hash = longHashCode(bytes);
        final int segmentNum = (int) (hash & (segments.length - 1));
        int hash2 = (int) (hash >>> segmentBits);
        return segments[segmentNum].replace(bytes, existingValue, newValue, hash2);
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
        static final int HEADER_USED = REPLICA_OFFSET + 8;

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
            hashLookup = new VanillaIntIntMultiMap(iimmapBytes);
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
            this.bytes.addUnsignedInt(SIZE_OFFSET, 1);
        }

        public void resetSize() {
            this.bytes.writeUnsignedInt(SIZE_OFFSET, 0);
        }

        /**
         * decrements the size by one
         */
        private void decrementSize() {
            this.bytes.addUnsignedInt(SIZE_OFFSET, -1);
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
         * @param keyBytes the key of the entry
         * @param value    an object to be reused, null creates a new object.
         * @param hash2    a hash code relating to the {@keyBytes} ( not the natural hash of {@keyBytes}  )
         * @param create   false - if the  {@keyBytes} can not be found null will be returned, true - if the  {@keyBytes} can not be found an value will be acquired
         * @return an entry.value whose entry.key equals {@param keyBytes}
         */
        V acquire(DirectBytes keyBytes, V value, int hash2, boolean create) {
            lock();
            try {
                hash2 = hashLookup.startSearch(hash2);
                while (true) {
                    int pos = hashLookup.nextPos();
                    if (pos < 0) {
                        return create ? acquireEntry(keyBytes, value, hash2) : null;

                    } else {
                        final long offset = entriesOffset + pos * entrySize;
                        tmpBytes.storePositionAndSize(bytes, offset, entrySize);
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
                        return readObjectUsing(value, offset + valueOffset);
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
         * @param value
         * @param hash2    a hash code relating to the {@keyBytes} ( not the natural hash of {@keyBytes}  )
         * @return
         */

        V acquireEntry(DirectBytes keyBytes, V value, int hash2) {
            value = createValueIfNull(value);

            final int pos = nextFree();
            final long offset = entriesOffset + pos * entrySize;
            tmpBytes.storePositionAndSize(bytes, offset, entrySize);
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
            writeKey(keyBytes, offset);
            appendInstance(keyBytes, value);
            // add to index if successful.
            hashLookup.put(hash2, pos);
            incrementSize();
        }

        void directPutEntry(Bytes keyBytes, Bytes value, int hash2) {
            final int pos = nextFree();
            final long offset = entriesOffset + pos * entrySize;
            writeKey(keyBytes, offset);
            appendValue(value);
            // add to index if successful.
            hashLookup.put(hash2, pos);
            incrementSize();
        }

        private void writeKey(Bytes keyBytes, long offset) {
            tmpBytes.storePositionAndSize(bytes, offset, entrySize);
            long keyLength = keyBytes.remaining();
            tmpBytes.writeStopBit(keyLength);
            tmpBytes.write(keyBytes);
        }

        int nextFree() {
            int ret = (int) freeList.setNFrom(nextSet, 1);
            if (ret == DirectBitSet.NOT_FOUND) {
                ret = (int) freeList.setNFrom(0, 1);
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
        V remove(final DirectBytes keyBytes, final V expectedValue, int hash2) {
            lock();
            try {
                hash2 = hashLookup.startSearch(hash2);
                while (true) {

                    final int pos = hashLookup.nextPos();
                    if (pos < 0) {
                        return null;

                    } else {
                        final long offset = entriesOffset + pos * entrySize;
                        tmpBytes.storePositionAndSize(bytes, offset, entrySize);
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
                        final long offset = entriesOffset + pos * entrySize;
                        tmpBytes.storePositionAndSize(bytes, offset, entrySize);
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

                        final long offset = entriesOffset + pos * entrySize;
                        tmpBytes.storePositionAndSize(bytes, offset, entrySize);

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
         * @param hash2         a hash code relating to the {@keyBytes} ( not the natural hash of {@keyBytes}  )
         * @return null if the value was not replaced, else the value that is replaced is returned
         */
        V replace(final DirectBytes keyBytes, final V expectedValue, final V newValue, final int hash2) {
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
        V put(final DirectBytes keyBytes, final V value, int hash2, boolean replaceIfPresent) {
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
                        if (replaceIfPresent) {
                            if (putReturnsNull) {
                                appendInstance(keyBytes, value);
                                return null;
                            }
                            long valuePosition = tmpBytes.position();
                            tmpBytes.readStopBit();
                            final long alignPosition = align(tmpBytes.position());
                            tmpBytes.position(alignPosition);
                            final V v = readObjectUsing(null, offset + alignPosition);
                            tmpBytes.position(valuePosition);
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

        void directPut(final Bytes key, final Bytes value, int hash2) {
            lock();
            try {
                hash2 = hashLookup.startSearch(hash2);
                while (true) {
                    final int pos = hashLookup.nextPos();
                    if (pos < 0) {
                        directPutEntry(key, value, hash2);

                        return;

                    } else {
                        final long offset = entriesOffset + pos * entrySize;
                        tmpBytes.storePositionAndSize(bytes, offset, entrySize);
                        if (!keyEquals(key, tmpBytes))
                            continue;
                        final long keyLength = key.remaining();
                        tmpBytes.skip(keyLength);
                        appendValue(value);
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
    }
}
