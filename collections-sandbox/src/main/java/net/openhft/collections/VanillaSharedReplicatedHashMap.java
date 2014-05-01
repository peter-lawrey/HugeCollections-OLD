/*
 * Copyright 2014 Higher Frequency Trading
 * <p/>
 * http://www.higherfrequencytrading.com
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.collections;

import net.openhft.lang.io.AbstractBytes;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.MultiStoreBytes;
import net.openhft.lang.io.NativeBytes;
import net.openhft.lang.model.Byteable;
import net.openhft.lang.model.constraints.NotNull;
import net.openhft.lang.model.constraints.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;


public class VanillaSharedReplicatedHashMap<K, V> extends AbstractVanillaSharedHashMap<K, V>
        implements ReplicatedSharedHashMap<K, V> {
    private static final Logger LOGGER =
            Logger.getLogger(VanillaSharedReplicatedHashMap.class.getName());

    private final boolean canReplicate;
    private final TimeProvider timeProvider;
    private final byte localIdentifier;

    public VanillaSharedReplicatedHashMap(VanillaSharedReplicatedHashMapBuilder builder,
                                          File file,
                                          Class<K> kClass, Class<V> vClass) throws IOException {
        super(builder, kClass, vClass);
        this.canReplicate = builder.canReplicate();
        this.timeProvider = builder.timeProvider();
        this.localIdentifier = builder.identifier();
        createMappedStoreAndSegments(file);
    }

    @Override
    VanillaSharedHashMap<K, V>.Segment createSegment(NativeBytes bytes, int index) {
        return new Segment(bytes, index);
    }

    @Override
    public VanillaSharedReplicatedHashMapBuilder builder() {
        // TODO implement
        return null;
    }

    @Override
    int multiMapsPerSegment() {
        return canReplicate ? 2 : 1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V put(K key, V value) {
        return put0(key, value, true, localIdentifier, timeProvider.currentTimeMillis());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V put(K key, V value, byte identifier, long timeStamp) {
        return put0(key, value, true, identifier, timeStamp);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V putIfAbsent(K key, V value) {
        return put0(key, value, false, localIdentifier, timeProvider.currentTimeMillis());
    }

    private V put0(K key, V value, boolean replaceIfPresent,
                   final byte identifier, long timeStamp) {
        checkKey(key);
        checkValue(value);
        Bytes keyBytes = getKeyAsBytes(key);
        long hash = Hasher.hash(keyBytes);
        int segmentNum = hasher.getSegment(hash);
        int segmentHash = hasher.segmentHash(hash);
        return segment(segmentNum).put(keyBytes, key, value, segmentHash, replaceIfPresent,
                identifier, timeStamp);
    }

    private Segment segment(int segmentNum) {
        return (Segment) segments[segmentNum];
    }

    V lookupUsing(K key, V value, boolean create) {
        checkKey(key);
        Bytes keyBytes = getKeyAsBytes(key);
        long hash = Hasher.hash(keyBytes);
        int segmentNum = hasher.getSegment(hash);
        int segmentHash = hasher.segmentHash(hash);
        return segment(segmentNum).acquire(keyBytes, key, value, segmentHash, create,
                timeProvider.currentTimeMillis());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V remove(final Object key) {
        return removeIfValueIs(key, null, localIdentifier, timeProvider.currentTimeMillis());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V remove(K key, V value, byte identifier, long timeStamp) {
        return removeIfValueIs(key, null, identifier, timeStamp);
    }


    @Override
    public void onUpdate(AbstractBytes entry) {

        if (!canReplicate)
            throw new IllegalStateException("This method should not be called if canReplicate is FALSE");

        final long keyLen = entry.readStopBit();
        final Bytes keyBytes = entry.createSlice(0, keyLen);
        entry.skip(keyLen);

        final long timeStamp = entry.readLong();
        final byte identifier = entry.readByte();
        final boolean isDeleted = entry.readBoolean();

        long hash = Hasher.hash(keyBytes);
        int segmentNum = hasher.getSegment(hash);
        int segmentHash = hasher.segmentHash(hash);

        if (isDeleted)
            segment(segmentNum).remoteRemove(keyBytes, segmentHash, timeStamp, identifier);
        else {

            long valueLen = entry.readStopBit();
            final Bytes value = entry.createSlice(0, valueLen);
            segment(segmentNum).replicatingPut(keyBytes, value, segmentHash, identifier, timeStamp, valueLen, this.entrySize);
        }

    }


    @Override
    public byte getIdentifier() {
        return localIdentifier;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean remove(final Object key, final Object value) {
        if (value == null)
            return false; // CHM compatibility; I would throw NPE
        return removeIfValueIs(key, (V) value,
                localIdentifier, timeProvider.currentTimeMillis()) != null;
    }


    /**
     * {@inheritDoc}
     */
    private V removeIfValueIs(final Object key, final V expectedValue,
                              final byte identifier, final long timestamp) {
        checkKey(key);
        Bytes keyBytes = getKeyAsBytes((K) key);
        long hash = Hasher.hash(keyBytes);
        int segmentNum = hasher.getSegment(hash);
        int segmentHash = hasher.segmentHash(hash);
        return segment(segmentNum).remove(keyBytes, (K) key, expectedValue, segmentHash,
                timestamp, identifier);
    }

    /**
     * {@inheritDoc}
     */
    V replaceIfValueIs(@NotNull final K key, final V existingValue, final V newValue) {
        checkKey(key);
        checkValue(newValue);
        Bytes keyBytes = getKeyAsBytes(key);
        long hash = Hasher.hash(keyBytes);
        int segmentNum = hasher.getSegment(hash);
        int segmentHash = hasher.segmentHash(hash);
        return segment(segmentNum).replace(keyBytes, key, existingValue, newValue, segmentHash,
                timeProvider.currentTimeMillis());
    }


    class Segment extends VanillaSharedHashMap<K, V>.Segment implements SharedSegment {
        private IntIntMultiMap hashLookupLiveAndDeleted;
        private IntIntMultiMap hashLookupLiveOnly;

        /**
         * {@inheritDoc}
         */
        Segment(NativeBytes bytes, int index) {
            super(bytes, index);
        }

        @Override
        void createHashLookups(long start) {
            final NativeBytes iimmapBytes =
                    new NativeBytes(null, start, start + sizeOfMultiMap(), null);
            iimmapBytes.load();
            hashLookupLiveAndDeleted = hashMask == ~0 ?
                    new VanillaIntIntMultiMap(iimmapBytes) :
                    new VanillaShortShortMultiMap(iimmapBytes);
            start += sizeOfMultiMap();

            if (canReplicate) {
                final NativeBytes iimmapBytes2 =
                        new NativeBytes(null, start, start + sizeOfMultiMap(), null);
                iimmapBytes2.load();
                hashLookupLiveOnly = hashMask == ~0 ?
                        new VanillaIntIntMultiMap(iimmapBytes2) :
                        new VanillaShortShortMultiMap(iimmapBytes2);
            } else {
                hashLookupLiveOnly = hashLookupLiveAndDeleted;
            }
        }

        private long entrySize(long keyLen, long valueLen) {
            return alignment.alignAddr(metaDataBytes +
                    expectedStopBits(keyLen) + keyLen + (canReplicate ? 10 : 0) +
                    expectedStopBits(valueLen)) + valueLen;
        }

        /**
         * @see net.openhft.collections.VanillaSharedHashMap.Segment#acquire(net.openhft.lang.io.Bytes, Object, Object, int, boolean)
         */
        V acquire(Bytes keyBytes, K key, V usingValue, int hash2, boolean create, long timestamp) {
            lock();
            try {
                MultiStoreBytes entry = tmpBytes;
                long offset = searchKey(keyBytes, hash2, entry, hashLookupLiveOnly);
                if (offset >= 0) {
                    if (canReplicate && shouldTerminate(entry, timestamp))
                        return null;
                    // skip the is deleted flag
                    entry.skip(1);

                    return onKeyPresentOnAcquire(key, usingValue, offset, entry);
                } else {
                    usingValue = tryObtainUsingValueOnAcquire(keyBytes, key, usingValue, create);
                    if (usingValue != null) {
                        offset = putEntryConsideringByteableValue(keyBytes, hash2, usingValue);
                        incrementSize();
                        notifyPut(offset, true, key, usingValue, posFromOffset(offset));
                        return usingValue;
                    } else {
                        return null;
                    }
                }
            } finally {
                unlock();
            }
        }

        private long putEntryConsideringByteableValue(Bytes keyBytes, int hash2, V value) {
            return putEntry(keyBytes, hash2, value, true, localIdentifier,
                    timeProvider.currentTimeMillis(), hashLookupLiveOnly);
        }


        /**
         * called from a remote node as part of replication
         *
         * @param keyBytes
         * @param hash2
         * @param timestamp
         * @param identifier
         * @return
         */
        private V remoteRemove(Bytes keyBytes, int hash2,
                               final long timestamp, final byte identifier) {
            lock();
            try {
                long keyLen = keyBytes.remaining();
                hashLookupLiveOnly.startSearch(hash2);
                int pos;
                while ((pos = hashLookupLiveOnly.nextPos()) >= 0) {
                    long offset = offsetFromPos(pos);
                    NativeBytes entry = entry(offset);
                    if (!keyEquals(keyBytes, keyLen, entry))
                        continue;
                    // key is found
                    entry.skip(keyLen);

                    long timeStampPos = 0;
                    if (canReplicate) {
                        timeStampPos = entry.position();
                        if (shouldTerminate(entry, timestamp))
                            return null;
                        // skip the is deleted flag
                        entry.skip(1);
                    }

                    long valueLen = readValueLen(entry);
                    long entryEndAddr = entry.positionAddr() + valueLen;


                    hashLookupLiveOnly.removePrevPos();
                    decrementSize();

                    if (canReplicate) {
                        entry.position(timeStampPos);
                        entry.writeLong(timestamp);
                        entry.writeByte(identifier);
                        // was deleted
                        entry.writeBoolean(true);
                        // set the value len to zero
                        //entry.writeStopBit(0);
                    } else {
                        free(pos, inBlocks(entryEndAddr - entryStartAddr(offset)));
                    }

                }
                // key is not found
                return null;
            } finally {
                unlock();
            }
        }


        /**
         * called from a remote node as part of replication
         *
         * @param keyBytes
         * @param value
         * @param hash2
         * @param identifier
         * @param timestamp
         * @param valueLen
         * @param entrySize1
         * @return
         */
        private void replicatingPut(Bytes keyBytes, Bytes value, int hash2,
                                    final byte identifier, final long timestamp, final long valueLen, final long entrySize1) {
            lock();

            try {
                long keyLen = keyBytes.remaining();
                hashLookupLiveAndDeleted.startSearch(hash2);
                int pos;
                while ((pos = hashLookupLiveAndDeleted.nextPos()) >= 0) {
                    long offset = offsetFromPos(pos);
                    NativeBytes entry = entry(offset);
                    if (!keyEquals(keyBytes, keyLen, entry))
                        continue;
                    // key is found
                    entry.skip(keyLen);

                    boolean wasDeleted;

                    final long timeStampPos = entry.positionAddr();
                    if (shouldTerminate(entry, timestamp))
                        return;

                    wasDeleted = entry.readBoolean();
                    entry.positionAddr(timeStampPos);
                    entry.writeLong(timestamp);
                    entry.writeByte(identifier);
                    // deleted flag
                    entry.writeBoolean(false);


                    // replaceValueOnPut
                    {
                        long valueLenPos = entry.position();

                        long entryEndAddr = entry.positionAddr() + valueLen;


                        // putValue may relocate entry and change offset
                        putValue(pos, offset, entry, valueLenPos, entryEndAddr, value);

                    }


                    if (wasDeleted) {
                        // remove() would have got rid of this so we have to add it back in
                        hashLookupLiveOnly.put(hash2, pos);
                        incrementSize();

                    }
                    return;


                }

                // key is not found
                //   long offset = putEntry(keyBytes, hash2, value, identifier);

                int pos1 = alloc(inBlocks(entrySize1));
                long offset = offsetFromPos(pos1);
                clearMetaData(offset);
                NativeBytes entry = entry(offset);

                entry.writeStopBit(keyLen);
                entry.write(keyBytes);
                entry.writeLong(timestamp);
                entry.writeByte(identifier);
                entry.writeBoolean(false);

                entry.writeStopBit(valueLen);
                alignment.alignPositionAddr(entry);
                entry.write(value);

                hashLookupLiveAndDeleted.putAfterFailedSearch(pos);
                hashLookupLiveOnly.put(hash2, pos);

                incrementSize();

            } finally {
                unlock();
            }
        }

        V put(Bytes keyBytes, K key, V value, int hash2, boolean replaceIfPresent,
              final byte identifier, final long timestamp) {
            lock();
            try {
                long keyLen = keyBytes.remaining();
                hashLookupLiveAndDeleted.startSearch(hash2);
                int pos;
                while ((pos = hashLookupLiveAndDeleted.nextPos()) >= 0) {
                    long offset = offsetFromPos(pos);
                    NativeBytes entry = entry(offset);
                    if (!keyEquals(keyBytes, keyLen, entry))
                        continue;
                    // key is found
                    entry.skip(keyLen);
                    if (replaceIfPresent) {
                        boolean wasDeleted = false;
                        if (canReplicate) {
                            final long timeStampPos = entry.positionAddr();
                            if (shouldTerminate(entry, timestamp))
                                return null;

                            wasDeleted = entry.readBoolean();
                            entry.positionAddr(timeStampPos);
                            entry.writeLong(timestamp);
                            entry.writeByte(identifier);
                            // deleted flag
                            entry.writeBoolean(false);
                        }

                        final V result = replaceValueOnPut(key, value, entry, pos, offset);

                        if (wasDeleted) {
                            // remove() would have got rid of this so we have to add it back in
                            hashLookupLiveOnly.put(hash2, pos);
                            incrementSize();
                            return null;
                        } else {
                            return result;
                        }

                    } else {

                        if (!canReplicate)
                            return putReturnsNull ? null : readValue(entry, null);

                        if (shouldTerminate(entry, timestamp))
                            return null;

                        final boolean wasDeleted = entry.readBoolean();

                        if (wasDeleted) {
                            // remove would have got rid of this so we have to add it back in
                            hashLookupLiveOnly.put(hash2, pos);
                            incrementSize();
                            return null;
                        }

                        return putReturnsNull ? null : readValue(entry, null);

                    }
                }
                // key is not found
                long offset = putEntry(keyBytes, hash2, value, identifier);
                incrementSize();
                notifyPut(offset, true, key, value, posFromOffset(offset));
                return null;
            } finally

            {
                unlock();
            }
        }

        /**
         * used only with replication, it sometimes possible to receive and old ( or stale update ) from a remote system
         * The method is used to determine if we should ignore such updates.
         * <p/>
         * we sometime will reject put() and removes()
         * when comparing times stamps with remote systems
         *
         * @param entry
         * @param providedTimestamp
         * @return
         */
        private boolean shouldTerminate(NativeBytes entry, long providedTimestamp) {

            final long lastModifiedTimeStamp = entry.readLong();

            // if the readTimeStamp is newer then we'll reject this put()
            // or they are the same and have a larger id

            if (lastModifiedTimeStamp < providedTimestamp) {
                entry.skip(1); // skip the byte used for the identifier
                return false;
            }

            if (lastModifiedTimeStamp > providedTimestamp)
                return true;

            return entry.readByte() > localIdentifier;

        }

        private long putEntry(Bytes keyBytes, int hash2, V value, final int remoteIdentifier) {
            return putEntry(keyBytes, hash2, value, false, remoteIdentifier,
                    timeProvider.currentTimeMillis(), hashLookupLiveAndDeleted);
        }

        /**
         * @param keyBytes
         * @param hash2
         * @param value
         * @param considerByteableValue
         * @param identifier
         * @param timestamp             the timestamp the entry was put ( this could be later if if was a remote put )
         * @param searchedHashLookup
         * @return
         */
        private long putEntry(Bytes keyBytes, int hash2, V value, boolean considerByteableValue,
                              final int identifier, final long timestamp,
                              IntIntMultiMap searchedHashLookup) {
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

            if (canReplicate) {
                entry.writeLong(timestamp);
                entry.writeByte(identifier);
                entry.writeBoolean(false);
            }

            writeValueOnPutEntry(byteableValue, valueLen, valueBytes, valueAsByteable, entry);

            // we have to add it both to the live
            if (searchedHashLookup == hashLookupLiveAndDeleted) {
                hashLookupLiveAndDeleted.putAfterFailedSearch(pos);
                hashLookupLiveOnly.put(hash2, pos);
            } else {
                hashLookupLiveOnly.putAfterFailedSearch(pos);
                hashLookupLiveAndDeleted.put(hash2, pos);
            }

            return offset;
        }

        /**
         * @see net.openhft.collections.VanillaSharedHashMap.Segment#remove(net.openhft.lang.io.Bytes, Object, Object, int)
         */
        public V remove(Bytes keyBytes, K key, V expectedValue, int hash2,
                        final long timestamp, final byte identifier) {
            lock();
            try {
                long keyLen = keyBytes.remaining();
                hashLookupLiveOnly.startSearch(hash2);
                int pos;
                while ((pos = hashLookupLiveOnly.nextPos()) >= 0) {
                    long offset = offsetFromPos(pos);
                    NativeBytes entry = entry(offset);
                    if (!keyEquals(keyBytes, keyLen, entry))
                        continue;
                    // key is found
                    entry.skip(keyLen);

                    long timeStampPos = 0;
                    if (canReplicate) {
                        timeStampPos = entry.position();
                        if (shouldTerminate(entry, timestamp))
                            return null;
                        // skip the is deleted flag
                        entry.skip(1);
                    }

                    long valueLen = readValueLen(entry);
                    long entryEndAddr = entry.positionAddr() + valueLen;
                    V valueRemoved = expectedValue != null || !removeReturnsNull
                            ? readValue(entry, null, valueLen) : null;
                    if (expectedValue != null && !expectedValue.equals(valueRemoved)) {
                        return null;
                    }

                    hashLookupLiveOnly.removePrevPos();
                    decrementSize();

                    if (canReplicate) {
                        entry.position(timeStampPos);
                        entry.writeLong(timestamp);
                        entry.writeByte(identifier);
                        // was deleted
                        entry.writeBoolean(true);
                        // set the value len to zero
                        //entry.writeStopBit(0);
                    } else {
                        free(pos, inBlocks(entryEndAddr - entryStartAddr(offset)));
                    }

                    notifyRemoved(offset, key, valueRemoved, pos);
                    return valueRemoved;
                }
                // key is not found
                return null;
            } finally {
                unlock();
            }
        }

        @Override
        IntIntMultiMap containsKeyHashLookup() {
            return hashLookupLiveOnly;
        }

        /**
         * @see net.openhft.collections.VanillaSharedHashMap.Segment#remove(net.openhft.lang.io.Bytes, Object, Object, int)
         */

        public V replace(Bytes keyBytes, K key, V expectedValue, V newValue, int hash2,
                         long timestamp) {
            lock();
            try {
                long keyLen = keyBytes.remaining();
                hashLookupLiveOnly.startSearch(hash2);
                int pos;
                while ((pos = hashLookupLiveOnly.nextPos()) >= 0) {
                    long offset = offsetFromPos(pos);
                    NativeBytes entry = entry(offset);
                    if (!keyEquals(keyBytes, keyLen, entry))
                        continue;
                    // key is found
                    entry.skip(keyLen);
                    if (canReplicate && shouldTerminate(entry, timestamp)) {
                        return null;
                    }

                    // skip the is deleted flag
                    entry.skip(1);

                    return onKeyPresentOnReplace(key, expectedValue, newValue, pos, offset, entry);
                }
                // key is not found
                return null;
            } finally {
                unlock();
            }
        }

        @Override
        void replacePosInHashLookupOnRelocation(int prevPos, int pos) {
            hashLookupLiveAndDeleted.replacePrevPos(pos);
            int hash = hashLookupLiveAndDeleted.getSearchHash();
            hashLookupLiveOnly.replace(hash, prevPos, pos);
        }

        void clear() {
            lock();
            try {
                hashLookupLiveOnly.clear();
                resetSize();
            } finally {
                unlock();
            }
        }

        void visit(IntIntMultiMap.EntryConsumer entryConsumer) {
            hashLookupLiveOnly.forEach(entryConsumer);
        }

        /**
         * returns a null value if the entry has been deleted
         *
         * @param pos
         * @return a null value if the entry has been deleted
         */
        @Nullable
        public Entry<K, V> getEntry(int pos) {
            long offset = offsetFromPos(pos);
            NativeBytes entry = entry(offset);
            entry.readStopBit();
            K key = entry.readInstance(kClass, null); //todo: readUsing?
            if (canReplicate) {
                // skip timestamp and id
                entry.skip(10);
            }
            V value = readValue(entry, null); //todo: reusable container
            //notifyGet(offset - metaDataBytes, key, value); //todo: should we call this?
            return new WriteThroughEntry(key, value);
        }

    }
}

