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

import net.openhft.lang.Maths;
import net.openhft.lang.collection.ATSDirectBitSet;
import net.openhft.lang.io.*;
import net.openhft.lang.model.Byteable;
import net.openhft.lang.model.constraints.NotNull;
import net.openhft.lang.model.constraints.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Set;
import java.util.logging.Logger;

import static net.openhft.collections.ReplicatedSharedHashMap.EventType.PUT;
import static net.openhft.collections.ReplicatedSharedHashMap.EventType.REMOVE;
import static net.openhft.lang.collection.DirectBitSet.NOT_FOUND;


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

        long modIterBitSetOffset = createMappedStoreAndSegments(file);

        if (canReplicate) {
            eventListener =
                    new ModificationIterator(builder.notifier(),
                            builder.watchList(),
                            ms.createSlice(modIterBitSetOffset, modIterBitSetSizeInBytes())
                    );
        }
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
    long additionalSize() {
        return canReplicate ? modIterBitSetSizeInBytes() : 0L;
    }

    private long modIterBitSetSizeInBytes() {
        return align64(bitsPerSegmentInModIterBitSet() * segments.length / 8);
    }

    private long bitsPerSegmentInModIterBitSet() {
        // min 128 * 8 to prevent false sharing on updating bits from different segments
        return Maths.nextPower2((long) entriesPerSegment, 128 * 8);
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

        if (isDeleted) {

            segment(segmentNum).remoteRemove(keyBytes, segmentHash, timeStamp, identifier);
        } else {
            long valueLen = entry.readStopBit();
            final Bytes value = entry.createSlice(0, valueLen);

     /*       StringBuilder builder = new StringBuilder();
            for (int i = 0; i < valueLen; i++) {
                builder.append((char) entry.readByte());
            }*/
            //    System.out.println("new-value=" + builder);
            value.position(0);

            segment(segmentNum).replicatingPut(keyBytes, value, segmentHash, identifier, timeStamp);
        }

    }


    @Override
    public ReplicatedSharedHashMap.ModificationIterator getModificationIterator() {
        if (!canReplicate)
            throw new UnsupportedOperationException();
        return (ReplicatedSharedHashMap.ModificationIterator) eventListener;
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
        private volatile IntIntMultiMap hashLookupLiveAndDeleted;
        private volatile IntIntMultiMap hashLookupLiveOnly;

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
                    if (canReplicate) {
                        if (shouldTerminate(entry, timestamp, localIdentifier))
                            return null;
                        // skip the is deleted flag
                        entry.skip(1);
                    }
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
        private void remoteRemove(Bytes keyBytes, int hash2,
                                  final long timestamp, final byte identifier) {
            lock();
            try {


                long keyLen = keyBytes.remaining();
                hashLookupLiveAndDeleted.startSearch(hash2);
                for (int pos; (pos = hashLookupLiveAndDeleted.nextPos()) >= 0; ) {
                    long offset = offsetFromPos(pos);
                    NativeBytes entry = entry(offset);
                    if (!keyEquals(keyBytes, keyLen, entry))
                        continue;

                    // key is found
                    entry.skip(keyLen);

                    long timeStampPos = entry.position();

                    //    System.out.print("map" + localIdentifier + ".remoteRemove(time=" + timestamp + ",identifier=" + identifier);

                    if (shouldTerminate(entry, timestamp, identifier)) {

                        // skip the is deleted flag
                        entry.skip(1);

                        //          System.out.println("Status=Terminate, prev-value=" + readValue(entry, null) + ")");
                        return;
                    }

                    //      System.out.println("Status=OK");
                    // skip the is deleted flag
                    boolean wasDeleted = entry.readBoolean();

                    //    System.out.println("Status=OK, prev-value=" + readValue(entry, null));


                    if (!wasDeleted)
                        hashLookupLiveOnly.remove(hash2, pos);

                    decrementSize();

                    entry.position(timeStampPos);
                    entry.writeLong(timestamp);
                    if (identifier <= 0)
                        throw new IllegalStateException("identifier=" + identifier);
                    entry.writeByte(identifier);
                    // was deleted
                    entry.writeBoolean(true);

                    // set the value len to zero
                    //entry.writeStopBit(0);
                }
            } finally {
                unlock();
            }
        }


        /**
         * called from a remote node as part of replication
         *
         * @param keyBytes
         * @param valueBytes
         * @param hash2
         * @param identifier
         * @param timestamp
         * @return
         */
        private void replicatingPut(Bytes keyBytes, Bytes valueBytes, int hash2,
                                    final byte identifier, final long timestamp) {
            lock();
            try {
                long keyLen = keyBytes.remaining();
                hashLookupLiveAndDeleted.startSearch(hash2);
                for (int pos; (pos = hashLookupLiveAndDeleted.nextPos()) >= 0; ) {
                    long offset = offsetFromPos(pos);
                    NativeBytes entry = entry(offset);
                    if (!keyEquals(keyBytes, keyLen, entry))
                        continue;
                    // key is found
                    entry.skip(keyLen);

                    final long timeStampPos = entry.positionAddr();




                  /*  final boolean isInteresting = keyBytes.startsWith(getKeyAsBytes((K) (new Integer(1))));
                    if (isInteresting) {

                        System.out.print("map" + localIdentifier + ".replicatingPut(time=" + timestamp + ",identifier=" + identifier + ",new-value=" + newValue + ",keyBytes=" + keyBytes);
                        System.out.print(",existing-timestamp=" + entry.readLong());
                        System.out.print(",existing-identifier=" + entry.readByte());
                    }*/

                    entry.positionAddr(timeStampPos);

                    if (shouldTerminate(entry, timestamp, identifier)) {

                   /*     if (isInteresting)
                            System.out.println(",status=TERMINATED");
*/
                        entry.positionAddr(timeStampPos);


                        return;
                    }
                    /*if (isInteresting)
                        System.out.println(",Status=OK)");
*/
                    boolean wasDeleted = entry.readBoolean();
                    entry.positionAddr(timeStampPos);
                    entry.writeLong(timestamp);

                    if (identifier <= 0)
                        throw new IllegalStateException("identifier=" + identifier);

                    entry.writeByte(identifier);

                    // deleted flag
                    entry.writeBoolean(false);

                    long valueLenPos = entry.position();
                    long valueLen = readValueLen(entry);
                    long entryEndAddr = entry.positionAddr() + valueLen;
                    putValue(pos, offset, entry, valueLenPos, entryEndAddr, valueBytes);

                    if (wasDeleted) {
                        // remove() would have got rid of this so we have to add it back in
                        hashLookupLiveOnly.put(hash2, pos);
                        incrementSize();
                    }
                    return;
                }
                // key is not found
                long valueLen = valueBytes.remaining();
                int pos = alloc(inBlocks(entrySize(keyLen, valueLen)));
                long offset = offsetFromPos(pos);
                clearMetaData(offset);
                NativeBytes entry = entry(offset);

                entry.writeStopBit(keyLen);
                entry.write(keyBytes);

                entry.writeLong(timestamp);
                entry.writeByte(identifier);
                entry.writeBoolean(false);

                entry.writeStopBit(valueLen);
                alignment.alignPositionAddr(entry);
                entry.write(valueBytes);

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

                     /*
                            long pos1 = entry.positionAddr();

                            if (key == (V) (Integer) 1 || key == (V) (Integer) 2) {
                                System.out.print("map" + localIdentifier + ".put(time=" + timestamp + ",identifier=" + identifier + "new-value=" + value);
                                System.out.print(",existing-timestamp=" + entry.readLong());
                                System.out.print(",existing-identifier=" + entry.readByte());
                            }

                            entry.positionAddr(pos1);
                   */
                            if (shouldTerminate(entry, timestamp, identifier)) {
                          /*      if (key == (V) (Integer) 1 || key == (V) (Integer) 2)
                                    System.out.println(",status=TERMINATED");
                */
                                return null;
                            }
                       /*     else if (key == (V) (Integer) 1 || key == (V) (Integer) 2)
                                System.out.println(",status=OK");
*/
                            wasDeleted = entry.readBoolean();
                            entry.positionAddr(timeStampPos);
                            entry.writeLong(timestamp);
                            entry.writeByte(identifier);
                            // deleted flag
                            entry.writeBoolean(false);
                        }

                        final V result = replaceValueOnPut(key, value, entry, pos, offset);

                        notifyPut(offset, true, key, value, posFromOffset(offset));

                        if (wasDeleted) {
                            // remove() would have got rid of this so we have to add it back in
                            hashLookupLiveOnly.put(hash2, pos);
                            incrementSize();
                            return null;
                        } else {

                            return result;
                        }

                    } else {
                        if (canReplicate) {

                            if (identifier <= 0)
                                throw new IllegalStateException("identifier=" + identifier);

                            if (shouldTerminate(entry, timestamp, identifier))
                                return null;
                        /*
                            long pos1 = entry.positionAddr();

                           if (key == (V) (Integer) 1 || key == (V) (Integer) 2) {
                                System.out.print("map" + localIdentifier + ".put(time=" + timestamp + ",identifier=" + identifier + "new-value=" + value);
                                System.out.print(",existing-timestamp=" + entry.readLong());
                                System.out.print(",existing-identifier=" + entry.readByte());
                                System.out.println(",status=OK");
                            }

                            entry.positionAddr(pos1);
                    */
                            boolean wasDeleted = entry.readBoolean();

                            if (wasDeleted) {
                                // remove would have got rid of this so we have to add it back in
                                hashLookupLiveOnly.put(hash2, pos);
                                incrementSize();
                                notifyPut(offset, true, key, value, posFromOffset(offset));
                                return null;
                            }
                        }
                        notifyPut(offset, true, key, value, posFromOffset(offset));
                        return putReturnsNull ? null : readValue(entry, null);
                    }
                }
                // key is not found
                long offset = putEntry(keyBytes, hash2, value, identifier, timestamp);
                incrementSize();
                notifyPut(offset, true, key, value, posFromOffset(offset));
                return null;
            } finally {
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
         * @param providedTimestamp the time the entry was created or updated
         * @param identifier
         * @return
         */
        private boolean shouldTerminate(NativeBytes entry, long providedTimestamp, byte identifier) {

            final long lastModifiedTimeStamp = entry.readLong();

            // if the readTimeStamp is newer then we'll reject this put()
            // or they are the same and have a larger id
            if (lastModifiedTimeStamp < providedTimestamp) {
                entry.skip(1); // skip the byte used for the identifier
                return false;
            }

            if (lastModifiedTimeStamp > providedTimestamp)
                return true;

            // check the identifier
            return entry.readByte() > identifier;

        }


        private long putEntry(Bytes keyBytes, int hash2, V value, final int remoteIdentifier, long timestamp) {
            return putEntry(keyBytes, hash2, value, false, remoteIdentifier,
                    timestamp, hashLookupLiveAndDeleted);
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

                //     System.out.println("map2.remove(key=" + key + ",timestamp=" + timestamp + ")");

                long keyLen = keyBytes.remaining();

                final IntIntMultiMap multiMap = canReplicate ? hashLookupLiveAndDeleted : hashLookupLiveOnly;

                multiMap.startSearch(hash2);
                for (int pos; (pos = multiMap.nextPos()) >= 0; ) {
                    long offset = offsetFromPos(pos);
                    NativeBytes entry = entry(offset);
                    if (!keyEquals(keyBytes, keyLen, entry))
                        continue;
                    // key is found
                    entry.skip(keyLen);

                    long timeStampPos = 0;


                    if (canReplicate) {
                        timeStampPos = entry.position();
                        if (identifier <= 0)
                            throw new IllegalStateException("identifier=" + identifier);

                        if (shouldTerminate(entry, timestamp, identifier)) {
                            //            System.out.println("map" + identifier + ".skippedRemove(timestamp=" + timestamp + ")");
                            return null;
                        }
                        // skip the is deleted flag
                        final boolean wasDeleted = entry.readBoolean();
                        if (wasDeleted) {
                            // this caters for the case when the pos in not in our hashLookupLiveOnly
                            // map but maybe in a map, so we have to send the deleted notification


                            entry.position(timeStampPos);
                            entry.writeLong(timestamp);
                            if (identifier <= 0)
                                throw new IllegalStateException("identifier=" + identifier);
                            entry.writeByte(identifier);
                            //     System.out.println("map" + identifier + ".removing(" + key + ",timestamp=" + timestamp + ")");
                            // was deleted
                            entry.writeBoolean(true);


                            notifyRemoved(offset, key, null, pos);
                            return null;
                        }
                    }


                    long valueLen = readValueLen(entry);
                    long entryEndAddr = entry.positionAddr() + valueLen;
                    V valueRemoved = expectedValue != null || !removeReturnsNull
                            ? readValue(entry, null, valueLen) : null;

                    if (expectedValue != null && !expectedValue.equals(valueRemoved)) {
                        //     System.out.println("map" + identifier + ".skippedRemove-because-expected-value(timestamp=" + timestamp + ")");
                        return null;
                    }


                    hashLookupLiveOnly.remove(hash2, pos);
                    decrementSize();

                    if (canReplicate) {
                        entry.position(timeStampPos);
                        entry.writeLong(timestamp);
                        if (identifier <= 0)
                            throw new IllegalStateException("identifier=" + identifier);
                        entry.writeByte(identifier);
                        //    System.out.println("map" + identifier + ".removing(" + key + ",timestamp=" + timestamp + ")");
                        // was deleted
                        entry.writeBoolean(true);
                        // set the value len to zero
                        //entry.writeStopBit(0);
                    } else {
                        free(pos, inBlocks(entryEndAddr - entryStartAddr(offset)));
                    }
                    // System.out.println("notifying remove");
                    notifyRemoved(offset, key, valueRemoved, pos);
                    return valueRemoved;
                }
                //  System.out.println("remove key - not found!");
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
                for (int pos; (pos = hashLookupLiveOnly.nextPos()) >= 0; ) {
                    long offset = offsetFromPos(pos);
                    NativeBytes entry = entry(offset);
                    if (!keyEquals(keyBytes, keyLen, entry))
                        continue;
                    // key is found
                    entry.skip(keyLen);
                    if (canReplicate) {
                        if (shouldTerminate(entry, timestamp, localIdentifier))
                            return null;
                        // skip the is deleted flag
                        entry.skip(1);
                    }
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

                // todo improve how we do this, this its going to be slow,
                // we have to make sure that every calls notifys on remove, so that the replicators can pick it up, but there must be a quicker
                // way to do it than this.
                if (canReplicate) {
                    for (K k : keySet()) {
                        VanillaSharedReplicatedHashMap.this.remove(k);
                    }
                }

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
        public Entry<K, V> getEntry(long pos) {
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


    /**
     * Once a change occurs to a map, map replication requires
     * that these changes are picked up by another thread,
     * this class provides an iterator like interface to poll for such changes.
     *
     * In most cases the thread that adds data to the node is unlikely to be the same thread
     * that replicates the data over to the other nodes,
     * so data will have to be marshaled between the main thread storing data to the map,
     * and the thread running the replication.
     *
     * One way to perform this marshalling, would be to pipe the data into a queue. However,
     * This class takes another approach. It uses a bit set, and marks bits
     * which correspond to the indexes of the entries that have changed.
     * It then provides an iterator like interface to poll for such changes.
     *
     * @author Rob Austin.
     */
    class ModificationIterator extends SharedMapEventListener<K, V, SharedHashMap<K, V>>
            implements ReplicatedSharedHashMap.ModificationIterator {

        private final Object notifier;
        private final ATSDirectBitSet changes;
        private final int segmentIndexShift;
        private final long posMask;
        private final EnumSet<EventType> watchList;
        private final SharedMapEventListener<K, V, SharedHashMap<K, V>> nextListener;

        private volatile long position = -1;


        public ModificationIterator(@Nullable final Object notifier,
                                    Set<EventType> watchList, DirectBytes bytes) {
            this.notifier = notifier;
            this.watchList = EnumSet.copyOf(watchList);
            nextListener = eventListener;
            long bitsPerSegment = bitsPerSegmentInModIterBitSet();
            segmentIndexShift = Long.numberOfTrailingZeros(bitsPerSegment);
            posMask = bitsPerSegment - 1;
            changes = new ATSDirectBitSet(bytes);
        }

        private long combine(int segmentIndex, long pos) {
            return (((long) segmentIndex) << segmentIndexShift) | pos;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onPut(SharedHashMap<K, V> map, Bytes entry, int metaDataBytes,
                          boolean added, K key, V value, long pos, SharedSegment segment) {
            assert VanillaSharedReplicatedHashMap.this == map :
                    "ModificationIterator.onPut() is called from outside of the parent map";
            if (!watchList.contains(PUT))
                return;

            changes.set(combine(segment.getIndex(), pos));

            if (notifier != null) {
                synchronized (notifier) {
                    notifier.notifyAll();
                }
            }

            nextListener.onPut(map, entry, metaDataBytes, added, key, value, pos, segment);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onRemove(SharedHashMap<K, V> map, Bytes entry, int metaDataBytes,
                             K key, V value, int pos, SharedSegment segment) {
            assert VanillaSharedReplicatedHashMap.this == map :
                    "ModificationIterator.onRemove() is called from outside of the parent map";
            if (!watchList.contains(REMOVE))
                return;
            changes.set(combine(segment.getIndex(), pos));

            if (notifier != null) {
                synchronized (notifier) {
                    notifier.notifyAll();
                }
            }

            nextListener.onRemove(map, entry, metaDataBytes, key, value, pos, segment);
        }

        /**
         * Ensures that garbage in the old entry's location won't be broadcast as changed entry.
         */
        @Override
        void onRelocation(int pos, SharedSegment segment) {
            changes.clear(combine(segment.getIndex(), pos));
            // don't call nextListener.onRelocation(),
            // because no one event listener else overrides this method.
        }


        @Override
        public V onGetMissing(SharedHashMap<K, V> map, Bytes keyBytes, K key,
                              V usingValue) {
            return nextListener.onGetMissing(map, keyBytes, key, usingValue);
        }

        @Override
        public void onGetFound(SharedHashMap<K, V> map, Bytes entry, int metaDataBytes,
                               K key, V value) {
            nextListener.onGetFound(map, entry, metaDataBytes, key, value);
        }


        /**
         * you can continue to poll hasNext() until data becomes available.
         * If are are in the middle of processing an entry via {@code nextEntry}, hasNext will return true until the bit is cleared
         *
         * @return true if there is an entry
         */
        public boolean hasNext() {
            final long position = this.position;
            return changes.nextSetBit(position == -1 ? 0 : position) != NOT_FOUND ||
                    changes.nextSetBit(0) != NOT_FOUND;
        }


        /**
         * @param entryCallback call this to get an entry, this class will take care of the locking
         * @return true if an entry was processed
         */
        public boolean nextEntry(@NotNull final EntryCallback entryCallback) {
            long oldPosition = position;
            while (true) {

                long position = changes.nextSetBit(oldPosition + 1);

                if (position == NOT_FOUND) {
                    if (oldPosition == NOT_FOUND) {
                        this.position = -1;
                        return false;
                    }
                    oldPosition = position;
                    continue;
                }

                this.position = position;
                SharedSegment segment = segment((int) (position >>> segmentIndexShift));
                segment.lock();
                try {

                    entryCallback.onBeforeEntry();

                    if (changes.clearIfSet(position)) {

                        if (changes.get(position)) {
                            throw new IllegalStateException("position should be null.");
                        }

                        long segmentPos = position & posMask;
                        NativeBytes entry = segment.entry(segment.offsetFromPos(segmentPos));

                        // if the entry should be ignored, we'll move the next entry
                        if (entryCallback.onEntry(entry)) {
                            return true;
                        }
                    }


                    // if the position was already cleared by another thread
                    // while we were trying to obtain segment lock (for example, in onReplication()),
                    // go to pick up next
                } finally {
                    entryCallback.onAfterEntry();
                    segment.unlock();
                }
            }
        }
    }
}


