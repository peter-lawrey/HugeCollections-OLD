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

import net.openhft.chronicle.sandbox.queue.locators.shared.remote.ByteUtils;
import net.openhft.lang.Maths;
import net.openhft.lang.collection.ATSDirectBitSet;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.DirectBytes;
import net.openhft.lang.io.MultiStoreBytes;
import net.openhft.lang.io.NativeBytes;
import net.openhft.lang.model.Byteable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Set;
import java.util.logging.Logger;

import static net.openhft.collections.ReplicatedSharedHashMap.EventType.PUT;
import static net.openhft.collections.ReplicatedSharedHashMap.EventType.REMOVE;
import static net.openhft.lang.collection.DirectBitSet.NOT_FOUND;

/**
 * A Replicating Multi Master HashMap
 * <p/>
 * Each remote hash-map, mirrors its changes over to another remote hash map, neither hash map is considered the master
 * store of data, each hash map uses timestamps to reconcile changes.
 * We refer to in instance of a remote hash-map as a node. A node will be connected to any number of other nodes, for
 * the first implementation the maximum number of nodes will be fixed.
 * The data that is stored locally in each node will become eventually consistent. So changes made to one node,
 * for example by calling put() will be replicated over to the other node.
 * To achieve a high level of performance and throughput, the call to put() won’t block, with concurrentHashMap,
 * It is typical to check the return code of some methods to obtain the old value for example remove().
 * Due to the loose coupling and lock free nature of this multi master implementation,  this return value will only be
 * the old value on the nodes local data store.
 * In other words the nodes are only concurrent locally. Its worth realising that another node performing exactly the
 * same operation may return a different value.
 * However reconciliation will ensure the maps themselves become eventually consistent.
 * <p/>
 * Reconciliation
 * <p/>
 * If two ( or more nodes ) were to receive a change to their maps for the same key but different values, say by a user
 * of the maps, calling the put(<key>,<value>). Then, initially each node will update its local store and each local
 * store will hold a different value, but the aim of multi master replication is to provide eventual consistency across
 * the nodes. So, with multi master when ever a node is changed it will notify the other nodes of its change. We will
 * refer to this notification as an event. The event will hold a timestamp indicating the time the change occurred,
 * it will also hold the state transition, in this case it was a put with a key and value.
 * Eventual consistency is achieved by looking at the timestamp from the remote node, if for a given key, the remote
 * nodes timestamp is newer than the local nodes timestamp, then the event from the remote node will be applied to the
 * local node, otherwise the event will be ignored.
 * <p/>
 * However there is an edge case that we have to concern ourselves with, If two nodes update their map at the same time
 * with different values, we have to deterministically resolve which update wins, because of eventual consistency both
 * nodes should end up locally holding the same data. Although it is rare two remote nodes could receive an update to
 * their maps at exactly the same time for the same key, we have to handle this edge case, its therefore important not
 * to rely on timestamps alone to reconcile the updates. Typically the update with the newest timestamp should win, but
 * in this example both timestamps are the same, and the decision made to one node should be identical to the decision
 * made to the other. We resolve this simple dilemma by using a node identifier, each node will have a unique identifier,
 * the update from the node with the smallest identifier wins.
 *
 * @param <K>
 * @param <V>
 */
public class VanillaSharedReplicatedHashMap<K, V> extends AbstractVanillaSharedHashMap<K, V>
        implements ReplicatedSharedHashMap<K, V>, ReplicatedSharedHashMap.EntryExternalizable {

    private static final Logger LOG =
            Logger.getLogger(VanillaSharedReplicatedHashMap.class.getName());

    private final boolean canReplicate;
    private final TimeProvider timeProvider;
    private final byte localIdentifier;
    private final int maxNumberOfExternalMaps;

    // todo allow for dynamic creation of modificationIterators
    private Object[] modificationIterators = new Object[127];

    public VanillaSharedReplicatedHashMap(@NotNull VanillaSharedReplicatedHashMapBuilder builder,
                                          @NotNull File file,
                                          @NotNull Class<K> kClass,
                                          @NotNull Class<V> vClass) throws IOException {
        super(builder, kClass, vClass);

        this.canReplicate = builder.canReplicate();
        this.timeProvider = builder.timeProvider();
        this.localIdentifier = builder.identifier();
        this.maxNumberOfExternalMaps = builder.externalIdentifiers() == null ? 0 : builder.externalIdentifiers().length;

        long offset = createMappedStoreAndSegments(file);
        if (canReplicate && builder.externalIdentifiers() != null) {

            // todo allow for dynamic creation of modificationIterators

            // this implantation is flaky, but it allows us to get something working for now
            for (byte externalIdentifier : builder.externalIdentifiers()) {

                final long length = modIterBitSetSizeInBytes();

                eventListener = new ModificationIterator(builder.notifier(),
                        builder.watchList(),
                        ms.bytes(offset, length), eventListener);
                offset += length;
                modificationIterators[externalIdentifier] = eventListener;
            }
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
        return canReplicate ? (modIterBitSetSizeInBytes() * maxNumberOfExternalMaps) : 0L;
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

    /**
     * @param key              key with which the specified value is associated
     * @param value            value expected to be associated with the specified key
     * @param replaceIfPresent set to false for putIfAbsent()
     * @param identifier       used to identify which replicating node made the change
     * @param timeStamp        the time that that change was made, this is used for replication
     * @return the value that was replaced
     */
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

    /**
     * @param segmentNum a unique index of the segment
     * @return the segment associated with the {@code segmentNum}
     */
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

    /**
     * todo HCOLL-79 allow for dynamic creation of modificationIterators
     * {@inheritDoc}
     */
    @Override
    public ReplicatedSharedHashMap.ModificationIterator getModificationIterator(byte identifier) {
        if (!canReplicate)
            throw new UnsupportedOperationException();

        final Object modificationIterator = modificationIterators[identifier];

        if (modificationIterator == null)
            throw new IllegalStateException("identifier=" + identifier + ", please include this byte in the builder.externalIdentifiers()");


        return (ReplicatedSharedHashMap.ModificationIterator) modificationIterator;
    }

    /**
     * {@inheritDoc}
     */
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
    V replaceIfValueIs(final K key, final V existingValue, final V newValue) {
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

                    if (shouldTerminate(entry, timestamp, identifier)) {

                        // skip the is deleted flag
                        entry.skip(1);
                        return;
                    }

                    //      System.out.println("Status=OK");
                    // skip the is deleted flag
                    boolean wasDeleted = entry.readBoolean();


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
                }
            } finally {
                unlock();
            }
        }


        /**
         * called from a remote node when it wishes to propagate a remove event
         *
         * @param hash2
         * @param identifier
         * @param timestamp
         * @param keyPosition
         * @param keyLimit
         * @return
         */
        private void replicatingPut(@NotNull final Bytes inBytes,
                                    int hash2,
                                    final byte identifier,
                                    final long timestamp,
                                    long valuePos,
                                    long valueLimit,
                                    long keyPosition,
                                    long keyLimit) {
            lock();
            try {

                final long keyLen = keyLimit - keyPosition;

                hashLookupLiveAndDeleted.startSearch(hash2);
                for (int pos; (pos = hashLookupLiveAndDeleted.nextPos()) >= 0; ) {

                    inBytes.limit(keyLimit);
                    inBytes.position(keyPosition);

                    long offset = offsetFromPos(pos);
                    NativeBytes entry = entry(offset);
                    if (!keyEquals(inBytes, keyLen, entry))
                        continue;
                    // key is found
                    entry.skip(keyLen);

                    final long timeStampPos = entry.positionAddr();

                    entry.positionAddr(timeStampPos);

                    if (shouldTerminate(entry, timestamp, identifier)) {
                        entry.positionAddr(timeStampPos);
                        return;
                    }

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

                    // write the value
                    inBytes.limit(valueLimit);
                    inBytes.position(valuePos);

                    putValue(pos, offset, entry, valueLenPos, entryEndAddr, inBytes);

                    if (wasDeleted) {
                        // remove() would have got rid of this so we have to add it back in
                        hashLookupLiveOnly.put(hash2, pos);
                        incrementSize();
                    }
                    return;
                }

                // key is not found
                long valueLen = valueLimit - valuePos;
                int pos = alloc(inBlocks(entrySize(keyLen, valueLen)));
                long offset = offsetFromPos(pos);
                clearMetaData(offset);
                NativeBytes entry = entry(offset);

                entry.writeStopBit(keyLen);

                // write the key
                inBytes.limit(keyLimit);
                inBytes.position(keyPosition);

                entry.write(inBytes);

                entry.writeLong(timestamp);
                entry.writeByte(identifier);
                entry.writeBoolean(false);

                entry.writeStopBit(valueLen);
                alignment.alignPositionAddr(entry);

                // write the value
                inBytes.limit(valueLimit);
                inBytes.position(valuePos);

                entry.write(inBytes);

                hashLookupLiveAndDeleted.putAfterFailedSearch(pos);
                hashLookupLiveOnly.put(hash2, pos);

                incrementSize();

            } finally {
                unlock();
            }
        }

        /**
         * todo doc
         *
         * @param keyBytes
         * @param key
         * @param value
         * @param hash2
         * @param replaceIfPresent
         * @param identifier
         * @param timestamp
         * @return
         */
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
                    final long timeStampPos = entry.positionAddr();
                    boolean wasDeleted = false;

                    if (canReplicate) {

                        if (shouldTerminate(entry, timestamp, identifier))
                            return null;
                        wasDeleted = entry.readBoolean();
                    }

                    // if wasDeleted==true then we even if replaceIfPresent==false we can treat it the same way as replaceIfPresent true
                    if (replaceIfPresent || wasDeleted) {

                        if (canReplicate) {

                            entry.positionAddr(timeStampPos);
                            entry.writeLong(timestamp);
                            entry.writeByte(identifier);
                            // deleted flag
                            entry.writeBoolean(false);
                        }

                        final V prevValue = replaceValueOnPut(key, value, entry, pos, offset);

                        if (wasDeleted) {
                            // remove() would have got rid of this so we have to add it back in
                            hashLookupLiveOnly.put(hash2, pos);
                            incrementSize();
                            return null;
                        } else {
                            return prevValue;
                        }

                    }

                    long valueLenPos = entry.positionAddr();

                    // this is the case where we wont replaceIfPresent and the entry is not deleted
                    final long valueLen = readValueLen(entry);
                    final V prevValue = readValue(entry, value, valueLen);

                    if (prevValue != null)
                        // as we already have a previous value then we will return this and do nothing.
                        return prevValue;

                    // so we don't have a previous value, lets add one.

                    if (canReplicate) {
                        entry.positionAddr(timeStampPos);
                        entry.writeLong(timestamp);
                        entry.writeByte(identifier);
                        // deleted flag
                        entry.writeBoolean(false);
                    }

                    long entryEndAddr = entry.positionAddr() + valueLen;
                    offset = putValue(pos, offset, entry, valueLenPos,
                            entryEndAddr, getValueAsBytes(value));
                    notifyPut(offset, true, key, value, posFromOffset(offset));

                    // putIfAbsent() when the entry is NOT absent, so we return null as the prevValue
                    return null;

                }

                // key is not found
                long offset = putEntry(keyBytes, hash2, value, identifier, timestamp);
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
         * @param providedTimestamp the time the entry was created or updated
         * @param identifier
         * @return true if the entry should not be processed
         */
        private boolean shouldTerminate(@NotNull final NativeBytes entry, final long providedTimestamp, final byte identifier) {

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

        /**
         * todo doc
         *
         * @param keyBytes
         * @param hash2
         * @param value
         * @param remoteIdentifier
         * @param timestamp
         * @return
         */
        private long putEntry(Bytes keyBytes, int hash2, V value, final int remoteIdentifier, long timestamp) {
            return putEntry(keyBytes, hash2, value, false, remoteIdentifier,
                    timestamp, hashLookupLiveAndDeleted);
        }

        /**
         * todo doc
         *
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
                            return null;
                        }
                        // skip the is deleted flag
                        final boolean wasDeleted = entry.readBoolean();
                        if (wasDeleted) {

                            // this caters for the case when the entry in not in our hashLookupLiveOnly
                            // map but maybe in our hashLookupLiveAndDeleted, so we have to send the deleted notification
                            entry.position(timeStampPos);
                            entry.writeLong(timestamp);
                            if (identifier <= 0)
                                throw new IllegalStateException("identifier=" + identifier);
                            entry.writeByte(identifier);

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
                        entry.writeBoolean(true);

                    } else {
                        free(pos, inBlocks(entryEndAddr - entryStartAddr(offset)));
                    }

                    notifyRemoved(offset, key, valueRemoved, pos);
                    return valueRemoved;
                }


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

        /**
         * removes all the entries
         */
        void clear() {
            lock();
            try {

                // we have to make sure that every calls notifies on remove, so that the replicators can pick it up, but there must be a quicker
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
     * {@inheritDoc}
     */
    public void writeExternalEntry(@NotNull NativeBytes entry, @NotNull Bytes destination) {

        final long limt = entry.limit();
        final long keyLen = entry.readStopBit();
        final long keyPosition = entry.position();
        entry.skip(keyLen);
        final long keyLimit = entry.position();
        final long timeStamp = entry.readLong();

        final byte identifier = entry.readByte();
        if (identifier != localIdentifier) {
            return;
        }

        final boolean isDeleted = entry.readBoolean();
        long valueLen = isDeleted ? 0 : entry.readStopBit();

        // set the limit on the entry to the length ( in bytes ) of our entry
        final long position = entry.position();

        destination.writeStopBit(keyLen);
        destination.writeStopBit(valueLen);
        destination.writeStopBit(timeStamp);

        // we store the isDeleted flag in the identifier ( when the identifier is negative is it is deleted )
        if (isDeleted)
            destination.writeByte(-identifier);
        else
            destination.writeByte(identifier);

        // write the key
        entry.limit(keyLimit);
        entry.position(keyPosition);
        destination.write(entry);

        if (isDeleted || valueLen == 0)
            return;

        // skipping the alignment, as alignment wont work when we send the data over the wire.
        entry.limit(limt);
        entry.position(position);

        alignment.alignPositionAddr(entry);

        // writes the value
        entry.limit(entry.position() + valueLen);
        destination.write(entry);
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void readExternalEntry(@NotNull Bytes source) {

        final long keyLen = source.readStopBit();
        final long valueLen = source.readStopBit();
        final long timeStamp = source.readStopBit();
        final byte id = source.readByte();

        final byte remoteIdentifier;
        final boolean isDeleted;

        if (id < 0) {
            isDeleted = true;
            remoteIdentifier = (byte) -id;
        } else {
            isDeleted = false;
            remoteIdentifier = id;
        }


        final long keyPosition = source.position();
        final long keyLimit = source.position() + keyLen;

        source.limit(keyLimit);

        long hash = Hasher.hash(source);

        int segmentNum = hasher.getSegment(hash);
        int segmentHash = hasher.segmentHash(hash);

        if (isDeleted) {

            System.out.println("reading data into local=" + localIdentifier + ", remote=" + remoteIdentifier + ", remove(key=" + ByteUtils.toCharSequence(source).trim() + ")");

            segment(segmentNum).remoteRemove(source, segmentHash, timeStamp, remoteIdentifier);
            return;
        }

        // todo change to debug log
        final String message = "reading data into local=" + localIdentifier + ", remote=" + remoteIdentifier + ", put(key=" + ByteUtils.toCharSequence(source).trim();


        final long valuePosition = keyLimit;
        final long valueLimit = valuePosition + valueLen;
        segment(segmentNum).replicatingPut(source, segmentHash, remoteIdentifier, timeStamp, valuePosition, valueLimit, keyPosition, keyLimit);

        // todo change to debug log
        source.position(valuePosition);
        source.limit(valueLimit);
        System.out.println(message + "value=" + ByteUtils.toCharSequence(source).trim() + ")");
    }

    /**
     * Once a change occurs to a map, map replication requires
     * that these changes are picked up by another thread,
     * this class provides an iterator like interface to poll for such changes.
     * <p/>
     * In most cases the thread that adds data to the node is unlikely to be the same thread
     * that replicates the data over to the other nodes,
     * so data will have to be marshaled between the main thread storing data to the map,
     * and the thread running the replication.
     * <p/>
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
        @NotNull

        private final ATSDirectBitSet changes;
        private final int segmentIndexShift;
        private final long posMask;
        private final EnumSet<EventType> watchList;
        private final SharedMapEventListener<K, V, SharedHashMap<K, V>> nextListener;

        private volatile long position = -1;

        public ModificationIterator(@Nullable final Object notifier,
                                    @NotNull final Set<EventType> watchList,
                                    @NotNull final DirectBytes bytes,
                                    @NotNull final SharedMapEventListener<K, V, SharedHashMap<K, V>> nextListener) {
            this.notifier = notifier;
            this.watchList = EnumSet.copyOf(watchList);
            this.nextListener = nextListener;
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

            nextListener.onPut(map, entry, metaDataBytes, added, key, value, pos, segment);

            if (!watchList.contains(PUT))
                return;

            changes.set(combine(segment.getIndex(), pos));

            if (notifier != null) {
                synchronized (notifier) {
                    notifier.notifyAll();
                }
            }


        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onRemove(SharedHashMap<K, V> map, Bytes entry, int metaDataBytes,
                             K key, V value, int pos, SharedSegment segment) {
            assert VanillaSharedReplicatedHashMap.this == map :
                    "ModificationIterator.onRemove() is called from outside of the parent map";

            nextListener.onRemove(map, entry, metaDataBytes, key, value, pos, segment);

            if (!watchList.contains(REMOVE))
                return;
            changes.set(combine(segment.getIndex(), pos));

            if (notifier != null) {
                synchronized (notifier) {
                    notifier.notifyAll();
                }
            }


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
                final SharedSegment segment = segment((int) (position >>> segmentIndexShift));
                segment.lock();
                try {
                    if (changes.clearIfSet(position)) {

                        entryCallback.onBeforeEntry();

                        final long segmentPos = position & posMask;
                        final NativeBytes entry = segment.entry(segment.offsetFromPos(segmentPos));

                        // if the entry should be ignored, we'll move the next entry
                        final boolean success = entryCallback.onEntry(entry);
                        entryCallback.onAfterEntry();
                        if (success) {
                            return true;
                        }
                    }

                    // if the position was already cleared by another thread
                    // while we were trying to obtain segment lock (for example, in onReplication()),
                    // go to pick up next (next iteration in while (true) loop)
                } finally {
                    segment.unlock();
                }
            }
        }
    }

}


