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
import net.openhft.lang.io.serialization.BytesMarshallableSerializer;
import net.openhft.lang.io.serialization.BytesMarshallerFactory;
import net.openhft.lang.io.serialization.JDKObjectSerializer;
import net.openhft.lang.io.serialization.ObjectSerializer;
import net.openhft.lang.io.serialization.impl.VanillaBytesMarshallerFactory;
import net.openhft.lang.model.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static net.openhft.collections.Objects.builderEquals;

public class SharedHashMapBuilder implements Cloneable {

    private static final Logger LOG = LoggerFactory.getLogger(SharedHashMapBuilder.class.getName());

    public static final short UDP_REPLICATION_MODIFICATION_ITERATOR_ID = 128;

    // used when configuring the number of segments.
    private int minSegments = -1;
    private int actualSegments = -1;

    // used when reading the number of entries per
    private int actualEntriesPerSegment = -1;

    private int entrySize = 256;
    private Alignment alignment = Alignment.OF_4_BYTES;
    private long entries = 1 << 20;
    private int replicas = 0;
    boolean transactional = false;
    private long lockTimeOutMS = 20000;
    private int metaDataBytes = 0;
    private SharedMapErrorListener errorListener = SharedMapErrorListeners.logging();
    private boolean putReturnsNull = false;
    private boolean removeReturnsNull = false;
    private boolean largeSegments = false;

    // replication
    private TimeProvider timeProvider = TimeProvider.SYSTEM;
    Replicator firstReplicator;
    Map<Class<? extends Replicator>, Replicator> replicators =
            new HashMap<Class<? extends Replicator>, Replicator>();

    private BytesMarshallerFactory bytesMarshallerFactory;
    private ObjectSerializer objectSerializer;

    boolean forceReplicatedImpl = false;


    public SharedHashMapBuilder() {
    }

    @Override
    public SharedHashMapBuilder clone() {

        try {
            @SuppressWarnings("unchecked")
            final SharedHashMapBuilder result = (SharedHashMapBuilder) super.clone();
            return result;

        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }


    /**
     * Set minimum number of segments. See concurrencyLevel
     * in {@link java.util.concurrent.ConcurrentHashMap}.
     *
     * @param minSegments the minimum number of segments in maps, constructed by this builder
     * @return this builder object back
     */
    public SharedHashMapBuilder minSegments(int minSegments) {
        this.minSegments = minSegments;
        return this;
    }

    public int minSegments() {
        return minSegments < 1 ? tryMinSegments(4, 65536) : minSegments;
    }

    private int tryMinSegments(int min, int max) {
        for (int i = min; i < max; i <<= 1) {
            if (i * i * i >= alignedEntrySize() * 2)
                return i;
        }
        return max;
    }

    /**
     * <p>Note that the actual entrySize will be aligned to 4 (default entry alignment). I. e. if you set
     * entry size to 30, the actual entry size will be 32 (30 aligned to 4 bytes). If you don't want entry
     * size to be aligned, set {@code entryAndValueAlignment(Alignment.NO_ALIGNMENT)}.
     *
     * @param entrySize the size in bytes
     * @return this {@code SharedHashMapBuilder} back
     * @see #entryAndValueAlignment(Alignment)
     * @see #entryAndValueAlignment()
     */
    public SharedHashMapBuilder entrySize(int entrySize) {
        this.entrySize = entrySize;
        return this;
    }

    public int entrySize() {
        return entrySize;
    }

    int alignedEntrySize() {
        return entryAndValueAlignment().alignSize(entrySize());
    }

    /**
     * Specifies alignment of address in memory of entries and independently of address in memory
     * of values within entries.
     *
     * <p>Useful when values of the map are updated intensively, particularly fields with
     * volatile access, because it doesn't work well if the value crosses cache lines. Also, on some
     * (nowadays rare) architectures any misaligned memory access is more expensive than aligned.
     *
     * <p>Note that specified {@link #entrySize()} will be aligned according to this alignment.
     * I. e. if you set {@code entrySize(20)} and {@link Alignment#OF_8_BYTES}, actual entry size
     * will be 24 (20 aligned to 8 bytes).
     *
     * @param alignment the new alignment of the maps constructed by this builder
     * @return this {@code SharedHashMapBuilder} back
     * @see #entryAndValueAlignment()
     */
    public SharedHashMapBuilder entryAndValueAlignment(Alignment alignment) {
        this.alignment = alignment;
        return this;
    }

    /**
     * Returns alignment of addresses in memory of entries and independently of values within
     * entries.
     *
     * <p>Default is {@link Alignment#OF_4_BYTES}.
     *
     * @see #entryAndValueAlignment(Alignment)
     */
    public Alignment entryAndValueAlignment() {
        return alignment;
    }

    public SharedHashMapBuilder entries(long entries) {
        this.entries = entries;
        return this;
    }

    public long entries() {
        return entries;
    }

    public SharedHashMapBuilder replicas(int replicas) {
        this.replicas = replicas;
        return this;
    }

    public int replicas() {
        return replicas;
    }

    public SharedHashMapBuilder actualEntriesPerSegment(int actualEntriesPerSegment) {
        this.actualEntriesPerSegment = actualEntriesPerSegment;
        return this;
    }

    public int actualEntriesPerSegment() {
        if (actualEntriesPerSegment > 0)
            return actualEntriesPerSegment;
        int as = actualSegments();
        // round up to the next multiple of 64.
        return (int) (Math.max(1, entries * 2L / as) + 63) & ~63;
    }

    public SharedHashMapBuilder actualSegments(int actualSegments) {
        this.actualSegments = actualSegments;
        return this;
    }

    public int actualSegments() {
        if (actualSegments > 0)
            return actualSegments;
        if (!largeSegments && entries > (long) minSegments() << 15) {
            long segments = Maths.nextPower2(entries >> 15, 128);
            if (segments < 1 << 20)
                return (int) segments;
        }
        // try to keep it 16-bit sizes segments
        return (int) Maths.nextPower2(Math.max((entries >> 30) + 1, minSegments()), 1);
    }

    /**
     * Not supported yet.
     *
     * @param transactional if the built map should be transactional
     * @return this {@code SharedHashMapBuilder} back
     */
    public SharedHashMapBuilder transactional(boolean transactional) {
        this.transactional = transactional;
        return this;
    }

    public boolean transactional() {
        return transactional;
    }

    public <K, V> SharedHashMap<K, V> create(File file, Class<K> kClass, Class<V> vClass)
            throws IOException {
        return new SharedHashMapKeyValueSpecificBuilder<K, V>(this.clone(), kClass, vClass)
                .create(file);
    }

    public <K, V> SharedHashMap<K, V> create(Class<K> kClass, Class<V> vClass)
            throws IOException {
        return new SharedHashMapKeyValueSpecificBuilder<K, V>(this.clone(), kClass, vClass)
                .create();
    }

    public <K, V> SharedHashMapKeyValueSpecificBuilder<K, V> toKeyValueSpecificBuilder(
            Class<K> kClass, Class<V> vClass) {
        return new SharedHashMapKeyValueSpecificBuilder<K, V>(this.clone(), kClass, vClass);
    }

    public SharedHashMapBuilder lockTimeOutMS(long lockTimeOutMS) {
        this.lockTimeOutMS = lockTimeOutMS;
        return this;
    }

    public long lockTimeOutMS() {
        return lockTimeOutMS;
    }

    public SharedHashMapBuilder errorListener(SharedMapErrorListener errorListener) {
        this.errorListener = errorListener;
        return this;
    }

    public SharedMapErrorListener errorListener() {
        return errorListener;
    }

    /**
     * Map.put() returns the previous value, functionality which is rarely used but fairly cheap for HashMap.
     * In the case, for an off heap collection, it has to create a new object (or return a recycled one)
     * Either way it's expensive for something you probably don't use.
     *
     * @param putReturnsNull false if you want SharedHashMap.put() to not return the object that was replaced
     *                       but instead return null
     * @return an instance of the map builder
     */
    public SharedHashMapBuilder putReturnsNull(boolean putReturnsNull) {
        this.putReturnsNull = putReturnsNull;
        return this;
    }

    /**
     * Map.put() returns the previous value, functionality which is rarely used but fairly cheap for HashMap.
     * In the case, for an off heap collection, it has to create a new object (or return a recycled one)
     * Either way it's expensive for something you probably don't use.
     *
     * @return true if SharedHashMap.put() is not going to return the object that was replaced but instead
     * return null
     */
    public boolean putReturnsNull() {
        return putReturnsNull;
    }

    /**
     * Map.remove()  returns the previous value, functionality which is rarely used but fairly cheap for
     * HashMap. In the case, for an off heap collection, it has to create a new object (or return a recycled
     * one) Either way it's expensive for something you probably don't use.
     *
     * @param removeReturnsNull false if you want SharedHashMap.remove() to not return the object that was
     *                          removed but instead return null
     * @return an instance of the map builder
     */
    public SharedHashMapBuilder removeReturnsNull(boolean removeReturnsNull) {
        this.removeReturnsNull = removeReturnsNull;
        return this;
    }


    /**
     * Map.remove() returns the previous value, functionality which is rarely used but fairly cheap for
     * HashMap. In the case, for an off heap collection, it has to create a new object (or return a recycled
     * one) Either way it's expensive for something you probably don't use.
     *
     * @return true if SharedHashMap.remove() is not going to return the object that was removed but instead
     * return null
     */
    public boolean removeReturnsNull() {
        return removeReturnsNull;
    }

    public boolean largeSegments() {
        return entries > 1L << (20 + 15) || largeSegments;
    }

    public SharedHashMapBuilder largeSegments(boolean largeSegments) {
        this.largeSegments = largeSegments;
        return this;
    }


    public SharedHashMapBuilder metaDataBytes(int metaDataBytes) {
        if ((metaDataBytes & 0xFF) != metaDataBytes)
            throw new IllegalArgumentException("MetaDataBytes must be [0..255] was " + metaDataBytes);
        this.metaDataBytes = metaDataBytes;
        return this;
    }

    public int metaDataBytes() {
        return metaDataBytes;
    }

    @Override
    public String toString() {
        return "SharedHashMapBuilder{" +
                "actualSegments=" + actualSegments() +
                ", minSegments=" + minSegments() +
                ", actualEntriesPerSegment=" + actualEntriesPerSegment() +
                ", entrySize=" + entrySize() +
                ", entryAndValueAlignment=" + entryAndValueAlignment() +
                ", entries=" + entries() +
                ", replicas=" + replicas() +
                ", transactional=" + transactional() +
                ", lockTimeOutMS=" + lockTimeOutMS() +
                ", metaDataBytes=" + metaDataBytes() +
                ", errorListener=" + errorListener() +
                ", putReturnsNull=" + putReturnsNull() +
                ", removeReturnsNull=" + removeReturnsNull() +
                ", largeSegments=" + largeSegments() +
                ", timeProvider=" + timeProvider() +
                ", bytesMarshallerfactory=" + bytesMarshallerFactory() +
                '}';
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(Object o) {
        return builderEquals(this, o);
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    public SharedHashMapBuilder addReplicator(Replicator replicator) {
        if (firstReplicator == null) {
            firstReplicator = replicator;
            replicators.put(replicator.getClass(), replicator);
        } else {
            if (replicator.identifier() != firstReplicator.identifier()) {
                throw new IllegalArgumentException(
                        "Identifiers of all replicators of the map should be the same");
            }
            if (replicators.containsKey(replicator.getClass())) {
                throw new IllegalArgumentException("Replicator of " + replicator.getClass() +
                        " class has already to the map");
            }
            replicators.put(replicator.getClass(), replicator);
        }
        return this;
    }

    public SharedHashMapBuilder timeProvider(TimeProvider timeProvider) {
        this.timeProvider = timeProvider;
        return this;
    }

    public TimeProvider timeProvider() {
        return timeProvider;
    }

    public BytesMarshallerFactory bytesMarshallerFactory() {
        return bytesMarshallerFactory == null ?
                bytesMarshallerFactory = new VanillaBytesMarshallerFactory() :
                bytesMarshallerFactory;
    }

    public SharedHashMapBuilder bytesMarshallerFactory(BytesMarshallerFactory bytesMarshallerFactory) {
        this.bytesMarshallerFactory = bytesMarshallerFactory;
        return this;
    }

    public ObjectSerializer objectSerializer() {
        return objectSerializer == null ?
                objectSerializer = BytesMarshallableSerializer.create(
                        bytesMarshallerFactory(), JDKObjectSerializer.INSTANCE) :
                objectSerializer;
    }

    public SharedHashMapBuilder objectSerializer(ObjectSerializer objectSerializer) {
        this.objectSerializer = objectSerializer;
        return this;
    }

    /**
     * For testing
     */
    SharedHashMapBuilder forceReplicatedImpl() {
        this.forceReplicatedImpl = true;
        return this;
    }
}

