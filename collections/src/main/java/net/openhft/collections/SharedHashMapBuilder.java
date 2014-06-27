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
import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.serialization.BytesMarshallableSerializer;
import net.openhft.lang.io.serialization.BytesMarshallerFactory;
import net.openhft.lang.io.serialization.JDKObjectSerializer;
import net.openhft.lang.io.serialization.ObjectSerializer;
import net.openhft.lang.io.serialization.impl.VanillaBytesMarshallerFactory;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetAddress;
import java.nio.ByteBuffer;

import static net.openhft.collections.Objects.equal;

public final class SharedHashMapBuilder implements Cloneable {

    static final Logger LOG = LoggerFactory.getLogger(TcpReplicator.class.getName());

    static final int SEGMENT_HEADER = 64;
    private static final byte[] MAGIC = "SharedHM".getBytes();

    // used when configuring the number of segments.
    private int minSegments = -1;
    private int actualSegments = -1;

    // used when reading the number of entries per
    private int actualEntriesPerSegment = -1;

    private int entrySize = 256;
    private Alignment alignment = Alignment.OF_4_BYTES;
    private long entries = 1 << 20;
    private int replicas = 0;
    private boolean transactional = false;
    private long lockTimeOutMS = 1000;
    private int metaDataBytes = 0;
    private SharedMapErrorListener errorListener = SharedMapErrorListeners.logging();
    private boolean putReturnsNull = false;
    private boolean removeReturnsNull = false;
    private boolean largeSegments = false;

    // replication
    private boolean canReplicate;
    private byte identifier = Byte.MIN_VALUE;
    TcpReplicatorBuilder tcpReplicatorBuilder;

    private TimeProvider timeProvider = TimeProvider.SYSTEM;
    UdpReplicatorBuilder udpReplicatorBuilder;
    private BytesMarshallerFactory bytesMarshallerFactory;
    private ObjectSerializer objectSerializer;

    @Override
    public SharedHashMapBuilder clone() {

        try {
            final SharedHashMapBuilder result = (SharedHashMapBuilder) super.clone();
            if (tcpReplicatorBuilder() != null)
                result.tcpReplicatorBuilder(tcpReplicatorBuilder().clone());
            if (udpReplicatorBuilder() != null)
                result.udpReplicatorBuilder(udpReplicatorBuilder().clone());
            return result;

        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }


    /**
     * Set minimum number of segments. See concurrencyLevel in {@link java.util.concurrent.ConcurrentHashMap}.
     *
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
     * Specifies alignment of address in memory of entries and independently of address in memory of values
     * within entries. <p/> <p>Useful when values of the map are updated intensively, particularly fields with
     * volatile access, because it doesn't work well if the value crosses cache lines. Also, on some (nowadays
     * rare) architectures any misaligned memory access is more expensive than aligned. <p/> <p>Note that
     * specified {@link #entrySize()} will be aligned according to this alignment. I. e. if you set {@code
     * entrySize(20)} and {@link net.openhft.collections.Alignment#OF_8_BYTES}, actual entry size will be 24
     * (20 aligned to 8 bytes).
     *
     * @return this {@code SharedHashMapBuilder} back
     * @see #entryAndValueAlignment()
     */
    public SharedHashMapBuilder entryAndValueAlignment(Alignment alignment) {
        this.alignment = alignment;
        return this;
    }

    /**
     * Returns alignment of addresses in memory of entries and independently of values within entries. <p/>
     * <p>Default is {@link net.openhft.collections.Alignment#OF_4_BYTES}.
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
                ", canReplicate=" + canReplicate() +
                ", identifier=" + identifierToString() +
                ", tcpReplicatorBuilder=" + tcpReplicatorBuilder() +
                ", udpReplicatorBuilder=" + udpReplicatorBuilder() +
                ", timeProvider=" + timeProvider() +
                ", bytesMarshallerfactory=" + bytesMarshallerFactory() +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SharedHashMapBuilder that = (SharedHashMapBuilder) o;

        if (actualEntriesPerSegment != that.actualEntriesPerSegment) return false;
        if (actualSegments != that.actualSegments) return false;
        if (canReplicate != that.canReplicate) return false;
        if (entries != that.entries) return false;
        if (entrySize != that.entrySize) return false;
        if (identifier != that.identifier) return false;
        if (largeSegments != that.largeSegments) return false;
        if (lockTimeOutMS != that.lockTimeOutMS) return false;
        if (metaDataBytes != that.metaDataBytes) return false;
        if (minSegments != that.minSegments) return false;
        if (putReturnsNull != that.putReturnsNull) return false;
        if (removeReturnsNull != that.removeReturnsNull) return false;
        if (replicas != that.replicas) return false;
        if (transactional != that.transactional) return false;

        if (alignment != that.alignment) return false;
        if (!equal(errorListener, that.errorListener))
            return false;
        if (!equal(tcpReplicatorBuilder, that.tcpReplicatorBuilder))
            return false;
        if (!equal(timeProvider, that.timeProvider))
            return false;
        if (!equal(udpReplicatorBuilder, that.udpReplicatorBuilder))
            return false;
        if (!equal(bytesMarshallerFactory, that.bytesMarshallerFactory))
            return false;
        return equal(objectSerializer, that.objectSerializer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(minSegments, actualSegments, actualEntriesPerSegment, entrySize,
                alignment, entries, replicas, transactional, lockTimeOutMS, metaDataBytes,
                errorListener, putReturnsNull, removeReturnsNull, largeSegments, canReplicate,
                identifier, tcpReplicatorBuilder, timeProvider, udpReplicatorBuilder,
                bytesMarshallerFactory, objectSerializer);
    }

    public boolean canReplicate() {
        return canReplicate || tcpReplicatorBuilder != null || udpReplicatorBuilder != null;
    }

    public SharedHashMapBuilder canReplicate(boolean canReplicate) {
        this.canReplicate = canReplicate;
        return this;
    }


    <K, V> void applyUdpReplication(VanillaSharedReplicatedHashMap<K, V> result,
                                    UdpReplicatorBuilder udpReplicatorBuilder) throws IOException {

        final InetAddress address = udpReplicatorBuilder.address();

        if (address == null) {
            throw new IllegalArgumentException("address can not be null");
        }

        if (address.isMulticastAddress() && udpReplicatorBuilder.networkInterface() == null) {
            throw new IllegalArgumentException("MISSING: NetworkInterface, " +
                    "When using a multicast addresses, please provided a " +
                    "networkInterface");
        }

        // the udp modification modification iterator will not be stored in shared memory
        final ByteBufferBytes updModIteratorBytes =
                new ByteBufferBytes(ByteBuffer.allocate((int) result.modIterBitSetSizeInBytes()));


        final UdpReplicator udpReplicator =
                new UdpReplicator(result, udpReplicatorBuilder.clone(), entrySize(), result.identifier());

        final VanillaSharedReplicatedHashMap.ModificationIterator udpModIterator =
                result.new ModificationIterator(
                        updModIteratorBytes,
                        result.eventListener,
                        udpReplicator);

        udpReplicator.setModificationIterator(udpModIterator);

        result.eventListener = udpModIterator;


        result.addCloseable(udpReplicator);
    }


    <K, V> void applyTcpReplication(@NotNull VanillaSharedReplicatedHashMap<K, V> result,
                                            @NotNull TcpReplicatorBuilder tcpReplicatorBuilder)
            throws IOException {
        result.addCloseable(new TcpReplicator(result,
                result,
                tcpReplicatorBuilder.clone(),
                entrySize()));
    }

    public SharedHashMapBuilder timeProvider(TimeProvider timeProvider) {
        this.timeProvider = timeProvider;
        return this;
    }

    public TimeProvider timeProvider() {
        return timeProvider;
    }

    public byte identifier() {

        if (identifier == Byte.MIN_VALUE)
            throw new IllegalStateException("identifier is not set.");

        return identifier;
    }

    private String identifierToString() {
        return identifier == Byte.MIN_VALUE ? "identifier is not set" : (identifier + "");
    }

    public SharedHashMapBuilder identifier(byte identifier) {
        this.identifier = identifier;
        return this;
    }

    public SharedHashMapBuilder tcpReplicatorBuilder(TcpReplicatorBuilder tcpReplicatorBuilder) {
        this.tcpReplicatorBuilder = tcpReplicatorBuilder;
        return this;
    }

    public TcpReplicatorBuilder tcpReplicatorBuilder() {
        return tcpReplicatorBuilder;
    }


    public UdpReplicatorBuilder udpReplicatorBuilder() {
        return udpReplicatorBuilder;
    }


    public SharedHashMapBuilder udpReplicatorBuilder(UdpReplicatorBuilder udpReplicatorBuilder) {
        this.udpReplicatorBuilder = udpReplicatorBuilder;
        return this;
    }

    public BytesMarshallerFactory bytesMarshallerFactory() {
        return bytesMarshallerFactory == null ? bytesMarshallerFactory = new VanillaBytesMarshallerFactory() : bytesMarshallerFactory;
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
}
