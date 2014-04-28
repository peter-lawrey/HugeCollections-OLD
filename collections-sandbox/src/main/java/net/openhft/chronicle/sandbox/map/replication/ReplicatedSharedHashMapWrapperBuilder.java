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

package net.openhft.chronicle.sandbox.map.replication;

import net.openhft.collections.*;

import java.io.File;
import java.io.IOException;

/**
 * @author Rob Austin.
 */
public class ReplicatedSharedHashMapWrapperBuilder extends SharedHashMapBuilder implements Cloneable {


    public static final int META_BYTES_SIZE = 16;

    public <K, V> ReplicatedShareHashMapWrapper<K, V> create(final File liveDataFile, final File metaDataFile, final Class<K> kClass, final Class<V> vClass, byte sequenceNumber) throws IOException {

        final SharedHashMapBuilder liveBuilder = clone();

        liveBuilder.entrySize(this.entrySize() + META_BYTES_SIZE);
        final SharedHashMap<K, MetaData<V>> live = liveBuilder.<K, MetaData<V>>create(liveDataFile, kClass, (Class) MetaData.class);

        liveBuilder.entrySize(META_BYTES_SIZE);
        final SharedHashMap<K, MetaData<V>> metaData = liveBuilder.<K, MetaData<V>>create(metaDataFile, kClass, (Class) MetaData.class);

        final MapModifier<K, V> mapModifier1 = new MapModifier<K, V>(sequenceNumber, live, metaData, vClass, generatedValueType());
        return new ReplicatedShareHashMapWrapper<K, V>(live, mapModifier1, vClass);

    }

    @Override
    public ReplicatedSharedHashMapWrapperBuilder clone() {
        return (ReplicatedSharedHashMapWrapperBuilder) super.clone();
    }

    /**
     * Set minimum number of segments.
     * See concurrencyLevel in {@link java.util.concurrent.ConcurrentHashMap}.
     *
     * @return this builder object back
     */
    public ReplicatedSharedHashMapWrapperBuilder minSegments(int minSegments) {
        super.minSegments(minSegments);
        return this;
    }


    /**
     * <p>Note that the actual entrySize will be aligned
     * to 4 (default entry alignment). I. e. if you set entry size to 30, the
     * actual entry size will be 32 (30 aligned to 4 bytes). If you don't want
     * entry size to be aligned, set
     * {@code entryAndValueAlignment(Alignment.NO_ALIGNMENT)}.
     *
     * @param entrySize the size in bytes
     * @return this {@code SharedReplicatedHashMapBuilder} back
     * @see #entryAndValueAlignment(Alignment)
     * @see #entryAndValueAlignment()
     */
    public ReplicatedSharedHashMapWrapperBuilder entrySize(int entrySize) {
        super.entrySize(entrySize);
        return this;
    }


    /**
     * Specifies alignment of address in memory of entries
     * and independently of address in memory of values within entries.
     * <p/>
     * <p>Useful when values of the map are updated intensively, particularly
     * fields with volatile access, because it doesn't work well
     * if the value crosses cache lines. Also, on some (nowadays rare)
     * architectures any misaligned memory access is more expensive than aligned.
     * <p/>
     * <p>Note that specified {@link #entrySize()} will be aligned according to
     * this alignment. I. e. if you set {@code entrySize(20)} and
     * {@link net.openhft.collections.Alignment#OF_8_BYTES}, actual entry size
     * will be 24 (20 aligned to 8 bytes).
     *
     * @return this {@code SharedReplicatedHashMapBuilder} back
     * @see #entryAndValueAlignment()
     */
    public ReplicatedSharedHashMapWrapperBuilder entryAndValueAlignment(Alignment alignment) {
        super.entryAndValueAlignment(alignment);
        return this;
    }


    public ReplicatedSharedHashMapWrapperBuilder entries(long entries) {
        super.entries(entries);
        return this;
    }


    public ReplicatedSharedHashMapWrapperBuilder replicas(int replicas) {
        super.replicas(replicas);
        return this;
    }


    public ReplicatedSharedHashMapWrapperBuilder actualEntriesPerSegment(int actualEntriesPerSegment) {
        super.actualEntriesPerSegment(actualEntriesPerSegment);
        return this;
    }


    public ReplicatedSharedHashMapWrapperBuilder actualSegments(int actualSegments) {
        super.actualSegments(actualSegments);
        return this;
    }

    /**
     * Not supported yet.
     *
     * @return an instance of the map builder
     */
    public ReplicatedSharedHashMapWrapperBuilder transactional(boolean transactional) {
        super.transactional(transactional);
        return this;
    }


    public ReplicatedSharedHashMapWrapperBuilder lockTimeOutMS(long lockTimeOutMS) {
        super.lockTimeOutMS(lockTimeOutMS);
        return this;
    }


    public ReplicatedSharedHashMapWrapperBuilder errorListener(SharedMapErrorListener errorListener) {
        super.errorListener(errorListener);
        return this;
    }


    /**
     * Map.put() returns the previous value, functionality which is rarely used but fairly cheap for HashMap.
     * In the case, for an off heap collection, it has to create a new object (or return a recycled one)
     * Either way it's expensive for something you probably don't use.
     *
     * @param putReturnsNull false if you want SharedHashMap.put() to not return the object that was replaced but instead return null
     * @return an instance of the map builder
     */
    public ReplicatedSharedHashMapWrapperBuilder putReturnsNull(boolean putReturnsNull) {
        super.putReturnsNull(putReturnsNull);
        return this;
    }


    /**
     * Map.remove()  returns the previous value, functionality which is rarely used but fairly cheap for HashMap.
     * In the case, for an off heap collection, it has to create a new object (or return a recycled one)
     * Either way it's expensive for something you probably don't use.
     *
     * @param removeReturnsNull false if you want SharedHashMap.remove() to not return the object that was removed but instead return null
     * @return an instance of the map builder
     */
    public ReplicatedSharedHashMapWrapperBuilder removeReturnsNull(boolean removeReturnsNull) {
        super.removeReturnsNull(removeReturnsNull);
        return this;
    }


    public ReplicatedSharedHashMapWrapperBuilder generatedKeyType(boolean generatedKeyType) {
        super.generatedKeyType(generatedKeyType);
        return this;
    }


    public ReplicatedSharedHashMapWrapperBuilder generatedValueType(boolean generatedValueType) {
        super.generatedValueType(generatedValueType);
        return this;
    }


    public ReplicatedSharedHashMapWrapperBuilder largeSegments(boolean largeSegments) {
        super.largeSegments(largeSegments);
        return this;
    }


    public ReplicatedSharedHashMapWrapperBuilder metaDataBytes(int metaDataBytes) {
        super.metaDataBytes(metaDataBytes);
        return this;
    }


    public ReplicatedSharedHashMapWrapperBuilder eventListener(SharedMapEventListener eventListener) {
        super.eventListener(eventListener);
        return this;
    }


}
