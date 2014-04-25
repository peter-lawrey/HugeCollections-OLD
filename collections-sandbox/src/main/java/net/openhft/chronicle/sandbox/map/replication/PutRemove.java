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

import net.openhft.collections.SharedHashMap;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

/**
 * This class is just concerned with adding and removing entries to the delegate map.
 *
 * @author Rob Austin.
 */
public class PutRemove<K extends Object, V> implements Closeable {

    private final byte sequenceNumber;

    private final SharedHashMap<K, MetaData<V>> delegate;
    private final SharedHashMap<K, MetaData<V>> deletedItemsMap;
    @NotNull
    private final Class<V> vClass;
    private boolean generatedValueType;


    /**
     * @param sequenceNumber
     * @param delegate
     * @param deletedItemsMap
     * @param generatedValueType
     */
    public PutRemove(byte sequenceNumber,
                     @NotNull final SharedHashMap<K, MetaData<V>> delegate,
                     @NotNull final SharedHashMap<K, MetaData<V>> deletedItemsMap,
                     @NotNull final Class<V> clazzV, boolean generatedValueType) {
        this.sequenceNumber = sequenceNumber;
        this.delegate = delegate;
        this.deletedItemsMap = deletedItemsMap;
        this.vClass = clazzV;
        this.generatedValueType = generatedValueType;
    }

    /**
     * @return null if no object was removed
     */
    public V remove(K key) {
        return removeWithTimeStamp(key, System.currentTimeMillis());
    }

    /**
     * remotes the entry and tags it the with the time stamp provided
     *
     * @param key
     * @param timestamp
     * @return
     */
    public V removeWithTimeStamp(K key, long timestamp) {
        return remove(key, timestamp, sequenceNumber);
    }


    public V remove(K key, long timestamp, byte sequenceNumber) {

        final MetaData<V> live = delegate.get(key);
        MetaData<V> deleted = deletedItemsMap.get(key);

        final MetaData metaData = (live != null && live.isNewer(deleted)) ? live : deleted;

        // someone else has made a change before us, so we are going to ignore this update
        if (metaData != null && metaData.isNewer(timestamp, sequenceNumber))
            return null;

        if (live != null && delegate.remove(key, live))
            return remove(key, timestamp, sequenceNumber);

        // if we dot have and item in live then we will put one
        if (deleted == null) {
            deleted = deletedItemsMap.putIfAbsent(key, MetaData.create(timestamp, sequenceNumber, (byte) 'D'));

            // someone has just made a change to the deleted items, so lets try again
            if (deleted != null)
                return remove(key, timestamp, sequenceNumber);


        } else if (!deletedItemsMap.replace(key, deleted, MetaData.create(timestamp, sequenceNumber, (byte) 'D'))) {
            return remove(key, timestamp, sequenceNumber);
        }

        return (live == null) ? null : live.get(vClass);
    }


    /**
     * @return null if no object was removed
     */
    public V put(K key, V value) {
        return put(key, value, System.currentTimeMillis());
    }

    /**
     * remotes the entry and tags it the with the time stamp provided
     *
     * @param key
     * @param timestamp
     * @return
     */
    public V put(K key, V value, long timestamp) {
        return put(key, value, timestamp, sequenceNumber);
    }


    public V put(K key, V value, long timestamp, byte sequenceNumber) {

        if (value == null)
            throw new NullPointerException("value is NULL.");

        MetaData<V> live = delegate.get(key);
        final MetaData<V> deleted = deletedItemsMap.get(key);

        final MetaData metaData = (live != null && live.isNewer(deleted)) ? live : deleted;

        // someone else has made a change before us, so we are going to ignore this update
        if (metaData != null && metaData.isNewer(timestamp, sequenceNumber))
            return null;

        // if we dot have and item in live then we will put one
        if (live == null) {
            live = delegate.putIfAbsent(key, MetaData.create(timestamp, sequenceNumber, (byte) 'P', value, vClass, generatedValueType));

            // someone has just made a change to live, so lets try again
            if (live != null)
                return put(key, value, timestamp, sequenceNumber);


        } else if (!delegate.replace(key, live, MetaData.create(timestamp, sequenceNumber, (byte) 'P', value, vClass, generatedValueType)))
            return put(key, value, timestamp, sequenceNumber);

        if (deleted != null)
            deletedItemsMap.remove(key, deleted);

        return (live == null) ? null : live.get(vClass);
    }

    public void close() {

    }

    public V putIfAbsent(K key, V value) {

        if (value == null)
            throw new NullPointerException("value is NULL.");


        long timestamp = System.currentTimeMillis();

        final MetaData<V> live = delegate.get(key);

        if (live != null)
            return live.get(vClass);

        final MetaData<V> deleted = deletedItemsMap.get(key);


        // someone else has made a change before us, so we are going to ignore this update
        if (deleted != null && deleted.isNewer(timestamp, sequenceNumber))
            return null;

        final MetaData<V> prev = delegate.putIfAbsent(key, MetaData.create(timestamp, sequenceNumber, (byte) 'P', value, vClass, generatedValueType));

        if (deleted != null)
            deletedItemsMap.remove(key, deleted);

        return (prev == null) ? null : prev.get(vClass);

    }

    public boolean remove(K key, V expectedValue) {
        return remove(key, expectedValue, System.currentTimeMillis());
    }

    public boolean remove(K key, V expectedValue, long timestamp) {

        if (expectedValue == null)
            return false;

        final MetaData<V> live = delegate.get(key);
        MetaData<V> deleted = deletedItemsMap.get(key);

        if (live == null || !expectedValue.equals(live.get(vClass)))
            return false;


        final MetaData metaData = live.isNewer(deleted) ? live : deleted;

        // someone else has made a change before us, so we are going to ignore this update
        if (metaData != null && metaData.isNewer(timestamp, sequenceNumber))
            return false;

        if (delegate.remove(key, live))
            return remove(key, expectedValue, timestamp);

        if (deleted == null) {
            deleted = deletedItemsMap.putIfAbsent(key, MetaData.create(timestamp, sequenceNumber, (byte) 'D'));

            // someone has just made a change to the deleted items, so lets try again
            if (deleted != null)
                return remove(key, expectedValue, timestamp);


        } else if (!deletedItemsMap.replace(key, deleted, MetaData.create(timestamp, sequenceNumber, (byte) 'D'))) {
            return remove(key, expectedValue, timestamp);
        }

        return true;
    }

    public boolean replace(K key, V oldValue, V newValue) {
        return replace(key, oldValue, newValue, System.currentTimeMillis());
    }


    public boolean replace(K key, V oldValue, V newValue, long timestamp) {

        if (oldValue == null)
            throw new NullPointerException("oldValue is NULL.");

        if (newValue == null)
            throw new NullPointerException("newValue is NULL.");


        // todo replace the get with a getUsing() me
        final MetaData<V> live = delegate.get(key);
        final MetaData<V> deleted = deletedItemsMap.get(key);


        if (live == null || !live.get(vClass).equals(oldValue))
            return false;

        // we take the newest meta data either from live or deleted
        final MetaData metaData = live.isNewer(deleted) ? live : deleted;

        // someone else has made a change before us, so we are going to ignore this update
        if (metaData != null && metaData.isNewer(timestamp, sequenceNumber))
            return false;

        // if we dot have and item in live then we will put one
        if (!delegate.replace(key, live, MetaData.create(timestamp, sequenceNumber, (byte) 'P', newValue, vClass, generatedValueType)))
            return replace(key, oldValue, newValue, timestamp);

        if (deleted != null)
            deletedItemsMap.remove(key, deleted);

        return true;

    }

    public V replace(K key, V value) {
        return replace(key, value, System.currentTimeMillis());
    }


    public V replace(K key, V value, long timestamp) {

        if (value == null)
            throw new NullPointerException("value is NULL.");

        MetaData<V> live = delegate.get(key);
        final MetaData<V> deleted = deletedItemsMap.get(key);


        if (live == null)
            return null;

        final MetaData metaData = (live != null && live.isNewer(deleted)) ? live : deleted;

        // someone else has made a change before us, so we are going to ignore this update
        if (metaData != null && metaData.isNewer(timestamp, sequenceNumber))
            return null;

        // if we dot have and item in live then we will put one
        if (!delegate.replace(key, live, MetaData.create(timestamp, sequenceNumber, (byte) 'P', value, vClass, generatedValueType)))
            return put(key, value, timestamp, sequenceNumber);

        if (deleted != null)
            deletedItemsMap.remove(key, deleted);

        return (live == null) ? null : live.get(vClass);
    }
}



