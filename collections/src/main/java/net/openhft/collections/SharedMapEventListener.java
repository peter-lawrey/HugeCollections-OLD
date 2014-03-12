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

import net.openhft.lang.io.Bytes;

/**
 * This event listener is called when key events occur.
 * <p>All these calls are synchronous while a lock is held so make them as quick as possible</p>
 */
public interface SharedMapEventListener<K, V> {
    /**
     * This is called when there was no existing entry for a key.  Optionally you can provide a value to add to the map.
     *
     * @param map        accessed
     * @param keyBytes   bytes of the key looked up
     * @param key        object used as key
     * @param usingValue value provided to reuse, could be null.
     * @return null if null should be returned, or a value to put in the map and return.
     */
    V onGetMissing(SharedHashMap<K, V> map, Bytes keyBytes, K key, V usingValue);

    /**
     * This method is called if a value is found in the map.
     *
     * @param map           accessed
     * @param entry         bytes of the entry
     * @param metaDataBytes length of meta data for this map.
     * @param key           looked up
     * @param value         found
     */
    void onGetFound(SharedHashMap<K, V> map, Bytes entry, int metaDataBytes, K key, V value);

    /**
     * This method is called if a key/value is put in the map
     *
     * @param map           accessed
     * @param entry         added/modified
     * @param metaDataBytes length of the meta data
     * @param added         if this is a new entry
     * @param key           looked up
     * @param value         set for key
     */
    void onPut(SharedHashMap<K, V> map, Bytes entry, int metaDataBytes, boolean added, K key, V value);

    /**
     * This is called when an entry is removed. Misses are not notified.
     *
     * @param map           accessed
     * @param entry         removed
     * @param metaDataBytes length of meta data
     * @param key           removed
     * @param value         removed
     */
    void onRemove(SharedHashMap<K, V> map, Bytes entry, int metaDataBytes, K key, V value);
}
