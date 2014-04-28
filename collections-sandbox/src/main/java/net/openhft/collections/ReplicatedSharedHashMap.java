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

/**
 * @author Rob Austin.
 */
public interface ReplicatedSharedHashMap<K, V> extends SharedHashMap<K, V>, SegmentInfoProvider {


    /**
     * Used in conjunction with map replication, all put() events that originate from a remote node will be processed using this method
     * <p/>
     * <p/>
     * Associates the specified value with the specified key in this map
     * (optional operation).  If the map previously contained a mapping for
     * the key, the old value is replaced by the specified value.  (A map
     * <tt>m</tt> is said to contain a mapping for a key <tt>k</tt> if and only
     * if {@link #containsKey(Object) m.containsKey(k)} would return
     * <tt>true</tt>.)
     *
     * @param key        key with which the specified value is to be associated
     * @param value      value to be associated with the specified key
     * @param timeStamp  timestamp in milliseconds, that the put() occurred
     * @param identifier a unique identifier for a replicating node
     * @return
     * @throws UnsupportedOperationException if the <tt>put</tt> operation
     *                                       is not supported by this map
     * @throws ClassCastException            if the class of the specified key or value
     *                                       prevents it from being stored in this map
     * @throws NullPointerException          if the specified key or value is null
     *                                       and this map does not permit null keys or values
     * @throws IllegalArgumentException      if some property of the specified key
     *                                       <p/>
     *                                       public V put(K key, V value, long timeStamp);
     *                                       <p/>
     *                                       /**
     */
    public V put(K key, V value, long timeStamp, byte identifier);

    /**
     * Used in conjunction with map replication, all remove() events that originate from a remote node will be processed using this method
     * <p/>
     * Removes the entry for a key only if currently mapped to a given value.
     * This is equivalent to
     * <pre>
     *   if (map.containsKey(key) &amp;&amp; map.get(key).equals(value)) {
     *       map.remove(key);
     *       return true;
     *   } else return false;</pre>
     * except that the action is performed atomically.
     *
     * @param key        key with which the specified value is associated
     * @param value      value expected to be associated with the specified key
     * @param identifier a unique identifier for a replicating node
     * @param timeStamp  timestamp in milliseconds, that the remove() occurred
     * @return <tt>true</tt> if the value was removed
     * @throws UnsupportedOperationException if the <tt>remove</tt> operation
     *                                       is not supported by this map
     * @throws ClassCastException            if the key or value is of an inappropriate
     *                                       type for this map
     *                                       (<a href="../Collection.html#optional-restrictions">optional</a>)
     * @throws NullPointerException          if the specified key or value is null,
     *                                       and this map does not permit null keys or values
     *                                       (<a href="../Collection.html#optional-restrictions">optional</a>)
     */
    public V remove(K key, V value, byte identifier, long timeStamp);


}
