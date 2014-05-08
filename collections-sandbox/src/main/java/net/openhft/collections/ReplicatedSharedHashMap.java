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
import net.openhft.lang.io.NativeBytes;

/**
 * @author Rob Austin.
 */
public interface ReplicatedSharedHashMap<K, V> extends SharedHashMap<K, V> {

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
     * @param identifier a unique identifier for a replicating node
     * @param timeStamp  timestamp in milliseconds, that the put() occurred
     * @return the previous value
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
    V put(K key, V value, byte identifier, long timeStamp);

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
    V remove(K key, V value, byte identifier, long timeStamp);


    /**
     * called when we receive a remote replication event
     *
     * @param entry the entry bytes of the remote node
     */
    void onUpdate(AbstractBytes entry);

    /**
     * Identifies which replicating node made the change
     * <p/>
     * If two nodes update their map at the same time with different values, we have to deterministically resolve which update wins,
     * because of eventual consistency both nodes should end up locally holding the same data.
     * Although it is rare two remote nodes could receive an update to their maps at exactly the same time for the same key, we have to handle this edge case,
     * its therefore important not to rely on timestamps alone to reconcile the updates. Typically the update with the newest timestamp should win,
     * but in this example both timestamps are the same, and the decision made to one node should be identical to the decision made to the other.
     * We resolve this simple dilemma by using a node identifier, each node will have a unique identifier, the update from the node with the smallest identifier wins.
     *
     * @return identifies which replicating node made the change
     */
    byte getIdentifier();

    // TODO doc
    ModificationIterator getModificationIterator();

    /**
     * Event types which should be replicated.
     *
     * @see VanillaSharedReplicatedHashMapBuilder#watchList()
     * @see VanillaSharedReplicatedHashMapBuilder#watchList(EventType, EventType...)
     */
    enum EventType {
        /**
         * For entry insertions and value updates (when the key is already present in the map).
         */
        PUT,

        /**
         * For entry removals.
         */
        REMOVE
    }

    // TODO doc
    interface ModificationIterator {
        boolean hasNext();

        boolean nextEntry(EntryCallback callback);
    }

    /**
     * Implemented typically by a replicator, This interface provides the event {@see onEntry(NativeBytes entry) } which will get called whenever a put() or remove() has occurred to the map
     */
    interface EntryCallback {

        /**
         * Called whenever a put() or remove() has occurred to the map, provided the {@code entry}, used typically by a replicator, which may indirectly via a socket connection pass it
         * on to another replicating maps {@see #onUpdate(AbstractBytes entry)} method.
         *
         * @param entry the entry you will receive, this does not have to be locked, as locking is already provided from the caller.
         * @return false if this entry should be ignored because the {@code identifier} is not from
         * one of our changes, WARNING even though we check the {@code identifier} in the
         * ModificationIterator the entry may have been updated.
         */
        boolean onEntry(final NativeBytes entry);

        /**
         * called just after {@see #onEntry(NativeBytes entry)}
         *
         * @see #onEntry(NativeBytes entry);
         */
        void onAfterEntry();

        /**
         * called just before {@see #onEntry(NativeBytes entry)}
         */
        void onBeforeEntry();
    }


}
