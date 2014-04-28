/*
 * Copyright 2014 Higher Frequency Trading
 * <p/>
 * http://www.higherfrequencytrading.com
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at * <p/>
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
import net.openhft.collections.SharedHashMapBuilder;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * @author Rob Austin.
 */
public class ReplicatedShareHashMap<K extends Object, V> implements SharedHashMap<K, V> {


    @NotNull
    private final MapModifier<K, V> mapModifier;
    private final Class<V> vClass;
    private final SharedHashMap<K, MetaData<V>> delegate;

    private EntrySet entrySet = null;
    private long timestamp;

    public ReplicatedShareHashMap(SharedHashMap<K, MetaData<V>> delegate, final MapModifier<K, V> mapModifier, Class<V> vClass) {
        this.delegate = delegate;
        this.mapModifier = mapModifier;
        this.vClass = vClass;
    }


    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return delegate.containsKey(key);
    }

    /**
     * {@inheritDoc}
     * <p/>
     * <p>This implementation iterates over <tt>entrySet()</tt> searching
     * for an entry with the specified value.  If such an entry is found,
     * <tt>true</tt> is returned.  If the iteration terminates without
     * finding such an entry, <tt>false</tt> is returned.  Note that this
     * implementation requires linear time in the size of the map.
     *
     * @throws ClassCastException   {@inheritDoc}
     * @throws NullPointerException {@inheritDoc}
     */
    public boolean containsValue(final Object value) {


        // we can perform a sneeky trick here providing our own equals method, it actually this method that is used in the comparison.
        Object wrapper = new Object() {

            public boolean equals(Object o) {

                if (this == o) return true;
                if (o == null || MetaData.class != o.getClass()) return false;

                MetaData<V> metaData = (MetaData) o;

                final V v = metaData.get(vClass);

                if (v == (V) value)
                    return true;

                if (v == null)
                    return false;

                return v.equals(value);

            }

        };

        return delegate.containsValue(wrapper);


    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V get(Object key) {

        // todo prehaps the meta data could be thread local make this thread local
        final MetaData<V> result = new MetaData<V>();

        // add for reuse of meta data
        result.clear();

        final MetaData<V> metaData = delegate.acquireUsing((K) key, result);
        return (metaData == null) ? null : metaData.get(vClass);
    }

    @Override
    public V put(K key, V value) {
        return mapModifier.put(key, value);
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public V remove(Object key) {
        return mapModifier.remove((K) key);
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void putAll(Map<? extends K, ? extends V> sourceMap) {

        for (Entry<? extends K, ? extends V> entry : sourceMap.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clear() {

        timestamp = System.currentTimeMillis();

        for (K key : delegate.keySet()) {
            mapModifier.removeWithTimeStamp(key, timestamp);
        }

    }


    private KeySet keySet = null;

    /**
     * Returns a {@link Set} view of the keys contained in this map.
     * The set is backed by the map, so changes to the map are
     * reflected in the set, and vice-versa.  If the map is modified
     * while an iteration over the set is in progress (except through
     * the iterator's own <tt>remove</tt> operation), the results of
     * the iteration are undefined.  The set supports element removal,
     * which removes the corresponding mapping from the map, via the
     * <tt>Iterator.remove</tt>, <tt>Set.remove</tt>,
     * <tt>removeAll</tt>, <tt>retainAll</tt>, and <tt>clear</tt>
     * operations.  It does not support the <tt>touch</tt> or <tt>addAll</tt>
     * operations.
     */
    @Override
    public Set<K> keySet() {

        if (keySet == null)
            keySet = new KeySet();

        return keySet;
    }


    @Override
    public Set<Entry<K, V>> entrySet() {
        if (entrySet == null)
            entrySet = new EntrySet();
        return entrySet;
    }


    @Override
    public long longSize() {
        return 0;
    }

    @Override
    public V getUsing(K key, V value) {
        // todo optimize this
        return delegate.get(key).getUsing(vClass, value);
    }

    @Override
    public V acquireUsing(K key, V value) {
        //todo
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public SharedHashMapBuilder builder() {
        return new SharedReplicatedHashMapBuilder();
    }

    @Override
    public File file() {
        return delegate.file();
    }

    @Override
    public void close() throws IOException {
        mapModifier.close();
    }

    @Override
    public V putIfAbsent(K key, V value) {
        return mapModifier.putIfAbsent(key, value);
    }

    @Override
    public boolean remove(Object key, Object value) {
        return mapModifier.remove(((K) key), (V) value);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        return mapModifier.replace(key,oldValue,newValue);
    }

    @Override
    public V replace(K key, V value) {
        return mapModifier.replace(key, value);
    }





    private final class KeySet extends AbstractSet<K> {
        public Iterator<K> iterator() {
            final Iterator<K> iterator = delegate.keySet().iterator();

            return new Iterator<K>() {

                K item = null;

                @Override
                public boolean hasNext() {
                    return iterator.hasNext();
                }

                @Override
                public K next() {
                    item = iterator.next();
                    return item;
                }

                @Override
                public void remove() {
                    if (item != null)
                        ReplicatedShareHashMap.this.remove(item);
                    else
                        throw new IllegalStateException("Next has not yet been called.");
                }
            };
        }

        public int size() {
            return ReplicatedShareHashMap.this.size();
        }

        public boolean contains(Object o) {
            return containsKey(o);
        }

        public boolean remove(Object o) {
            return ReplicatedShareHashMap.this.remove(o) != null;
        }

        public void clear() {
            ReplicatedShareHashMap.this.clear();
        }
    }


    final class EntrySet extends AbstractSet<Map.Entry<K, V>> {

        public Iterator<Map.Entry<K, V>> iterator() {

            final Iterator<Entry<K, MetaData<V>>> iterator = delegate.entrySet().iterator();

            return new Iterator<Map.Entry<K, V>>() {

                Entry<K, MetaData<V>> item = null;

                @Override
                public boolean hasNext() {
                    return iterator.hasNext();
                }

                @Override
                public Entry<K, V> next() {
                    final Map.Entry<K, MetaData<V>> metaDataEntry = iterator.next();

                    return new Map.Entry<K, V>() {

                        @Override
                        public K getKey() {
                            return metaDataEntry.getKey();
                        }

                        @Override
                        public V getValue() {
                            return metaDataEntry.getValue().get(vClass);
                        }

                        @Override
                        public V setValue(V value) {
                            return ReplicatedShareHashMap.this.put(metaDataEntry.getKey(), value);
                        }
                    };
                }

                @Override
                public void remove() {
                    if (item != null)
                        ReplicatedShareHashMap.this.remove(item.getKey());
                    else
                        throw new IllegalStateException("Next has not yet been called.");
                }
            };
        }


        public boolean contains(Object o) {
            if (!(o instanceof Map.Entry))
                return false;
            Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
            V v = ReplicatedShareHashMap.this.get(e.getKey());
            return v != null && v.equals(e.getValue());
        }

        public boolean remove(Object o) {
            if (!(o instanceof Map.Entry))
                return false;
            Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
            return ReplicatedShareHashMap.this.remove(e.getKey()) != null;
        }

        public int size() {
            return ReplicatedShareHashMap.this.size();
        }

        public boolean isEmpty() {
            return ReplicatedShareHashMap.this.isEmpty();
        }

        public void clear() {
            ReplicatedShareHashMap.this.clear();
        }
    }


    transient volatile Collection<V> values = null;

    @NotNull
    @Override
    public Collection<V> values() {
        Collection<V> vs = values;
        return (vs != null ? vs : (values = new Values()));
    }

    final class Values extends AbstractCollection<V> {

        public Iterator<V> iterator() {
            return new ValueIterator();
        }

        public int size() {
            return ReplicatedShareHashMap.this.size();
        }

        public boolean isEmpty() {
            return ReplicatedShareHashMap.this.isEmpty();
        }

        public boolean contains(Object o) {
            return ReplicatedShareHashMap.this.containsValue(o);
        }

        public void clear() {
            ReplicatedShareHashMap.this.clear();
        }
    }


    final class ValueIterator implements Iterator<V> {

        final Iterator<MetaData<V>> iterator = delegate.values().iterator();

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public V next() {
            return iterator.next().get(vClass);
        }

        @Override
        public void remove() {
            iterator.remove();
        }
    }


    /**
     * Compares the specified object with this map for equality.  Returns
     * <tt>true</tt> if the given object is also a map and the two maps
     * represent the same mappings.  More formally, two maps <tt>m1</tt> and
     * <tt>m2</tt> represent the same mappings if
     * <tt>m1.entrySet().equals(m2.entrySet())</tt>.  This ensures that the
     * <tt>equals</tt> method works properly across different implementations
     * of the <tt>Map</tt> interface.
     * <p/>
     * <p>This implementation first checks if the specified object is this map;
     * if so it returns <tt>true</tt>.  Then, it checks if the specified
     * object is a map whose size is identical to the size of this map; if
     * not, it returns <tt>false</tt>.  If so, it iterates over this map's
     * <tt>entrySet</tt> collection, and checks that the specified map
     * contains each mapping that this map contains.  If the specified map
     * fails to contain such a mapping, <tt>false</tt> is returned.  If the
     * iteration completes, <tt>true</tt> is returned.
     *
     * @param o object to be compared for equality with this map
     * @return <tt>true</tt> if the specified object is equal to this map
     */
    public boolean equals(Object o) {
        if (o == this)
            return true;

        if (!(o instanceof Map))
            return false;
        Map<K, V> m = (Map<K, V>) o;
        if (m.size() != size())
            return false;

        try {
            Iterator<Entry<K, V>> i = entrySet().iterator();
            while (i.hasNext()) {
                Entry<K, V> e = i.next();
                K key = e.getKey();
                V value = e.getValue();
                if (value == null) {
                    if (!(m.get(key) == null && m.containsKey(key)))
                        return false;
                } else {
                    if (!value.equals(m.get(key)))
                        return false;
                }
            }
        } catch (ClassCastException unused) {
            return false;
        } catch (NullPointerException unused) {
            return false;
        }

        return true;
    }

    /**
     * Returns a string representation of this map.  The string representation
     * consists of a list of key-value mappings in the order returned by the
     * map's <tt>entrySet</tt> view's iterator, enclosed in braces
     * (<tt>"{}"</tt>).  Adjacent mappings are separated by the characters
     * <tt>", "</tt> (comma and space).  Each key-value mapping is rendered as
     * the key followed by an equals sign (<tt>"="</tt>) followed by the
     * associated value.  Keys and values are converted to strings as by
     * {@link String#valueOf(Object)}.
     *
     * @return a string representation of this map
     */
    public String toString() {
        Iterator<Entry<K, V>> i = entrySet().iterator();
        if (!i.hasNext())
            return "{}";

        StringBuilder sb = new StringBuilder();
        sb.append('{');
        for (; ; ) {
            Entry<K, V> e = i.next();
            K key = e.getKey();
            V value = e.getValue();
            sb.append(key == this ? "(this Map)" : key);
            sb.append('=');
            sb.append(value == this ? "(this Map)" : value);
            if (!i.hasNext())
                return sb.append('}').toString();
            sb.append(',').append(' ');
        }
    }
}