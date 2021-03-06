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

import net.openhft.lang.model.constraints.NotNull;
import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Rob Austin.
 */
public class ReplicationCheckingMap<K, V> implements SharedHashMap<K, V> {

    SharedHashMap<K, V> map1;
    SharedHashMap<K, V> map2;

    public ReplicationCheckingMap(SharedHashMap map1, SharedHashMap map2) {
        this.map1 = map1;
        this.map2 = map2;
    }

    @Override
    public V putIfAbsent(K key, V value) {
        return map1.putIfAbsent(key, value);
    }

    @Override
    public boolean remove(Object key, Object value) {
        return map1.remove(key, value);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        return map1.replace(key, oldValue, newValue);
    }

    @Override
    public V replace(final K key, final V value) {

        return check(new Call<K, V>() {
                         @Override
                         public Object method(ConcurrentMap<K, V> map) {
                             return map.replace(key, value);
                         }
                     }
        );
    }

    @Override
    public int size() {

        return check(new Call<K, V>() {
                         @Override
                         public Object method(ConcurrentMap<K, V> map) {
                             return map.size();
                         }
                     }
        );

    }


    interface Call<K, V> {
        Object method(ConcurrentMap<K, V> map);
    }

    public <R> R check(Call instance) {
        R r1 = null;
        R r2 = null;
        for (int i = 0; i < 50; i++) {

            r1 = (R) instance.method(map1);
            r2 = (R) instance.method(map2);

            if (r1 != null && r1.equals(r2))
                return r1;

            if (i > 30) {
                try {
                    Thread.sleep(i);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            } else
                Thread.yield();

        }


        Assert.assertEquals(map1, map2);
        System.out.print(map1);
        System.out.print(map2);

        if (r1 != null)
            Assert.assertEquals(r1.toString(), r2.toString());


        return (R) r1;

    }

    @Override
    public boolean isEmpty() {

        return check(new Call<K, V>() {
                         @Override
                         public Object method(ConcurrentMap<K, V> map) {
                             return map.isEmpty();
                         }
                     }
        );

    }

    @Override
    public boolean containsKey(final Object key) {


        return check(new Call<K, V>() {
                         @Override
                         public Object method(ConcurrentMap<K, V> map) {
                             return map.containsKey(key);
                         }
                     }
        );

    }

    @Override
    public boolean containsValue(final Object value) {

        return check(new Call<K, V>() {
                         @Override
                         public Object method(ConcurrentMap<K, V> map) {
                             return map.containsValue(value);
                         }
                     }
        );

    }

    @Override
    public V get(final Object key) {


        return check(new Call<K, V>() {
                         @Override
                         public Object method(ConcurrentMap<K, V> map) {
                             return map.get(key);
                         }
                     }
        );

    }

    @Override
    public V put(K key, V value) {
        return map1.put(key, value);
    }

    @Override
    public V remove(Object key) {
        return map1.remove(key);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        map1.putAll(m);
    }

    @Override
    public void clear() {
        map1.clear();
    }

    @NotNull
    @Override
    public Set<K> keySet() {


        return check(new Call<K, V>() {
                         @Override
                         public Object method(ConcurrentMap<K, V> map) {
                             return map.keySet();
                         }
                     }
        );

    }


    @NotNull
    @Override
    public Collection<V> values() {


        return check(new Call<K, V>() {
                         @Override
                         public Collection<V> method(ConcurrentMap<K, V> map) {
                             return map.values();
                         }
                     }
        );

    }

    @NotNull
    @Override
    public Set<Entry<K, V>> entrySet() {


        return check(new Call<K, V>() {
                         @Override
                         public Object method(ConcurrentMap<K, V> map) {
                             return (map.entrySet());
                         }
                     }
        );

    }

    @Override
    public long longSize() {
        return map1.longSize();
    }

    @Override
    public V getUsing(K key, V value) {
        return map1.getUsing(key, value);
    }

    @Override
    public V acquireUsing(K key, V value) {
        return map1.acquireUsing(key, value);
    }

    @Override
    public File file() {
        throw new UnsupportedOperationException();
    }


    @Override
    public void close() throws IOException {
        map1.close();
        map2.close();
    }

    @Override
    public boolean equals(Object o) {
        return map1.equals(o);
    }

    @Override
    public int hashCode() {
        return map1.hashCode();
    }

    public String toString() {
        return map1.toString();
    }


}
