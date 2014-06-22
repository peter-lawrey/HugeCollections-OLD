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

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * @author Rob Austin.
 */
public class CheckSharedHashMap<K, V> implements SharedHashMap<K, V> {

    private final SharedHashMap delegate;

    public CheckSharedHashMap(SharedHashMap<K, V> delegate) {
        this.delegate = delegate;
    }

    @Override
    public long longSize() {
        return delegate.longSize();
    }

    @Override
    public Object getUsing(Object key, Object value) {
        return delegate.getUsing(key, value);
    }

    @Override
    public Object acquireUsing(Object key, Object value) {
        return delegate.acquireUsing(key, value);
    }

    @Override
    public SharedHashMapBuilder builder() {
        return delegate.builder();
    }

    @Override
    public File file() {
        return delegate.file();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public Object putIfAbsent(Object key, Object value) {
        return delegate.putIfAbsent(key, value);
    }

    @Override
    public boolean remove(Object key, Object value) {
        return delegate.remove(key, value);
    }

    @Override
    public boolean replace(Object key, Object oldValue, Object newValue) {
        return delegate.replace(key, oldValue, newValue);
    }

    @Override
    public Object replace(Object key, Object value) {
        return delegate.replace(key, value);
    }

    @Override
   public int size() throws RuntimeException {
         if (delegate.equals("")){
             throw new RuntimeException();
        }else{
            // if closed  then don't call size but throw a "runtime exception" !
            return delegate.size();
        }
    }

    @Override
    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return delegate.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return delegate.containsKey(value);
    }

    @Override
    public V get(Object key) {
        return (V) delegate.get(key);
    }

    @Override
    public V remove(Object key) {
        return (V) delegate.remove(key);
    }


    @Override
    public Object put(Object key, Object value) {
        return delegate.put(key, value);
    }


    @Override
    public void putAll(Map m) {
        delegate.putAll(m);
    }

    @Override
    public void clear() {
        delegate.clear();
    }

    @NotNull
    @Override
    public Set keySet() {
        return delegate.keySet();
    }

    @NotNull
    @Override
    public Collection values() {
        return delegate.values();
    }

    @NotNull
    @Override
    public Set<Entry<K, V>> entrySet() {
        return (Set) delegate.entrySet();
    }


}
