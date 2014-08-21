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

package net.openhft.chronicle.sandbox.queue;

import net.openhft.lang.model.constraints.NotNull;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by Rob Austin
 */
abstract class BlockingQueueDelegate<E> implements BlockingQueue<E> {

    abstract BlockingQueue<E> getDelegate();

    @Override
    public boolean add(E e) {
        return getDelegate().add(e);
    }

    @Override
    public boolean offer(E e) {
        return getDelegate().offer(e);
    }

    @Override
    public E remove() {
        return getDelegate().remove();
    }

    @Override
    public E poll() {
        return getDelegate().poll();
    }

    @Override
    public E element() {
        return getDelegate().element();
    }

    @Override
    public E peek() {
        return getDelegate().peek();
    }

    @Override
    public void put(E e) throws InterruptedException {
        getDelegate().put(e);
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        return getDelegate().offer(e, timeout, unit);
    }

    @Override
    public E take() throws InterruptedException {
        return getDelegate().take();
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        return getDelegate().poll(timeout, unit);
    }

    @Override
    public int remainingCapacity() {
        return getDelegate().remainingCapacity();
    }

    @Override
    public boolean remove(Object o) {
        return getDelegate().remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return getDelegate().containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        return getDelegate().addAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return getDelegate().removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return getDelegate().retainAll(c);
    }

    @Override
    public void clear() {
        getDelegate().clear();
    }

    @Override
    public int size() {
        return getDelegate().size();
    }

    @Override
    public boolean isEmpty() {
        return getDelegate().isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return getDelegate().contains(o);
    }

    @NotNull
    @Override
    public Iterator<E> iterator() {
        return getDelegate().iterator();
    }

    @NotNull
    @Override
    public Object[] toArray() {
        return getDelegate().toArray();
    }

    @NotNull
    @Override
    public <T> T[] toArray(T[] a) {
        return getDelegate().toArray(a);
    }

    @Override
    public int drainTo(Collection<? super E> c) {
        return getDelegate().drainTo(c);
    }

    @Override
    public int drainTo(Collection<? super E> c, int maxElements) {
        return getDelegate().drainTo(c, maxElements);
    }

    @Override
    public int hashCode() {
        return getDelegate().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return getDelegate().equals(obj);
    }

    @Override
    public String toString() {
        return getDelegate().toString();
    }
}
