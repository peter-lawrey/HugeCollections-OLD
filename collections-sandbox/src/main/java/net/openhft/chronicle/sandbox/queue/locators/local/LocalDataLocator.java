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

package net.openhft.chronicle.sandbox.queue.locators.local;

import net.openhft.chronicle.sandbox.queue.locators.DataLocator;

/**
 * Created by Rob Austin
 */
public class LocalDataLocator<E> implements DataLocator<E> {

    // intentionally not volatile, as we are carefully ensuring that the memory barriers are controlled below by other objects
    private final Object[] data;
    private final int capacity;

    public LocalDataLocator(int capacity) {

        // the ring buffer works by having 1 item spare, that's why we add one
        this.capacity = capacity;
        this.data = new Object[this.capacity];
    }

    @Override
    public E getData(final int index) {
        return (E) data[index];
    }

    @Override
    public int setData(final int index, final E value) {
        data[index] = value;
        return 0;
    }

    @Override
    public void writeAll(final E[] newData, final int length) {
        System.arraycopy(newData, 0, data, 0, length);
    }

    @Override
    public int getCapacity() {
        return capacity;
    }
}
