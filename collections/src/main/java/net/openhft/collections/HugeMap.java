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

import java.util.concurrent.ConcurrentMap;

/**
 * User: plawrey
 * Date: 07/12/13
 * Time: 11:44
 */
interface HugeMap<K, V> extends ConcurrentMap<K, V> {
    /**
     * Estimate how much off heap memory is used by this data structure (not based on how much of the map is used)
     *
     * @return the off heap used.
     */
    long offHeapUsed();

    /**
     * Get a value, optionally populating a pre-allocated object.
     *
     * @param key   to search for
     * @param value to set
     * @return the value, populated, a new object, or null if not found.
     */
    V get(K key, V value);
}
