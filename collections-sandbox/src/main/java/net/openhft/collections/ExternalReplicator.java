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

import net.openhft.lang.io.NativeBytes;
import org.jetbrains.annotations.NotNull;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Map;
import java.util.Set;

import static net.openhft.collections.ReplicatedSharedHashMap.EntryResolver;

/**
 * Used for both file and and database replication
 *
 * @author Rob Austin.
 */
public interface ExternalReplicator<K, V> {

    /**
     * gets the data <V> from an external source
     *
     * @param k the key of the entry
     * @return the value of the entry
     */
    V getExternal(K k);


    /**
     * reads all the entries from the external source and writes into the {@code usingMap}. This method will
     * alter the contents of {@code usingMap}
     *
     * @param usingMap the map to which the data will be written
     * @return the {@code usingMap}
     */
    Map<K, V> getAllExternal(@NotNull final Map<K, V> usingMap);


    /**
     * @return the timezone of the external source, for example database
     */
    DateTimeZone getZone();

    /**
     * an base implementation of an ExternalReplicator
     *
     * @param <K>
     * @param <V>
     */
    abstract class AbstractExternalReplicator<K, V>
            extends ReplicatedSharedHashMap.EntryCallback
            implements ExternalReplicator<K, V> {

        private final V usingValue;
        private final K usingKey;
        private final EntryResolver<K, V> entryResolver;

        protected AbstractExternalReplicator(final Class<K> kClass,
                                             final Class<V> vClass,
                                             final EntryResolver<K, V> entryResolver)
                throws InstantiationException {

            usingValue = (V) NativeBytes.UNSAFE.allocateInstance(vClass);
            usingKey = (K) NativeBytes.UNSAFE.allocateInstance(kClass);
            this.entryResolver = entryResolver;
        }

        @Override
        public boolean onEntry(NativeBytes entry) {

            final K key = entryResolver.key(entry, usingKey);

            if (entryResolver.wasRemoved(entry))
                removeExternal(key);

            final V value = entryResolver.value(entry, usingValue);
            if (value == null)
                return false;

            putExternal(key, value, false);
            return true;
        }


        /**
         * write the entry to an external source, be it a database or file, this is called indirectly when
         * ever data is put() on a shared hash map
         *
         * @param key   the key of the entry
         * @param value the value of the entry
         * @param added true if the entry has just been added to the map rather than updated
         */
        abstract void putExternal(K key, V value, boolean added);

        /**
         * removes a single entry from the external source which releates to key {@code k},
         */
        abstract void removeExternal(K k);


        /**
         * removes form the external source, all the entries who's keys are in {@code keys}
         */
        abstract void removeAllExternal(final Set<K> keys);

    }

    final DateTimeFormatter DEFAULT_DATE_TIME_FORMATTER = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss.S")
            .withZoneUTC();


}


