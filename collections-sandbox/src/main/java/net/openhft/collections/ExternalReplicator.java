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

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Used for both file and and database replication
 *
 * @author Rob Austin.
 */
public interface ExternalReplicator<K, V> {

    /**
     * write the entry to an external source, be it a database or file
     *
     * @param key   the key of the entry
     * @param value the value of the entry
     * @param added true if the entry has just been added to the map rather than updated
     */
    void put(K key, V value, boolean added);


    /**
     * gets the data <V> from an external source
     *
     * @param k the key of the entry
     * @return the value of the entry
     */
    V get(K k);


    ExternalReplicator withZone(DateTimeZone timeZone);

    abstract class AbstractExternalReplicator<K, V, M extends SharedHashMap<K, V>>
            extends SharedMapEventListener<K, V, M> implements ExternalReplicator<K, V> {

        final DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss.S")
                .withZoneUTC();

        public AbstractExternalReplicator withZone(DateTimeZone timeZone) {
            dateTimeFormatter.withZone(timeZone);
            return this;
        }

    }
}


