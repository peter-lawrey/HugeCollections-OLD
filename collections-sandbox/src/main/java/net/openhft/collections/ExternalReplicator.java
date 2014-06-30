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

import java.util.Map;

/**
 * Used for both file and and database replication
 *
 * @author Rob Austin.
 */
public interface ExternalReplicator<K, V> {

    final DateTimeFormatter DEFAULT_DATE_TIME_FORMATTER = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss.S")
            .withZoneUTC();

    /**
     * write the entry to an external source, be it a database or file, this is called indirectly when ever
     * data is put() on a shared hash map
     *
     * @param key   the key of the entry
     * @param value the value of the entry
     * @param added true if the entry has just been added to the map rather than updated
     */
    void putExternal(K key, V value, boolean added);


    /**
     * gets the data <V> from an external source
     *
     * @param k the key of the entry
     * @return the value of the entry
     */
    V getExternal(K k);


    /**
     * reads the entry from the external source and puts them in the map which is associated with this
     * replication
     *
     * @param k the key relating to the entry that has change at the eternal source
     */
    void putEntry(K k);


    /**
     * removes all the files or database records associated with the map
     */
    void removeAllExternal();

    /**
     * reads all the entries from the external source and writes into the map which is associated with this
     * replication
     */
    void putAllEntries();


    /**
     * @return the timezone of the external source, for example database
     */
    DateTimeZone getZone();


    abstract class AbstractExternalReplicator<K, V> implements ExternalReplicator<K, V> {


        final Map<K, V> map;

        protected AbstractExternalReplicator(Map<K, V> map) {
            this.map = map;
        }

        @Override
        public void putEntry(K k) {
            map.put(k, getExternal(k));
        }


    }

}


