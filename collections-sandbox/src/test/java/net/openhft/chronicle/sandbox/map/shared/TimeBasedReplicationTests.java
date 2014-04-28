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

package net.openhft.chronicle.sandbox.map.shared;

import net.openhft.chronicle.sandbox.map.replication.ReplicatedShareHashMap;
import net.openhft.chronicle.sandbox.map.replication.SharedReplicatedHashMapBuilder;
import net.openhft.chronicle.sandbox.queue.shared.SharedJSR166TestCase;
import net.openhft.collections.SharedHashMap;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

/**
 * @author Rob Austin.
 */
public class TimeBasedReplicationTests extends SharedJSR166TestCase {


    private static File getPersistenceFile() {
        String TMP = System.getProperty("java.io.tmpdir");
        File file = new File(TMP + "/shm-test" + System.nanoTime());
        file.delete();
        file.deleteOnExit();
        return file;
    }

    static ReplicatedShareHashMap<Integer, CharSequence> newShmIntString(int size) throws IOException {

        return new SharedReplicatedHashMapBuilder()
                .entries(size)
                .create(getPersistenceFile(), getPersistenceFile(), Integer.class, CharSequence.class, (byte) 1);

    }


    /**
     * Returns a new map from Integers 1-5 to Strings "A"-"E".
     */
    private static SharedHashMap map5() throws IOException {
        SharedHashMap<Integer, CharSequence> map = newShmIntString(5);
        assertTrue(map.isEmpty());
        map.put(one, "A");
        map.put(two, "B");
        map.put(three, "C");
        map.put(four, "D");
        map.put(five, "E");
        assertFalse(map.isEmpty());
        assertEquals(5, map.size());
        return map;
    }

    /**
     * Maps with same contents are equal
     */
    @Test
    public void testEquals() throws IOException {
        SharedHashMap map1 = map5();
        SharedHashMap map2 = map5();
        assertEquals(map1, map2);
        assertEquals(map2, map1);
        map1.clear();
        assertFalse(map1.equals(map2));
        assertFalse(map2.equals(map1));
    }
}
