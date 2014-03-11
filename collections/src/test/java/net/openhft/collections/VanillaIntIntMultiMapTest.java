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


import org.junit.Test;

import static org.junit.Assert.*;

/**
 * User: peter
 * Date: 09/12/13
 */
public class VanillaIntIntMultiMapTest {
    @Test
    public void testPutRemoveSearch() {
        IntIntMultiMap map = new VanillaIntIntMultiMap(16);
        assertEquals("{ }", map.toString());
        map.put(1, 11);
        map.startSearch(1);
        int n1 = map.nextPos();
        int n2 = map.nextPos();
        assertEquals(11, n1);
        assertTrue(n2 < 0);

        assertEquals("{ 1=11 }", map.toString());
        map.put(3, 33);
        assertEquals("{ 1=11, 3=33 }", map.toString());
        map.put(1, 12);
        map.put(1, 13);
        map.put(1, 14);
        map.put(3, 32);
        map.put(1, 15);
        assertEquals("{ 1=11, 1=12, 3=33, 1=13, 1=14, 3=32, 1=15 }", map.toString());

        assertTrue(map.remove(1, 11));
        assertEquals("{ 1=15, 1=12, 3=33, 1=13, 1=14, 3=32 }", map.toString());
        assertFalse(map.remove(1, 11));

        map.startSearch(3);
        assertEquals(33, map.nextPos());
        assertEquals(32, map.nextPos());
        assertTrue(map.nextPos() < 0);

        map.startSearch(1);
        assertEquals(15, map.nextPos());
        assertEquals(12, map.nextPos());
        assertEquals(13, map.nextPos());
        assertEquals(14, map.nextPos());
        assertTrue(map.nextPos() < 0);

        map.remove(1, 12);
        assertEquals("{ 1=15, 1=14, 3=33, 1=13, 3=32 }", map.toString());

        map.remove(1, 15);
        assertEquals("{ 1=13, 1=14, 3=33, 3=32 }", map.toString());

        map.remove(1, 13);
        assertEquals("{ 1=14, 3=33, 3=32 }", map.toString());

        map.remove(1, 14);
        assertEquals("{ 3=33, 3=32 }", map.toString());
    }

    @Test
    public void testPutLimited() {
        IntIntMultiMap map = new VanillaIntIntMultiMap(16);
        assertTrue(map.putLimited(1, 11, 2));
        assertTrue(map.putLimited(1, 12, 2));
        assertFalse(map.putLimited(1, 13, 2));
        assertTrue(map.putLimited(3, 31, 2));
        assertTrue(map.putLimited(3, 32, 2));
        assertFalse(map.putLimited(3, 33, 2));
        // not enough space
        assertFalse(map.putLimited(2, 22, 2));
        assertTrue(map.putLimited(2, 22, 4));
    }

    @Test
    public void firstAndNextNonEmptyPos() {
        IntIntMultiMap map = new VanillaIntIntMultiMap(16);
        map.put(1, 11);
        map.put(2, 22);
        map.put(3, 33);
        assertEquals(11, map.firstPos());
        assertEquals(22, map.nextKeyAfter(1));
        assertEquals(33, map.nextKeyAfter(2));
        assertEquals(-1, map.nextKeyAfter(3));
    }

    @Test
    public void testRemoveSpecific() {
        // Testing a specific case when the remove method on the map does (did) not work as expected. The size goes correctly to
        // 0 but the value is still present in the map.
        IntIntMultiMap map = new VanillaIntIntMultiMap(10);

        map.put(15, 1);
        map.remove(15, 1);
        map.startSearch(15);
        assertTrue(map.nextPos() < 0);
    }
}
