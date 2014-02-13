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
 * Created with IntelliJ IDEA. User: peter Date: 09/12/13 Time: 15:21 To change this template use File | Settings | File
 * Templates.
 */
public class IntIntMultiMapTest {
    @Test
    public void testPutRemoveSearch() {
        IntIntMultiMap map = new IntIntMultiMap(16);
        assertEquals("{ }", map.toString());
        assertEquals(0, map.size());
        map.put(1, 11);
        assertEquals(1, map.size());
        assertEquals("{ 1=11 }", map.toString());
        map.put(3, 33);
        assertEquals(2, map.size());
        assertEquals("{ 1=11, 3=33 }", map.toString());
        map.put(1, 12);
        map.put(1, 13);
        map.put(1, 14);
        map.put(3, 32);
        map.put(1, 15);
        assertEquals(7, map.size());
        assertEquals("{ 1=11, 1=12, 3=33, 1=13, 1=14, 3=32, 1=15 }", map.toString());

        assertTrue(map.remove(1, 11));
        assertEquals(6, map.size());
        assertEquals("{ 1=15, 1=12, 3=33, 1=13, 1=14, 3=32 }", map.toString());
        assertFalse(map.remove(1, 11));

        map.startSearch(3);
        assertEquals(33, map.nextInt());
        assertEquals(32, map.nextInt());
        assertEquals(IntIntMultiMap.UNSET, map.nextInt());

        map.startSearch(1);
        assertEquals(15, map.nextInt());
        assertEquals(12, map.nextInt());
        assertEquals(13, map.nextInt());
        assertEquals(14, map.nextInt());
        assertEquals(IntIntMultiMap.UNSET, map.nextInt());

        map.remove(1, 12);
        assertEquals(5, map.size());
        assertEquals("{ 1=15, 1=14, 3=33, 1=13, 3=32 }", map.toString());

        map.remove(1, 15);
        assertEquals(4, map.size());
        assertEquals("{ 1=13, 1=14, 3=33, 3=32 }", map.toString());

        map.remove(1, 13);
        assertEquals(3, map.size());
        assertEquals("{ 1=14, 3=33, 3=32 }", map.toString());

        map.remove(1, 14);
        assertEquals(2, map.size());
        assertEquals("{ 3=33, 3=32 }", map.toString());
    }
    
    @Test
    public void testRemoveSpecific() {
    	// Testing a specific case when the remove method on the map does (did) not work as expected. The size goes correctly to
    	// 0 but the value is still present in the map.
    	IntIntMultiMap map = new IntIntMultiMap(10);
    	
    	map.put(15, 1);
		map.remove(15, 1);
		map.startSearch(15);    		
		assertEquals(IntIntMultiMap.UNSET, map.nextInt());
    }
}
