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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created with IntelliJ IDEA. User: peter Date: 09/12/13 Time: 15:56 To change this template use File | Settings | File
 * Templates.
 */
public class SegmentTest {
    @Test
    public void testSegment() {
        HugeConfig config = HugeConfig.SMALL.clone();
        config.setSmallEntrySize(32);
        config.setEntriesPerSegment(32);
        HugeHashMap.Segment<Integer, String> segment = new HugeHashMap.Segment<Integer, String>(config, false, false, String.class);
        segment.put(1, 111, "one");
        segment.put(1, 112, "two");
        segment.put(3, 301, "three");
        segment.put(1, 113, "four");
        segment.put(3, 302, "five");
        segment.put(1, 114, "six");
        assertEquals(6, segment.size());

        assertEquals("one", segment.get(1, 111, null));
        assertEquals("two", segment.get(1, 112, null));
        assertEquals("three", segment.get(3, 301, null));
        assertEquals("four", segment.get(1, 113, null));
        assertEquals("five", segment.get(3, 302, null));
        assertEquals("six", segment.get(1, 114, null));

        assertTrue(segment.remove(1, 111));
        assertEquals(5, segment.size());
        assertEquals(null, segment.get(1, 111, null));
        assertEquals("two", segment.get(1, 112, null));
        assertEquals("three", segment.get(3, 301, null));
        assertEquals("four", segment.get(1, 113, null));
        assertEquals("five", segment.get(3, 302, null));
        assertEquals("six", segment.get(1, 114, null));

        assertTrue(segment.remove(1, 112));
        assertEquals(null, segment.get(1, 112, null));
        assertEquals("three", segment.get(3, 301, null));
        assertEquals("four", segment.get(1, 113, null));
        assertEquals("five", segment.get(3, 302, null));
        assertEquals("six", segment.get(1, 114, null));

        assertTrue(segment.remove(1, 113));
        assertEquals(null, segment.get(1, 113, null));
        assertEquals("three", segment.get(3, 301, null));
        assertEquals("five", segment.get(3, 302, null));
        assertEquals("six", segment.get(1, 114, null));

        assertTrue(segment.remove(1, 114));
        assertEquals(null, segment.get(1, 114, null));
        assertEquals("three", segment.get(3, 301, null));
        assertEquals("five", segment.get(3, 302, null));

        assertTrue(segment.remove(3, 301));
        assertEquals(null, segment.get(3, 301, null));
        assertEquals("five", segment.get(3, 302, null));

        assertTrue(segment.remove(3, 302));
        assertEquals(null, segment.get(3, 302, null));
        assertEquals(0, segment.size());
    }
}
