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
    public void testPutReplace() {
        HugeConfig config = HugeConfig.SMALL.clone();
        config.setSegments(1);
        config.setSmallEntrySize(32);
        config.setCapacity(config.getSegments() * 32);
        HugeHashMap.Segment<Integer, String> segment = new HugeHashMap.Segment<Integer, String>(config, null, false, false, String.class);
        segment.put(1, 111, "one", true, true);
        segment.put(1, 112, "two", true, true);
        assertEquals(2, segment.size());
        segment.put(1, 111, "one-one", false, true);
        assertEquals("one", segment.get(1, 111, null));
        segment.put(1, 111, "one-one", true, false);
        assertEquals("one-one", segment.get(1, 111, null));
        segment.put(1, 113, "four", true, false);
        assertEquals(null, segment.get(1, 113, null));
        segment.put(1, 113, "four", false, true);
        assertEquals("four", segment.get(1, 113, null));

    }

    @Test
    public void testSegment() {
        HugeConfig config = HugeConfig.SMALL.clone();
        config.setSegments(1);
        config.setSmallEntrySize(32);
        config.setCapacity(config.getSegments() * 32);
        HugeHashMap.Segment<Integer, String> segment = new HugeHashMap.Segment<Integer, String>(config, null, false, false, String.class);
        segment.put(1, 111, "one", true, true);
        segment.put(1, 112, "two", true, true);
        segment.put(3, 301, "three", true, true);
        segment.put(1, 113, "four", true, true);
        segment.put(3, 302, "five", true, true);
        segment.put(1, 114, "six", true, true);
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
