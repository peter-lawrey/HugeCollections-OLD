/**
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
package net.openhft.collections.replication;

import java.io.File;

/**
 * @author Rob Austin.
 */
public class SegmentModificationIteratorTest {


    static File getPersistenceFile() {
        String TMP = System.getProperty("java.io.tmpdir");
        File file = new File(TMP + "/shm-test" + System.nanoTime());
        file.delete();
        file.deleteOnExit();
        return file;
    }


   /* private static SharedHashMap<CharSequence, LongValue> getSharedMap(
            long entries, int segments, int entrySize, Alignment alignment, SharedMapEventListener eventListener)
            throws IOException {
        return new SharedHashMapBuilder()
                .entries(entries)
                .minSegments(segments)
                .entrySize(entrySize)
                .entryAndValueAlignment(alignment)
                .generatedValueType(true)
                .eventListener(eventListener)
                .create(getPersistenceFile(), CharSequence.class, LongValue.class);
    }

    @Test
    public void testUsingHasNext() throws IOException {


        final SegmentModificationIterator segmentModificationIterator = new SegmentModificationIterator(PUT);
        final SharedHashMap<CharSequence, LongValue> sharedMap = getSharedMap(5, 128, 24, NO_ALIGNMENT, segmentModificationIterator);

        segmentModificationIterator.setSegmentInfoProvider(sharedMap);

        final LongValue value = DataValueClasses.newInstance(LongValue.class);
        final int initialCapacity = 1000;
        final HashSet<String> expectedKeys = new HashSet<String>(initialCapacity);

        for (int i = 1; i < initialCapacity; i++) {
            final String key = Integer.toString(i);
            expectedKeys.add(key);
            sharedMap.put(key, value);
        }

        final HashSet actualKeys = new HashSet(expectedKeys.size());

        while (segmentModificationIterator.hasNext()) {
            final Map.Entry entry = segmentModificationIterator.nextEntry();
            actualKeys.add(entry.getKey());
        }

        assertEquals(expectedKeys, actualKeys);

        for (String key : expectedKeys) {
            sharedMap.put(key, value);
        }

        final HashSet actualKeys2 = new HashSet(expectedKeys.size());

        while (segmentModificationIterator.hasNext()) {
            final Map.Entry entry = segmentModificationIterator.nextEntry();
            actualKeys2.add(entry.getKey());
        }

        assertEquals(expectedKeys, actualKeys2);
    }


    @Test
    public void testWithoutHasNext() throws IOException {


        final SegmentModificationIterator segmentModificationIterator = new SegmentModificationIterator(null, PUT);
        final SharedHashMap<CharSequence, LongValue> sharedMap = getSharedMap(5, 128, 24, NO_ALIGNMENT, segmentModificationIterator);

        segmentModificationIterator.setSegmentInfoProvider( sharedMap);

        final LongValue value = DataValueClasses.newInstance(LongValue.class);
        final int initialCapacity = 1000;
        final HashSet<String> expectedKeys = new HashSet<String>(initialCapacity);

        for (int i = 1; i < initialCapacity; i++) {
            final String key = Integer.toString(i);
            expectedKeys.add(key);
            sharedMap.put(key, value);
        }

        final HashSet actualKeys = new HashSet(expectedKeys.size());

        for (; ; )
            try {
                final Map.Entry entry = segmentModificationIterator.nextEntry();
                actualKeys.add(entry.getKey());
            } catch (IllegalArgumentException e) {
                break;
            }


        assertEquals(expectedKeys, actualKeys);

        for (String key : expectedKeys) {
            sharedMap.put(key, value);
        }

        final HashSet actualKeys2 = new HashSet(expectedKeys.size());

        for (; ; )
            try {
                final Map.Entry entry = segmentModificationIterator.nextEntry();
                actualKeys2.add(entry.getKey());

            } catch (IllegalArgumentException e) {
                break;
            }

        assertEquals(expectedKeys, actualKeys2);
    }
*/

}




