/*
 * Copyright 2014 Peter Lawrey
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
package net.openhft.collections.shm;

import net.openhft.collections.SharedHashMap;
import net.openhft.collections.SharedHashMapBuilder;
import net.openhft.lang.values.LongValue;
import net.openhft.lang.values.LongValue£native;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * @author lburgazzoli
 */
public class LearningSharedHashMapTest {

    // *************************************************************************
    //
    // *************************************************************************

    @Test
    public void testAcquire() throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        int entries = 10;
        SharedHashMap<CharSequence, LongValue> map = getSharedMap(entries, 128, 24);

        LongValue value1 = new LongValue£native();

        CharSequence user = getUserCharSequence(0);
        map.acquireUsing(user, value1);
        map.getUsing(user, value1);

        map.close();
    }

    // *************************************************************************
    //
    // *************************************************************************

    private CharSequence getUserCharSequence(int i) {
        return new StringBuilder().append("user:").append(i);
    }

    private static SharedHashMap<CharSequence, LongValue> getSharedMap(long entries, int segments, int entrySize) throws IOException {
        return new SharedHashMapBuilder()
            .entries(entries)
            .segments(segments)
            .entrySize(entrySize)
            .create(
                getPersistenceFile(),
                CharSequence.class,
                LongValue.class);
    }

    private static File getPersistenceFile() {
        File file = new File(System.getProperty("java.io.tmpdir"),"hft-collections-test-shm");
        file.delete();
        file.deleteOnExit();

        return file;
    }
}
