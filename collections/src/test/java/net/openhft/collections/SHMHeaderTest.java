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

import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * Created by win7 on 08/03/14.
 */
public class SHMHeaderTest {


    // added added an @Ignore as it take 5 mins to run
    @Test
    @Ignore
    public void testDifferentHeaders() throws IOException {
        Random rand = new Random(1);
        for (int i = 1; i <= 1000; i++) {
            rand.setSeed(i);
//            System.out.println("i: " + i);
            File file = new File(System.getProperty("java.io.tmpdir"), "headers-" + i);
            file.deleteOnExit();
            SharedHashMapBuilder builder1 = createBuilder(rand);
            SharedHashMap<String, String> map = builder1.create(file, String.class, String.class);
            // this is the sanitized builder
            SharedHashMapBuilder builder2 = builder1.clone();
            map.close();
            // on reopening
            SharedHashMapBuilder builder3 = createBuilder(rand);
            SharedHashMap<String, String> map2 = builder3.create(file, String.class, String.class);
            // this is the sanitized builder
            SharedHashMapBuilder builder4 = builder3.clone();
            assertEquals(builder2.toString(), builder4.toString());
            assertEquals(builder2, builder4);
            map2.close();
            // delete now if possible.
            file.delete();
        }
    }

    private static SharedHashMapBuilder createBuilder(Random rand) {
        return new SharedHashMapBuilder()
                .entrySize(rand.nextInt(100) * 3 + 1)
                .entries(rand.nextInt(1000) * 1111 + 1)
                .minSegments(rand.nextInt(200) + 1)
                .replicas(rand.nextInt(3));
    }
}
