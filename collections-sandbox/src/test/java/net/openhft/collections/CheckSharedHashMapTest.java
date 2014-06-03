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

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;


/**
 * @author Rob Austin.
 */
public class CheckSharedHashMapTest {

    @Test
    public void test() throws IOException {

        final SharedHashMap<Integer, CharSequence> delegate = new SharedHashMapBuilder()
                .entries(1000)
                .create(Builder.getPersistenceFile(), Integer.class, CharSequence.class);


        final CheckSharedHashMap<Integer, CharSequence> checkSharedHashMap = new CheckSharedHashMap<Integer, CharSequence>(delegate);

        assertEquals(checkSharedHashMap.size(), delegate.size());
        assertEquals(checkSharedHashMap.entrySet(), delegate.entrySet());
        assertEquals(checkSharedHashMap.values(), delegate.values());
        assertEquals(checkSharedHashMap.keySet(), delegate.keySet());

        // todo add the others


    }
}
