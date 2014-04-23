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

package net.openhft.chronicle.sandbox.queue.locators;

import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by Rob Austin
 */
//@RunWith(value = Parameterized.class)
public class SharedRingIndexTest extends TestCase {


  /*  @Parameterized.Parameters
    public static Collection<Object[]> data() {
        int capacityInBytes = 256 / 8;
        return Arrays.asList(new Object[][]{
                {
                        new ATSDirectBitSet(new ByteBufferBytes(
                                ByteBuffer.allocate(capacityInBytes)))
                },
                {
                        new SingleThreadedDirectBitSet(new ByteBufferBytes(
                                ByteBuffer.allocate(capacityInBytes)))
                }
        });
    }*/


    @Test
    public void test() {
        // todo
        Assert.assertTrue(true);
    }

}
