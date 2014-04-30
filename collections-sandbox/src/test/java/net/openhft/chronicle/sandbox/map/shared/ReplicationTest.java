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

import net.openhft.collections.*;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * @author Rob Austin.
 */
public class ReplicationTest {

    private static File getPersistenceFile() {
        String TMP = System.getProperty("java.io.tmpdir");
        File file = new File(TMP + "/shm-test" + System.nanoTime());
        file.delete();
        file.deleteOnExit();
        return file;
    }

    SharedHashMap<Integer, CharSequence> newShmIntString(int size, final SegmentModificationIterator segmentModificationIterator) throws IOException {

        final VanillaSharedReplicatedHashMapBuilder builder = new VanillaSharedReplicatedHashMapBuilder()
                .entries(size)
                .eventListener(segmentModificationIterator);


        final VanillaSharedReplicatedHashMap<Integer, CharSequence> result = builder.create(getPersistenceFile(), Integer.class, CharSequence.class);

        segmentModificationIterator.setSegmentInfoProvider(result);


        // final CharSequence result = map.put(1, "one");

        final BlockingQueue<byte[]> input = new ArrayBlockingQueue<byte[]>(100);
        //final Queue<byte[]> output = new ConcurrentLinkedQueue<byte[]>();
        final Executor e = Executors.newFixedThreadPool(2);

        new QueueBasedReplicator(result, segmentModificationIterator, input, input, e, builder.alignment(), builder.entrySize());

        return result;

    }

    @Test
    @Ignore
    public void test() throws IOException, InterruptedException {


        final SharedHashMap<Integer, CharSequence> map = newShmIntString(10, new SegmentModificationIterator());

        map.put(1, "EXAMPLE");

        Thread.sleep(10000);
    }


}
