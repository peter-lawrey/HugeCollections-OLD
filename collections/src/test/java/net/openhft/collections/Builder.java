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


import net.openhft.lang.values.IntValue;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ArrayBlockingQueue;

import static net.openhft.collections.Replica.ModificationNotifier.NOP;

/**
 * @author Rob Austin.
 */
public class Builder {

    // added to ensure uniqueness
    static int count;
    static String WIN_OS = "WINDOWS";

    public static File getPersistenceFile() throws IOException {
        String TMP = System.getProperty("java.io.tmpdir");
        File file = new File(TMP + "/shm-test" + System.nanoTime() + (count++));

        //Not Guaranteed to work on Windows, since OS file-lock takes precedence
        if (System.getProperty("os.name").indexOf(WIN_OS) > 0) {
            /*Windows will lock a file that are currently in use. You cannot delete it, however,
              using setwritable() and then releasing RandomRW lock adds the file to JVM exit cleanup.
    		  This will only work if the user is an admin on windows.
    		*/
            file.setWritable(true);//just in case relative path was used.
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            raf.close();//allows closing the file access on windows. forcing to close access. Only works for admin-access.
        }

        //file.delete(); //isnt guaranteed on windows.
        file.deleteOnExit();//isnt guaranteed on windows.

        return file;
    }


    static VanillaSharedReplicatedHashMap<Integer, CharSequence> newShmIntString(
            int size, final ArrayBlockingQueue<byte[]> input,
            final ArrayBlockingQueue<byte[]> output, final byte localIdentifier, byte externalIdentifier) throws IOException {

        final SharedHashMapBuilder builder =
                new SharedHashMapBuilder()
                        .entries(size)
                        .identifier(localIdentifier);

        final VanillaSharedReplicatedHashMap<Integer, CharSequence> result = (VanillaSharedReplicatedHashMap<Integer, CharSequence>)
                builder.canReplicate(true).file(getPersistenceFile()).kClass(Integer.class).vClass(CharSequence.class).create();

        final Replica.ModificationIterator modificationIterator = result
                .acquireModificationIterator(externalIdentifier, NOP);
        new QueueReplicator(modificationIterator,
                input, output, builder.entrySize(), (Replica.EntryExternalizable) result);

        return result;

    }

    interface MapProvider<T> {
        T getMap();

        boolean isQueueEmpty();
    }

    static MapProvider<VanillaSharedReplicatedHashMap<Integer, Integer>> newShmIntInt(
            int size, final ArrayBlockingQueue<byte[]> input,
            final ArrayBlockingQueue<byte[]> output, final byte localIdentifier, byte externalIdentifier) throws IOException {

        final SharedHashMapBuilder builder =
                new SharedHashMapBuilder()
                        .entries(size)
                        .identifier(localIdentifier);

        final VanillaSharedReplicatedHashMap<Integer, Integer> result = (VanillaSharedReplicatedHashMap<Integer, Integer>)
                builder.canReplicate(true).file(getPersistenceFile()).kClass(Integer.class).vClass(Integer.class).create();


        final QueueReplicator q = new QueueReplicator(result.acquireModificationIterator(externalIdentifier, NOP),
                input, output, builder.entrySize(), (Replica.EntryExternalizable) result);

        return new MapProvider<VanillaSharedReplicatedHashMap<Integer, Integer>>() {

            @Override
            public VanillaSharedReplicatedHashMap<Integer, Integer> getMap() {
                return result;
            }

            @Override
            public boolean isQueueEmpty() {
                return q.isEmpty();
            }

        };


    }


    static Replica<IntValue, IntValue> newShmIntValueIntValue(
            int size, final ArrayBlockingQueue<byte[]> input,
            final ArrayBlockingQueue<byte[]> output, final byte localIdentifier, byte externalIdentifier) throws IOException {

        final SharedHashMapBuilder builder =
                new SharedHashMapBuilder()
                        .entries(size)
                        .identifier(localIdentifier);

        final Replica<IntValue, IntValue> result = (Replica<IntValue, IntValue>)
                builder.canReplicate(true).file(getPersistenceFile()).kClass(IntValue.class).vClass(IntValue.class).create();


        final QueueReplicator q = new QueueReplicator(result.acquireModificationIterator(externalIdentifier, NOP),
                input, output, builder.entrySize(), (Replica.EntryExternalizable) result);

        return result;


    }


    static SharedHashMap<CharSequence, CharSequence> newShmStringString(
            int size, final ArrayBlockingQueue<byte[]> input,
            final ArrayBlockingQueue<byte[]> output, final byte localIdentifier, byte externalIdentifier) throws IOException {

        final SharedHashMapBuilder builder =
                new SharedHashMapBuilder()
                        .entries(size)
                        .identifier(localIdentifier);

        final VanillaSharedReplicatedHashMap<CharSequence, CharSequence> result = (VanillaSharedReplicatedHashMap<CharSequence, CharSequence>)
                builder.canReplicate(true).file(getPersistenceFile()).kClass(CharSequence.class).vClass(CharSequence.class).create();


        final QueueReplicator q = new QueueReplicator(result.acquireModificationIterator(externalIdentifier, NOP),
                input, output, builder.entrySize(), (Replica.EntryExternalizable) result);

        return result;


    }

}
