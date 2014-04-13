package net.openhft.chronicle.sandbox.queue.common;


import net.openhft.chronicle.sandbox.queue.LocalConcurrentBlockingObjectQueue;
import net.openhft.chronicle.sandbox.queue.SharedConcurrentBlockingObjectQueue;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;

/**
 * Copyright 2013 Peter Lawrey
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
 *
 * @author Rob Austin
 */
@RunWith(value = Parameterized.class)
public class PutAndTakeTests {


    public final BlockingQueue<Integer> producerQueue;
    public final BlockingQueue<Integer> consumerQueue;

    public PutAndTakeTests(BlockingQueue<Integer> producerQueue, BlockingQueue<Integer> consumerQueue) {
        this.producerQueue = producerQueue;
        this.consumerQueue = consumerQueue;
    }

    @Parameterized.Parameters
    public static Collection<BlockingQueue<Integer>[]> data() throws IOException {

        final ArrayList<BlockingQueue<Integer>[]> result = new ArrayList<BlockingQueue<Integer>[]>();
        //local
        {
            BlockingQueue<Integer> queue = new LocalConcurrentBlockingObjectQueue<Integer>(1024);
            result.add(new BlockingQueue[]{queue, queue});
        }

        // shared
        {
            BlockingQueue<Integer> queue = new SharedConcurrentBlockingObjectQueue<Integer>(1024, Integer.class);
            result.add(new BlockingQueue[]{queue, queue});
        }

        // remote
      /*   {
            BlockingQueue<Integer> producerQueue = new ProducerConcurrentBlockingObjectQueue<Integer>(1024, Integer.class);
            BlockingQueue<Integer> consumerQueue = new ConsumerConcurrentBlockingObjectQueue<Integer>(1024, Integer.class,"localhost");
            result.add(new BlockingQueue[]{producerQueue, consumerQueue});
        }
        */
        return result;

    }

    /**
     * reader and add, reader and writers on different threads
     *
     * @throws Exception
     */
    @Test
    public void testWithFasterReader() throws Exception {


        final int max = 100;
        final CountDownLatch countDown = new CountDownLatch(1);

        final AtomicBoolean success = new AtomicBoolean(true);

        Thread writerThread = new Thread(
                new Runnable() {

                    @Override
                    public void run() {
                        for (int i = 1; i < max; i++) {
                            try {
                                producerQueue.put(i);
                                Thread.sleep((int) (Math.random() * 10));
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }

                    }
                }
        );


        writerThread.setName("writer");

        Thread readerThread = new Thread(
                new Runnable() {

                    @Override
                    public void run() {

                        int value = 0;
                        for (int i = 1; i < max; i++) {

                            try {
                                final int newValue = consumerQueue.take();

                                assertEquals(i, newValue);


                                if (newValue != value + 1) {
                                    success.set(false);
                                    return;
                                }

                                value = newValue;
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        countDown.countDown();

                    }
                }
        );

        readerThread.setName("reader");

        writerThread.start();
        readerThread.start();

        countDown.await();
        Assert.assertTrue(success.get());
    }


    /**
     * faster writer
     *
     * @throws Exception
     */
    @Test
    public void testWithFasterWriter() throws Exception {

        final int max = 200;
        final CountDownLatch countDown = new CountDownLatch(1);
        final AtomicBoolean success = new AtomicBoolean(true);

        Thread writerThread = new Thread(
                new Runnable() {

                    @Override
                    public void run() {
                        for (int i = 1; i < max; i++) {
                            try {
                                producerQueue.put(i);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }

                    }
                }
        );


        writerThread.setName("writer");

        Thread readerThread = new Thread(
                new Runnable() {

                    @Override
                    public void run() {

                        int value = 0;
                        for (int i = 1; i < max; i++) {

                            try {
                                final int newValue = consumerQueue.take();

                                assertEquals(i, newValue);


                                if (newValue != value + 1) {
                                    success.set(false);
                                    return;
                                }

                                value = newValue;


                                Thread.sleep((int) (Math.random() * 10));
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        countDown.countDown();

                    }
                }
        );

        readerThread.setName("reader");

        writerThread.start();
        readerThread.start();

        countDown.await();
        Assert.assertTrue(success.get());
    }




  /*  public void testLatency() throws NoSuchFieldException, InterruptedException, IOException {


        for (int pwr = 2; pwr < 200; pwr++) {
            int i = (int) Math.pow(2, pwr);


            final long arrayBlockingQueueStart = System.nanoTime();
            testArrayBlockingQueue(i);
            final double arrayBlockingDuration = System.nanoTime() - arrayBlockingQueueStart;


            final long queueStart = System.nanoTime();
            testConcurrentBlockingObjectQueue(i);
            final double concurrentBlockingDuration = System.nanoTime() - queueStart;

            System.out.printf("Performing %,d loops, ArrayBlockingQueue() took %.3f ms and calling ConcurrentBlockingObjectQueue<Integer> took %.3f ms on average, ratio=%.1f%n",
                    i, arrayBlockingDuration / 1000000.0, concurrentBlockingDuration / 1000000.0, (double) arrayBlockingDuration / (double) concurrentBlockingDuration);
            *//**
     System.out.printf("%d\t%.3f\t%.3f\n",
     i, arrayBlockingDuration / 1000000.0, concurrentBlockingDuration / 1000000.0, (double) arrayBlockingDuration / (double) concurrentBlockingDuration);
     **//*
        }
*/


}
