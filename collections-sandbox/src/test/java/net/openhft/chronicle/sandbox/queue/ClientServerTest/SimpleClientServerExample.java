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

package net.openhft.chronicle.sandbox.queue.ClientServerTest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;

/**
 * Created by Rob Austin
 */
public class SimpleClientServerExample {

    private static Logger LOG = LoggerFactory.getLogger(SimpleClientServerExample.class);


    public static void main(String... args) throws InterruptedException {

        final CountDownLatch latch = new CountDownLatch(1);

        Thread server = new Thread(new Runnable() {

            @Override
            public void run() {

                try {
                    final ServerSocketChannel serverSocket = ServerSocketChannel.open();
                    serverSocket.socket().bind(new InetSocketAddress(8095));
                    serverSocket.configureBlocking(true);

                    final SocketChannel socketChannel = serverSocket.accept();

                    final String message = "hello world";
                    final byte[] bytes = message.getBytes();

                    ByteBuffer buffer = ByteBuffer.wrap(bytes);


                    socketChannel.write(buffer);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        server.setName("Server");
        server.start();

        Thread.sleep(100);

        Thread client = new Thread(new Runnable() {

            @Override
            public void run() {

                try {

                    final SocketChannel socketChannel = SocketChannel.open(new InetSocketAddress("localhost", 8095));
                    socketChannel.socket().setReceiveBufferSize(256 * 1024);

                    final int size = "hello world".length();
                    byte[] dest = new byte[size];
                    final ByteBuffer buffer = ByteBuffer.allocateDirect(size);

                    while (buffer.position() < size) {
                        socketChannel.read(buffer);
                    }
                    buffer.flip();
                    buffer.get(dest);

                    System.out.println("read = " + new String(dest));

                    latch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        client.setName("client");
        client.start();

        latch.await();

    }
}
