package net.openhft.chronicle.sandbox.queue.ClientServerTest;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

/**
 * Created by Rob Austin
 */
public class SimpleClientServerExample {

    private static Logger LOG = Logger.getLogger(SimpleClientServerExample.class.getName());


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
