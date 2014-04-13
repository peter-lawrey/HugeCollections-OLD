package net.openhft.chronicle.sandbox.queue.locators.shared.remote.channel.provider;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by Rob Austin
 */
public class ProducerSocketChannelProvider implements SocketChannelProvider {

    public static final int RECEIVE_BUFFER_SIZE = 256 * 1024;
    private static Logger LOG = Logger.getLogger(ProducerSocketChannelProvider.class.getName());
    private final AtomicReference<SocketChannel> socketChannel = new AtomicReference<SocketChannel>();
    private final CountDownLatch latch = new CountDownLatch(1);

    public ProducerSocketChannelProvider(final int port) {

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    ServerSocketChannel serverSocket = ServerSocketChannel.open();
                    serverSocket.socket().setReuseAddress(true);
                    serverSocket.socket().bind(new InetSocketAddress(port));
                    serverSocket.configureBlocking(true);
                    LOG.info("Server waiting for client on port " + port);
                    serverSocket.socket().setReceiveBufferSize(RECEIVE_BUFFER_SIZE);
                    final SocketChannel result = serverSocket.accept();

                    socketChannel.set(result);
                    latch.countDown();
                } catch (Exception e) {
                    LOG.log(Level.SEVERE, "", e);
                }
            }
        }).start();
    }


    @Override
    public SocketChannel getSocketChannel() throws IOException, InterruptedException {

        final SocketChannel result = socketChannel.get();
        if (result != null)
            return result;

        latch.await();
        return socketChannel.get();
    }
}
