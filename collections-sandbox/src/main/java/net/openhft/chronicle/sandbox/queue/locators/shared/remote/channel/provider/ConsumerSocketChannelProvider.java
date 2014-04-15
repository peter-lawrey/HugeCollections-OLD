package net.openhft.chronicle.sandbox.queue.locators.shared.remote.channel.provider;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by Rob Austin
 */
public class ConsumerSocketChannelProvider implements SocketChannelProvider {

    public static final int RECEIVE_BUFFER_SIZE = 256 * 1024;
    private static Logger LOG = Logger.getLogger(ConsumerSocketChannelProvider.class.getName());
    private final AtomicReference<SocketChannel> socketChannel = new AtomicReference<SocketChannel>();
    private final CountDownLatch latch = new CountDownLatch(1);

    public ConsumerSocketChannelProvider(final int port, @NotNull final String host) {

        new Thread(new Runnable() {
            @Override
            public void run() {

                SocketChannel result = null;
                try {
                    result = SocketChannel.open(new InetSocketAddress(host, port));
                    result.socket().setReceiveBufferSize(RECEIVE_BUFFER_SIZE);

                    socketChannel.set(result);
                    latch.countDown();
                } catch (Exception e) {
                    LOG.log(Level.SEVERE, "", e);
                    if (result != null)
                        try {
                            result.close();
                        } catch (IOException e1) {
                            e1.printStackTrace();
                        }
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
