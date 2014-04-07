package net.openhft.chronicle.sandbox.queue.locators.shared.replication;

import net.openhft.lang.model.constraints.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by Rob Austin
 */
public class Connection implements Closeable {

    private static Logger LOG = Logger.getLogger(Connection.class.getName());
    private final String hostname;
    private final int port;
    private SocketChannel socketChannel = null;
    private volatile boolean closed = false;

    Connection(String hostname, int port) {
        this.hostname = hostname;
        this.port = port;
    }

    public SocketChannel getSocket() {
        return socketChannel;
    }

    @Override
    public void close() throws IOException {
        if (socketChannel != null)
            socketChannel.close();
        closed = true;
    }


    @Nullable
    SocketChannel openSocket() {
        while (!closed) {
            try {
                InetSocketAddress address = new InetSocketAddress(hostname, port);
                socketChannel = SocketChannel.open(address);
                socketChannel.socket().setReceiveBufferSize(256 * 1024);
                LOG.info("Connected to " + address);
                return socketChannel;
            } catch (IOException e) {
                if (LOG.isLoggable(Level.FINE))
                    LOG.log(Level.SEVERE, "Failed to connect to " + hostname + port + " retrying", e);
                else if (LOG.isLoggable(Level.INFO))
                    LOG.log(Level.INFO, "Failed to connect to " + hostname + port + " retrying " + e);
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
        }
        return null;
    }

    boolean isOpen() {
        return socketChannel != null && socketChannel.isOpen();
    }


}
