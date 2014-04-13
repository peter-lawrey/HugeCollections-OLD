package net.openhft.chronicle.sandbox.queue.locators.shared.remote.channel.provider;

import java.io.IOException;
import java.nio.channels.SocketChannel;

/**
 * Created by Rob Austin
 */
public interface SocketChannelProvider {

    SocketChannel getSocketChannel() throws IOException, InterruptedException;
}
