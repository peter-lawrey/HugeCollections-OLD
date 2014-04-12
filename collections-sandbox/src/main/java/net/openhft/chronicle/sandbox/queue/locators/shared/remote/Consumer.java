package net.openhft.chronicle.sandbox.queue.locators.shared.remote;

import net.openhft.chronicle.sandbox.queue.locators.RingIndex;
import net.openhft.chronicle.sandbox.queue.locators.shared.Index;
import net.openhft.chronicle.sandbox.queue.locators.shared.OffsetProvider;
import net.openhft.chronicle.sandbox.queue.locators.shared.SliceProvider;
import net.openhft.lang.io.ByteBufferBytes;
import org.jetbrains.annotations.NotNull;

import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Rob Austin
 * <p/>
 * A consumer is only used by the read thread
 * <p/>
 * Data is publish to the consumer by the other process, the consumer only has to
 * - sore the data that is publish over tcp
 * - update its index of the last write location
 */
public class Consumer<BYTES extends ByteBufferBytes> implements RingIndex {

    @NotNull
    private final RingIndex ringIndex;
    private final SocketWriter toPublisher;

    public Consumer(@NotNull final SocketChannel socketChannel,
                    @NotNull final RingIndex ringIndex,
                    @NotNull final SliceProvider<BYTES> sliceProvider,
                    @NotNull final OffsetProvider offsetProvider) {


        final Index index = new Index() {

            @Override
            public void setNextLocation(int index) {
                ringIndex.setWriterLocation(index);
            }

            @Override
            public int getProducerWriteLocation() {
                throw new UnsupportedOperationException();
            }
        };

        new SocketReader(index, sliceProvider.getWriterSlice().buffer(), socketChannel, offsetProvider);
        final ExecutorService producerService = Executors.newSingleThreadExecutor();
        toPublisher = new SocketWriter(producerService, socketChannel);
        this.ringIndex = ringIndex;
    }

    @Override
    public int getWriterLocation() {
        return ringIndex.getWriterLocation();
    }

    @Override
    public void setWriterLocation(int nextWriteLocation) {
        ringIndex.setWriterLocation(nextWriteLocation);

    }

    @Override
    public int getReadLocation() {
        return ringIndex.getReadLocation();
    }

    @Override
    public void setReadLocation(int nextReadLocation) {
        ringIndex.setReadLocation(nextReadLocation);
        toPublisher.writeNextLocation(nextReadLocation);
    }

    @Override
    public int getProducerWriteLocation() {
        return ringIndex.getProducerWriteLocation();
    }

    @Override
    public void setProducerWriteLocation(int nextWriteLocation) {
        ringIndex.setProducerWriteLocation(nextWriteLocation);
    }


}
