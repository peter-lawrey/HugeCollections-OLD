package net.openhft.collections.fromdocs.pingpong_latency;

import net.openhft.collections.SharedHashMap;
import net.openhft.collections.SharedHashMapBuilder;
import net.openhft.collections.fromdocs.BondVOInterface;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static net.openhft.collections.fromdocs.pingpong_latency.PingPongPlayerLeft.playPingPong;

public class PingPongPlayerRight {

    @Test
    public void bondExample() throws IOException, InterruptedException {
        SharedHashMap<String, BondVOInterface> shmLeft = new SharedHashMapBuilder()
                .generatedValueType(true)
                .entrySize(320)
                .create(
                        new File("/dev/shm/BondPortfolioSHM"),
                        String.class,
                        BondVOInterface.class
                );

        playPingPong(shmLeft, 5, 4, false);
    }
}