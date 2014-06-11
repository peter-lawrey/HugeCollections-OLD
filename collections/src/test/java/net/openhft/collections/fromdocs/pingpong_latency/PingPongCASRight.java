package net.openhft.collections.fromdocs.pingpong_latency;

import net.openhft.collections.SharedHashMap;
import net.openhft.collections.SharedHashMapBuilder;
import net.openhft.collections.fromdocs.BondVOInterface;

import java.io.File;
import java.io.IOException;

import static net.openhft.collections.fromdocs.pingpong_latency.PingPongCASLeft.playPingPong;

public class PingPongCASRight {
    public static void main(String... ignored) throws IOException {
        SharedHashMap<String, BondVOInterface> shm = PingPongCASLeft.acquireSHM();

        playPingPong(shm, 5, 4, false);
    }
}