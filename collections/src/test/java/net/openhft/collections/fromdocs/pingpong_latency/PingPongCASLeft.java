package net.openhft.collections.fromdocs.pingpong_latency;

import net.openhft.affinity.AffinitySupport;
import net.openhft.collections.SharedHashMap;
import net.openhft.collections.SharedHashMapBuilder;
import net.openhft.collections.fromdocs.BondVOInterface;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import static net.openhft.lang.model.DataValueClasses.newDirectReference;

/* on an i7-4500 laptop.

PingPongLEFT: Timing 1 x off-heap operations on /dev/shm/RDR_DIM_Mock
#0:  compareAndSwapCoupon() 50/90/99%tile was 41 / 52 / 132
#1:  compareAndSwapCoupon() 50/90/99%tile was 40 / 56 / 119
#2:  compareAndSwapCoupon() 50/90/99%tile was 39 / 48 / 54
#3:  compareAndSwapCoupon() 50/90/99%tile was 39 / 48 / 56
#4:  compareAndSwapCoupon() 50/90/99%tile was 39 / 48 / 54
#5:  compareAndSwapCoupon() 50/90/99%tile was 39 / 48 / 54
#6:  compareAndSwapCoupon() 50/90/99%tile was 39 / 48 / 55
#7:  compareAndSwapCoupon() 50/90/99%tile was 39 / 48 / 55
#8:  compareAndSwapCoupon() 50/90/99%tile was 39 / 48 / 54
#9:  compareAndSwapCoupon() 50/90/99%tile was 39 / 48 / 54

 */
public class PingPongCASLeft {
    public static void main(String... ignored) throws IOException {
        SharedHashMap<String, BondVOInterface> shm = PingPongCASLeft.acquireSHM();

        playPingPong(shm, 4, 5, true);
    }

    static void playPingPong(SharedHashMap<String, BondVOInterface> shm, double _coupon, double _coupon2, boolean setFirst) {
        BondVOInterface bond1 = newDirectReference(BondVOInterface.class);
        BondVOInterface bond2 = newDirectReference(BondVOInterface.class);
        BondVOInterface bond3 = newDirectReference(BondVOInterface.class);
        BondVOInterface bond4 = newDirectReference(BondVOInterface.class);

        shm.acquireUsing("369604101", bond1);
        shm.acquireUsing("369604102", bond2);
        shm.acquireUsing("369604103", bond3);
        shm.acquireUsing("369604104", bond4);
        System.out.printf("\n\nPingPongLEFT: Timing 1 x off-heap operations on /dev/shm/RDR_DIM_Mock\n");
        if (setFirst) {
            bond1.setCoupon(_coupon);
            bond2.setCoupon(_coupon);
            bond3.setCoupon(_coupon);
            bond4.setCoupon(_coupon);
        }
        int timeToCallNanoTime = 30;
        int runs = 1000000;
        long[] timings = new long[runs];
        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < runs; i++) {
                long _start = System.nanoTime(); //
                while (!bond1.compareAndSwapCoupon(_coupon, _coupon2)) ;
                while (!bond2.compareAndSwapCoupon(_coupon, _coupon2)) ;
                while (!bond3.compareAndSwapCoupon(_coupon, _coupon2)) ;
                while (!bond4.compareAndSwapCoupon(_coupon, _coupon2)) ;

                timings[i] = (System.nanoTime() - _start - timeToCallNanoTime) / 4;
            }
            Arrays.sort(timings);
            System.out.printf("#%d:  compareAndSwapCoupon() 50/90/99%%tile was %,d / %,d / %,d%n",
                    j, timings[runs / 2], timings[runs * 9 / 10], timings[runs * 99 / 100]);
        }
    }

    static SharedHashMap<String, BondVOInterface> acquireSHM() throws IOException {
        // ensure thread ids are globally unique.
        AffinitySupport.setThreadId();

        String TMP = System.getProperty("java.io.tmpdir");
        return new SharedHashMapBuilder()
                .generatedValueType(true)
                .entries(16)
                .entrySize(64)
                .create(
                        new File(TMP + "/BondPortfolioSHM"),
                        String.class,
                        BondVOInterface.class
                );
    }
}