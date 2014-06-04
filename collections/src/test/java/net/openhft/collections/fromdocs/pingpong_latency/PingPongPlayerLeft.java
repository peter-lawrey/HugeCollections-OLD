package net.openhft.collections.fromdocs.pingpong_latency;

import net.openhft.collections.SharedHashMap;
import net.openhft.collections.SharedHashMapBuilder;
import net.openhft.collections.fromdocs.BondVOInterface;
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
public class PingPongPlayerLeft {

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
        playPingPong(shmLeft, 4, 5, true);
    }

    static void playPingPong(SharedHashMap<String, BondVOInterface> shmLeft, double _coupon, double _coupon2, boolean setFirst) {
        BondVOInterface bondOffHeap1 = newDirectReference(BondVOInterface.class);
        BondVOInterface bondOffHeap2 = newDirectReference(BondVOInterface.class);
        BondVOInterface bondOffHeap3 = newDirectReference(BondVOInterface.class);
        BondVOInterface bondOffHeap4 = newDirectReference(BondVOInterface.class);

        shmLeft.acquireUsing("369604101", bondOffHeap1);
        shmLeft.acquireUsing("369604102", bondOffHeap2);
        shmLeft.acquireUsing("369604103", bondOffHeap3);
        shmLeft.acquireUsing("369604104", bondOffHeap4);
        System.out.printf("\n\nPingPongLEFT: Timing 1 x off-heap operations on /dev/shm/RDR_DIM_Mock\n");
        if (setFirst) {
            bondOffHeap1.setCoupon(_coupon);
            bondOffHeap2.setCoupon(_coupon);
            bondOffHeap3.setCoupon(_coupon);
            bondOffHeap4.setCoupon(_coupon);
        }
        int timeToCallNanoTime = 30;
        int runs = 1000000;
        long[] timings = new long[runs];
        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < runs; i++) {
                long _start = System.nanoTime(); //
                while (!bondOffHeap1.compareAndSwapCoupon(_coupon, _coupon2)) ;
                while (!bondOffHeap2.compareAndSwapCoupon(_coupon, _coupon2)) ;
                while (!bondOffHeap3.compareAndSwapCoupon(_coupon, _coupon2)) ;
                while (!bondOffHeap4.compareAndSwapCoupon(_coupon, _coupon2)) ;

                timings[i] = (System.nanoTime() - _start - timeToCallNanoTime) / 4;
            }
            Arrays.sort(timings);
            System.out.printf("#%d:  compareAndSwapCoupon() 50/90/99%%tile was %,d / %,d / %,d%n",
                    j, timings[runs / 2], timings[runs * 9 / 10], timings[runs * 99 / 100]);
        }
    }
}