package net.openhft.collections.fromdocs.pingpong_latency;

import net.openhft.collections.SharedHashMap;
import net.openhft.collections.SharedHashMapBuilder;
import net.openhft.collections.fromdocs.BondVOInterface;
import net.openhft.lang.model.DataValueClasses;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static net.openhft.lang.model.DataValueClasses.*;

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
        for (int i = 0; i < 500000; i++) {
            long _start = System.nanoTime(); //
            while (!bondOffHeap1.compareAndSwapCoupon(_coupon, _coupon2)) ;
            while (!bondOffHeap2.compareAndSwapCoupon(_coupon, _coupon2)) ;
            while (!bondOffHeap3.compareAndSwapCoupon(_coupon, _coupon2)) ;
            while (!bondOffHeap4.compareAndSwapCoupon(_coupon, _coupon2)) ;

            long _duration = System.nanoTime() - _start;
            System.out.printf("#%d:  1 x _bondEntryV.getCoupon() (last _coupon=[%.2f %%]) in %,d nanos\n",
                    i, _coupon, _duration / 4);
        }
    }
}