package com.pingcap.tikv;

import com.google.common.primitives.Longs;

import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zhaoziming on 2017/6/23.
 */
public class TikvTest implements Runnable{
    static  AtomicInteger count = new AtomicInteger();
    TikvClient client = new TikvClient("localhost:2379");
    CountDownLatch latch;
    int loopCount;
    public TikvTest(int loopCount,CountDownLatch latch){
        this.loopCount = loopCount;
        this.latch = latch;
    }

    Random rnd = new Random();
    public static void main(String[] args) {

        int tcount = 100;
        int loopCount =10000;
        ExecutorService pool = Executors.newFixedThreadPool(tcount);

        CountDownLatch latch = new CountDownLatch(tcount);
        long start = System.currentTimeMillis();
        for (int i = 0; i < tcount; i++) {
            TikvTest r = new TikvTest(loopCount,latch);
            pool.execute(r);
        }
        try {
            latch.await();
            //System.out.println("=-------------------------------------=");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        long end = System.currentTimeMillis();
        System.out.println("========================" + (tcount * loopCount) / ((end - start) / 1000.0)+ "tps");
    }


    @Override
    public void run() {
        for (int i = 0; i < loopCount; i++) {
            long v = rnd.nextLong() % 1000000;
            client.set(Long.toString(v), Long.toString(v + 1).getBytes());
            count.incrementAndGet();
        }
//        System.out.println(count.get());
        latch.countDown();
    }
}