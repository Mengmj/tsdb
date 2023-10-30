package com.alibaba.lindorm.contest;

import java.util.concurrent.atomic.AtomicInteger;

public class AtomicIIntegerTest {
    public static void main(String[] args) throws InterruptedException{
        int nThread = 160;
        Thread[] threads = new Thread[nThread];
        AtomicInteger count = new AtomicInteger(0);
        long t1 = System.currentTimeMillis();
        for(int i = 0;i <nThread;++i){
            threads[i] = new Thread(new Task(count));
            threads[i].start();
        }
        for(int i = 0;i < nThread;++i){
            threads[i].join();
        }
        long t2 = System.currentTimeMillis();
        System.out.println(count.get());
        System.out.printf("%ds\n",(t2-t1)/1000);
    }
}
class Task implements Runnable{
    AtomicInteger count;
    public Task(AtomicInteger count){
        this.count = count;
    }
    @Override
    public void run() {
        for(int i = 0;i < 1000000;++i){
            count.incrementAndGet();
        }
    }
}
