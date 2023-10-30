package com.alibaba.lindorm.contest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class PoolTest {
    static class Task implements Callable<Integer> {

        @Override
        public Integer call() throws Exception {
            int size = 1024*1024;
            byte[] bytes = new byte[size];
            for(int i = 0;i < size;++i){
                bytes[i] = (byte)i;
            }
            return size;
        }
    }
    public static void main(String[] args) throws Exception{
        ExecutorService pool = Executors.newFixedThreadPool(8);
        int taskNum = 80*800;
        List<Future<Integer>> futureList = new ArrayList<>();
        long t1 = System.currentTimeMillis();
        for(int i = 0;i < taskNum;++i){
            futureList.add(pool.submit(new Task()));
        }
        for(int i = 0;i < taskNum;++i){
            int r = futureList.get(i).get();
            //System.out.println(r);
        }
        long t2 = System.currentTimeMillis();
        System.out.printf("%dms",t2-t1);
        pool.shutdown();

    }
}
