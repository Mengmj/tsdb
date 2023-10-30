package com.alibaba.lindorm.contest;

import com.alibaba.lindorm.contest.manager.IdManager;
import com.alibaba.lindorm.contest.structs.Vin;
import com.alibaba.lindorm.contest.test.TestUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class IdManagerTest {
    static class Task implements Runnable{
        IdManager idManager;
        List<Vin> vinList;
        Set<Integer> idSet;
        public Task(IdManager idManager){
            this.idManager = idManager;
            vinList = TestUtils.randomVins(10000);
        }
        @Override
        public void run() {
            for(Vin vin:vinList){
                int id = idManager.getId(vin,true);
            }
            for(int i = 0;i < 10;++i){
                for(Vin vin:vinList){
                    int id = idManager.getId(vin,true);
                }
            }
        }
    }

    public static void main(String[] args) throws InterruptedException{
        int nThread = 10;
        IdManager idManager = IdManager.getInstance();
        Thread[] threads = new Thread[nThread];
        long t1 = System.currentTimeMillis();
        for(int i = 0;i < nThread;++i){
            threads[i] = new Thread(new Task(idManager));
            threads[i].start();
        }
        for(int i = 0;i < nThread;++i){
            threads[i].join();
        }
        long t2 = System.currentTimeMillis();
        System.out.printf("%dms",(t2-t1));
    }
}

