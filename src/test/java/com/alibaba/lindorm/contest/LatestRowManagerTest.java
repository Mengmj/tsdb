package com.alibaba.lindorm.contest;

import com.alibaba.lindorm.contest.manager.LatestRowManager;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.Schema;
import com.alibaba.lindorm.contest.structs.Vin;
import com.alibaba.lindorm.contest.test.TestUtils;

import java.util.List;

public class LatestRowManagerTest {
    private static class Task implements Runnable{
        List<Vin> vinList;
        LatestRowManager latestRowManager;
        public Task(List<Vin> vinList,LatestRowManager latestRowManager){
            this.vinList = vinList;
            this.latestRowManager = latestRowManager;
        }
        @Override
        public void run() {
            Schema schema = TestUtils.TEST_SCHEMA;
            for(int i = 0;i < 10000;++i){
                List<Row> rows = TestUtils.emptyRows(vinList,System.currentTimeMillis(),schema);
                for(Row row:rows){
                    latestRowManager.upsert(row);
                }
            }
        }
    }

    public static void main(String[] args) throws InterruptedException{
        int nThread = 80;
        List<Vin> vinList = TestUtils.randomVins(10000);
        LatestRowManager latestRowManager = LatestRowManager.getInstance();
        Thread[] threads = new Thread[nThread];
        long t1 = System.currentTimeMillis();
        for(int i = 0;i < nThread;++i){
            threads[i] = new Thread(new Task(vinList,latestRowManager));
            threads[i].start();
        }
        for(int i = 0;i < nThread;++i){
            threads[i].join();
        }
        long t2 = System.currentTimeMillis();
        System.out.printf("%dms\n",t2-t1);
    }
}
