package com.alibaba.lindorm.contest;

import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.TimeRangeQueryRequest;
import com.alibaba.lindorm.contest.structs.Vin;
import com.alibaba.lindorm.contest.structs.WriteRequest;
import com.alibaba.lindorm.contest.test.RandomUtils;
import com.alibaba.lindorm.contest.test.TestUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class WriteSpeedTest {
    private static class WriteTest implements Runnable{
        private TSDBEngine db;
        private String tableName;
        WriteTest(TSDBEngine db,String tableName){
            this.db = db;
            this.tableName = tableName;
        }
        @Override
        public void run(){
            RandomUtils randomUtils = new RandomUtils();
            List<Vin> vinList = randomUtils.randomVins(625);
            long beginSecond = 1699372626000L;
            try {
                for(int i = 0;i < 600;++i){
                    long ts = (beginSecond+i)*1000+ randomUtils.nextInt(1000);
                    List<Row> rowList = randomUtils.randomRows(vinList,ts, TestUtils.TEST_SCHEMA);
                    db.write(new WriteRequest(tableName,rowList));
                    if((i+1)%200==0){
                        Row expected = rowList.get(100);
                        List<Row> read = db.executeTimeRangeQuery(new TimeRangeQueryRequest(tableName,expected.getVin(),null,expected.getTimestamp(),expected.getTimestamp()+1));
                        TestUtils.check(TestUtils.compareRow(expected,read.get(0)));
                    }
                }
            }catch (Exception e){
                throw new RuntimeException(e);
            }
        }
    }

    public static void main(String[] args) throws IOException{
        File rootPath = new File(TestUtils.TEST_DIR,"speedTest");
        if(rootPath.exists()){
            TestUtils.cleanDir(rootPath);
        }
        rootPath.mkdirs();
        String tableName = "testTable";
        TSDBEngine db = new TSDBEngineImpl(rootPath);
        db.connect();
        db.createTable(tableName,TestUtils.TEST_SCHEMA);
        List<Future<?>> futures = new ArrayList<>();
        int taskNum = 8;
        ExecutorService pool = Executors.newFixedThreadPool(taskNum);
        long t1 = System.currentTimeMillis();
        for(int i = 0;i < taskNum;++i){
            futures.add(pool.submit(new WriteTest(db,tableName)));
        }
        try {
            for(int i = 0;i < taskNum;++i){
                futures.get(i).get();
            }
        }catch (Exception e){
            throw new RuntimeException(e);
        }
        long t2 = System.currentTimeMillis();
        System.out.printf("%dms, %d/s\n",t2-t1,(long)5000*10*60*1000/(t2-t1));
        db.shutdown();
        pool.shutdown();

    }
}
