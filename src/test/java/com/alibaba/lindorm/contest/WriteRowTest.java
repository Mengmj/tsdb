package com.alibaba.lindorm.contest;

import com.alibaba.lindorm.contest.manager.TSDBFileSystem;
import com.alibaba.lindorm.contest.structs.*;
import com.alibaba.lindorm.contest.test.RandomUtils;
import com.alibaba.lindorm.contest.test.TestUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class WriteRowTest {
    private static class Task implements Runnable{
        private TSDBEngine db;
        private List<Vin> vins;
        private long beginTime;
        private int n;
        private String tableName;
        private Schema schema;

        public Task(TSDBEngine db, List<Vin> vins, long beginTime, int n, String tableName, Schema schema){
            this.db = db;
            this.vins = vins;
            this.beginTime = beginTime;
            this.n = n;
            this.tableName = tableName;
            this.schema = schema;
        }
        @Override
        public void run() {
            Random random = new Random(System.currentTimeMillis());
            for(int i = 0;i < n;++i){
                List<Row>  rows = TestUtils.randomRows(vins,beginTime+i*1000,schema);
                try {
                    db.write(new WriteRequest(tableName,rows));
                }catch (IOException e){
                    throw new RuntimeException(e);
                }
                int idx = random.nextInt(rows.size());
                Row row = rows.get(idx);
                List<Vin> queriedVins = new ArrayList<>();
                queriedVins.add(row.getVin());
                LatestQueryRequest lqr = new LatestQueryRequest(tableName,queriedVins,new HashSet<>());
                try {
                    List<Row> ret = db.executeLatestQuery(lqr);
                    TestUtils.check(TestUtils.compareRow(ret.get(0),row));
                }catch (IOException e){
                    throw new RuntimeException(e);
                }
                LogUtils.debug("finish loop: %d\n",i);
            }
        }
    }

    public static void main(String[] args) {
        writeAndLatest();
    }
    public static void writeAndLatest(){
        String tableName = "testTable";
        Schema schema = TestUtils.TEST_SCHEMA;
        File rootPath = new File(TestUtils.TEST_DIR,"writeAndLatest");
        if(!rootPath.exists()){
            rootPath.mkdirs();
        }else {
            TestUtils.cleanDir(rootPath);
        }
        TSDBEngine db = new TSDBEngineImpl(rootPath);
        try {
            db.connect();
            db.createTable(tableName,schema);
        }catch (IOException e){
            throw new RuntimeException(e);
        }
        int n = 8;
        int vinNum = 100;
        List<List<Vin>> vinList = new ArrayList<>();
        List<Future<?>> futures = new ArrayList<>();
        RandomUtils randomUtils = new RandomUtils();
        ExecutorService pool = Executors.newFixedThreadPool(8);
        Set<Vin> vinSet = new HashSet<>();
        for(int i = 0;i < n;++i){
            List<Vin> vins = randomUtils.randomVins(vinNum);
            vinList.add(vins);
            vinSet.addAll(vins);
            futures.add(pool.submit(new Task(db,vins,1698905700000L,120,tableName,schema)));
        }
        try {
            for(int i = 0;i < n;++i){
                futures.get(i).get();
            }
        }catch (Exception e){
            throw new RuntimeException(e);
        }
        pool.shutdown();
        db.shutdown();
        db = null;
        try{
            db = new TSDBEngineImpl(rootPath);
            db.connect();
            LatestQueryRequest  lqr = new LatestQueryRequest(tableName,vinSet,new HashSet<>());
            List<Row> rows = db.executeLatestQuery(lqr);
            System.out.println(rows);
            db.shutdown();
        }catch (IOException e){
            throw new RuntimeException(e);
        }


    }
}
