package com.alibaba.lindorm.contest;

import com.alibaba.lindorm.contest.structs.*;
import com.alibaba.lindorm.contest.test.RandomUtils;
import com.alibaba.lindorm.contest.test.TestUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class WriteAndRead {
    private static class WriteAndReadTask implements Runnable{
        private TSDBEngine db;
        private String tableName;
        WriteAndReadTask(TSDBEngine db,String tableName){
            this.db = db;
            this.tableName = tableName;
        }
        @Override
        public void run() {
            RandomUtils randomUtils = new RandomUtils();
            List<Vin> vinList = randomUtils.randomVins(100);
            long time = 1699372626777L;
            ArrayList<Row> rows = new ArrayList<>();
            for(Vin vin:vinList){
                rows.addAll(randomUtils.randomRows(vin,time,TestUtils.TEST_SCHEMA,600));
            }
            try {
                db.write(new WriteRequest(tableName,rows));
                for(int i = 0;i < 64;++i){
                    List<Row> readRows = db.executeTimeRangeQuery(new TimeRangeQueryRequest(tableName,vinList.get(i),new HashSet<>(),time,time+3600*1000));
                    for(int j = 0;j < 600;++j){
                        Row read = readRows.get(j);
                        Row expected = rows.get(i*600+j);
                        TestUtils.check(TestUtils.compareRow(read,expected));
                    }
                }
            }catch (Exception e){
                throw new RuntimeException(e);
            }
        }
    }
    public static void main(String[] args) throws IOException {
        pickUpCheck();
    }
    private static void writeAndRead() throws IOException{
        RandomUtils randomUtils = new RandomUtils();
        List<Vin> vinList = randomUtils.randomVins(100);
        long time = 1699372626231L;
        ArrayList<Row> rows = new ArrayList<>();
        for(Vin vin:vinList){
            rows.addAll(randomUtils.randomRows(vin,time,TestUtils.TEST_SCHEMA,3600));
        }
        File rootPath = new File(TestUtils.TEST_DIR,"wnr");
        if(rootPath.exists()){
            TestUtils.cleanDir(rootPath);
        }
        rootPath.mkdirs();
        String tableName = "testTable";
        TSDBEngine db = new TSDBEngineImpl(rootPath);
        db.connect();
        db.createTable(tableName,TestUtils.TEST_SCHEMA);
        db.write(new WriteRequest(tableName,rows));
        for(int i = 0;i < 64;++i){
            List<Row> readRows = db.executeTimeRangeQuery(new TimeRangeQueryRequest(tableName,vinList.get(i),new HashSet<>(),time,time+3600*1000));
            for(int j = 0;j < 3600;++j){
                Row read = readRows.get(j);
                Row expected = rows.get(i*3600+j);
                TestUtils.check(TestUtils.compareRow(read,expected));
            }
        }
        db.shutdown();
        System.out.println(1);
    }

    private static void writeAndAgg() throws IOException{
        RandomUtils randomUtils = new RandomUtils();
        List<Vin> vinList = randomUtils.randomVins(64);
        long time = 1699354800000L;
        ArrayList<Row> rows = new ArrayList<>();
        for(Vin vin:vinList){
            rows.addAll(randomUtils.randomRows(vin,time,TestUtils.TEST_SCHEMA,3600));
        }
        File rootPath = new File(TestUtils.TEST_DIR,"wna");
        if(rootPath.exists()){
            TestUtils.cleanDir(rootPath);
        }
        rootPath.mkdirs();
        String tableName = "testTable";
        TSDBEngine db = new TSDBEngineImpl(rootPath);
        db.connect();
        db.createTable(tableName,TestUtils.TEST_SCHEMA);
        db.write(new WriteRequest(tableName,rows));
        db.shutdown();
        db.connect();
        ArrayList<Row> aggRows = db.executeAggregateQuery(new TimeRangeAggregationRequest(tableName,vinList.get(0),"double_10",time-100*1000,time+100*1000,Aggregator.MAX));
        db.shutdown();
        System.out.println(aggRows.get(0));
    }

    private static void writeAndDs() throws IOException{
        RandomUtils randomUtils = new RandomUtils();
        List<Vin> vinList = randomUtils.randomVins(64);
        long time = 1699354800000L;
        ArrayList<Row> rows = new ArrayList<>();
        for(Vin vin:vinList){
            rows.addAll(randomUtils.randomRows(vin,time,TestUtils.TEST_SCHEMA,3600));
        }
        File rootPath = new File(TestUtils.TEST_DIR,"wna");
        if(rootPath.exists()){
            TestUtils.cleanDir(rootPath);
        }
        rootPath.mkdirs();
        String tableName = "testTable";
        TSDBEngine db = new TSDBEngineImpl(rootPath);
        db.connect();
        db.createTable(tableName,TestUtils.TEST_SCHEMA);
        db.write(new WriteRequest(tableName,rows));
        db.shutdown();
        db.connect();
        TimeRangeDownsampleRequest dsr = new TimeRangeDownsampleRequest(tableName,vinList.get(0),"int_12",time,time+3600*1000,Aggregator.AVG,300000,new CompareExpression(new ColumnValue.IntegerColumn(-10), CompareExpression.CompareOp.GREATER));
        List<Row> result = db.executeDownsampleQuery(dsr);
        db.shutdown();
        System.out.println(1);
    }
    public static void pickUpCheck() throws IOException{
        RandomUtils randomUtils = new RandomUtils();
        List<Vin> vinList = randomUtils.randomVins(5000);
        long beginSecond = 1699372626L;
        File rootPath = new File(TestUtils.TEST_DIR,"wna");
        if(rootPath.exists()){
            TestUtils.cleanDir(rootPath);
        }
        rootPath.mkdirs();
        String tableName = "testTable";
        TSDBEngine db = new TSDBEngineImpl(rootPath);
        db.connect();
        db.createTable(tableName,TestUtils.TEST_SCHEMA);
        for(int i = 0;i < 600;++i){
            long ts = (beginSecond+i)*1000+ randomUtils.nextInt(1000);
            List<Row> rowList = randomUtils.randomRows(vinList,ts,TestUtils.TEST_SCHEMA);
            db.write(new WriteRequest(tableName,rowList));
            if((i+1)%200==0){
                Row expected = rowList.get(4999);
                List<Row> read = db.executeTimeRangeQuery(new TimeRangeQueryRequest(tableName,expected.getVin(),null,expected.getTimestamp(),expected.getTimestamp()+1));
                TestUtils.check(TestUtils.compareRow(expected,read.get(0)));
            }
            System.out.println(i);
        }
        db.shutdown();
    }
    public static void multiWrite() throws IOException{
        File rootPath = new File(TestUtils.TEST_DIR,"multiW");
        if(rootPath.exists()){
            TestUtils.cleanDir(rootPath);
        }
        rootPath.mkdirs();
        String tableName = "testTable";
        TSDBEngine db = new TSDBEngineImpl(rootPath);
        db.connect();
        db.createTable(tableName,TestUtils.TEST_SCHEMA);
        ExecutorService pool = Executors.newFixedThreadPool(8);
        int taskCount = 8;
        List<Future<?>> futures = new ArrayList<>();
        long t1 = System.currentTimeMillis();
        for(int i = 0;i < taskCount;++i){
            futures.add(pool.submit(new WriteAndReadTask(db,tableName)));
        }
        try {
            for (int i = 0;i < taskCount;++i){
                futures.get(i).get();
            }
        }catch (Exception e){
            throw new RuntimeException(e);
        }
        db.shutdown();
        long t2 = System.currentTimeMillis();
        System.out.printf("%dms\n",t2-t1);
        pool.shutdown();
    }
}
