package com.alibaba.lindorm.contest;

import com.alibaba.lindorm.contest.structs.*;
import com.alibaba.lindorm.contest.test.RandomUtils;
import com.alibaba.lindorm.contest.test.TestUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class WriteAndRead {
    public static void main(String[] args) throws IOException {
        writeAndDs();
    }
    private static void writeAndRead() throws IOException{
        RandomUtils randomUtils = new RandomUtils();
        List<Vin> vinList = randomUtils.randomVins(64);
        long time = 1699354800000L;
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
        db.shutdown();
        db.connect();
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
}
