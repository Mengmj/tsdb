package com.alibaba.lindorm.contest;

import com.alibaba.lindorm.contest.structs.*;
import com.alibaba.lindorm.contest.test.RandomUtils;
import com.alibaba.lindorm.contest.test.TestUtils;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AggTest {
    public static void main(String[] args) throws IOException {
        File rootPath = new File(TestUtils.TEST_DIR,"aggQuery");
        if(rootPath.exists()){
            TestUtils.cleanDir(rootPath);
        }else {
            rootPath.mkdirs();
        }
        TSDBEngine db = new TSDBEngineImpl(rootPath);
        String tableName = "testTable";
        db.connect();
        db.createTable(tableName,TestUtils.TEST_SCHEMA);
        RandomUtils randomUtils = new RandomUtils();
        Vin vin = randomUtils.randomVin();
        long beginTime = 1699158600000L;
        List<Row> rows = randomUtils.randomRows(vin,beginTime,TestUtils.TEST_SCHEMA,10800);
        db.write(new WriteRequest(tableName,rows));
        db.shutdown();
        db = new TSDBEngineImpl(rootPath);
        db.connect();

        TimeRangeAggregationRequest aRequest = new TimeRangeAggregationRequest(tableName,vin,"int_11",beginTime-10*1000,beginTime+3600*1000,Aggregator.AVG);
        List<Row> result = db.executeAggregateQuery(aRequest);
        System.out.println(result);
        db.shutdown();
    }
}
