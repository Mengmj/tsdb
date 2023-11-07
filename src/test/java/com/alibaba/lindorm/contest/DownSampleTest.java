package com.alibaba.lindorm.contest;

import com.alibaba.lindorm.contest.structs.*;
import com.alibaba.lindorm.contest.test.RandomUtils;
import com.alibaba.lindorm.contest.test.TestUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class DownSampleTest {
    public static void main(String[] args) throws IOException {
        File rootPath = new File(TestUtils.TEST_DIR,"downSampleTest");
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
        long beginTime = 1694078735906L;
        List<Row> rows = randomUtils.randomRows(vin,beginTime,TestUtils.TEST_SCHEMA,180);
        db.write(new WriteRequest(tableName,rows));
        db.shutdown();
        db = new TSDBEngineImpl(rootPath);
        db.connect();
        CompareExpression filter = new CompareExpression(new ColumnValue.IntegerColumn(-100), CompareExpression.CompareOp.GREATER);
        TimeRangeDownsampleRequest dsr = new TimeRangeDownsampleRequest(tableName,vin,"int_10",beginTime,beginTime+180*1000,Aggregator.MAX,30000,filter);
        List<Row> result = db.executeDownsampleQuery(dsr);
        System.out.println(result);
        db.shutdown();
    }
}
