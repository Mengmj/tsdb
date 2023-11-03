package com.alibaba.lindorm.contest;

import com.alibaba.lindorm.contest.structs.*;
import com.alibaba.lindorm.contest.test.RandomUtils;
import com.alibaba.lindorm.contest.test.TestUtils;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
        long beginTime = 1698994650000L;
        List<Row> rows = randomUtils.randomRows(vin,beginTime,TestUtils.TEST_SCHEMA,600);
        db.write(new WriteRequest(tableName,rows));
        db.shutdown();
        db = new TSDBEngineImpl(rootPath);
        db.connect();
        CompareExpression filter = new CompareExpression(new ColumnValue.IntegerColumn(100), CompareExpression.CompareOp.GREATER);
        TimeRangeDownsampleRequest dsr = new TimeRangeDownsampleRequest(tableName,vin,"int_10",beginTime,beginTime+600*1000,Aggregator.AVG,60000,filter);
        List<Row> result = db.executeDownsampleQuery(dsr);
        System.out.println(result);
        db.shutdown();
    }
}
