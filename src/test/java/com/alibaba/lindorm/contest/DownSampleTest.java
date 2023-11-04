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
        long beginTime0 = 1698994560000L;
        List<Row> rows0 = randomUtils.randomRows(vin,beginTime0,TestUtils.TEST_SCHEMA,10);
        db.write(new WriteRequest(tableName,rows0));
        long beginTime = 1698994620000L;
        List<Row> rows = randomUtils.randomRows(vin,beginTime,TestUtils.TEST_SCHEMA,150);
        db.write(new WriteRequest(tableName,rows));
        db.shutdown();
        db = new TSDBEngineImpl(rootPath);
        db.connect();
        CompareExpression filter = new CompareExpression(new ColumnValue.IntegerColumn(1), CompareExpression.CompareOp.EQUAL);
        TimeRangeDownsampleRequest dsr = new TimeRangeDownsampleRequest(tableName,vin,"int_10",beginTime-10*1000,beginTime+170*1000,Aggregator.AVG,30000,filter);
        List<Row> result = db.executeDownsampleQuery(dsr);
        System.out.println(result);
        db.shutdown();
    }
}
