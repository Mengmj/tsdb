package com.alibaba.lindorm.contest;

import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.TimeRangeQueryRequest;
import com.alibaba.lindorm.contest.structs.Vin;
import com.alibaba.lindorm.contest.structs.WriteRequest;
import com.alibaba.lindorm.contest.test.RandomUtils;
import com.alibaba.lindorm.contest.test.TestUtils;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RangeQueryTest {
    public static void main(String[] args) throws IOException{
        File rootPath = new File(TestUtils.TEST_DIR,"rangeQuery");
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
        List<Row> rows = randomUtils.randomRows(vin,beginTime,TestUtils.TEST_SCHEMA,60);
        db.write(new WriteRequest(tableName,rows));
        db.shutdown();
        db = new TSDBEngineImpl(rootPath);
        db.connect();
        Set<String> colNames = new HashSet<>();
//        colNames.add("int_12");
//        colNames.add("int_13");
        List<Row> readRows = db.executeTimeRangeQuery(new TimeRangeQueryRequest(tableName,vin,colNames,beginTime,beginTime+60*1000));
        TestUtils.check(rows.size()==readRows.size());
        for(int i = 0;i < rows.size();++i){
            TestUtils.check(TestUtils.compareRow(rows.get(i),readRows.get(i)));
        }
        for(Row row:readRows){
            System.out.println(row);
        }
        db.shutdown();
    }
}
