package com.alibaba.lindorm.contest;

import com.alibaba.lindorm.contest.structs.*;

import java.io.File;
import java.util.*;

public class FlinkTest {
    public static void main(String[] args) throws Exception{
        //连接数据库
        File dataPath = new File("/data/home/mingjinmeng/playground/test_data/flink-test");
        TSDBEngine db = new TSDBEngineImpl(dataPath);
        db.connect();

        //建表
        String tableName = "table0";
        // createTable(db);
        String vinStr = "abcdefghijklmnopq";
        byte[] bytes = vinStr.getBytes();
        System.out.println(bytes.length);
        List<Vin> vins = new ArrayList<>();
        vins.add(new Vin(vinStr.getBytes()));
        List<Row> result = db.executeLatestQuery(new LatestQueryRequest(tableName,vins,new HashSet<>()));
        if(result.size()>0){
            System.out.println(result.get(0));
        }else{
            System.out.println("empty");
        }
        db.shutdown();

    }

    public static void createTable(TSDBEngine db, String tableName) throws Exception{
        // 建表
        Map<String, ColumnValue.ColumnType> columns = new HashMap<>();
        columns.put("int_field", ColumnValue.ColumnType.COLUMN_TYPE_INTEGER);
        columns.put("double_field", ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT);
        columns.put("string_field", ColumnValue.ColumnType.COLUMN_TYPE_STRING);
        Schema schema = new Schema(columns);
        db.createTable(tableName, schema);
    }

}
