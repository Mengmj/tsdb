package com.alibaba.lindorm.contest.test;

import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.Schema;
import com.alibaba.lindorm.contest.structs.Vin;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;

public class TestUtils {

    static final File TEST_DIR = new File("/Users/meng/playground/testdir");
    static final Schema TEST_SCHEMA;
    static {
        SortedMap<String, ColumnValue.ColumnType> columnTypeMap = new TreeMap<>();
        for(int i = 0;i < 20;++i){
            columnTypeMap.put("int_"+i, ColumnValue.ColumnType.COLUMN_TYPE_INTEGER);
            columnTypeMap.put("double_"+i, ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT);
            columnTypeMap.put("string_"+i, ColumnValue.ColumnType.COLUMN_TYPE_STRING);
        }
        TEST_SCHEMA = new Schema(columnTypeMap);
    }
    public static boolean compareRow(Row row1, Row row2){
        if(row1 == null || row2 == null){
            return false;
        }
        if(!row1.getVin().equals(row2.getVin())){
            return false;
        }
        Map<String, ColumnValue> columns1 = row1.getColumns();
        Map<String,ColumnValue> columns2 = row2.getColumns();
        if(columns1.size()!=columns2.size()){
            return false;
        }
        for(var entry: columns1.entrySet()){
            String colName = entry.getKey();
            ColumnValue value1 = entry.getValue();
            if(!columns2.containsKey(colName)){
                return false;
            }
            ColumnValue value2 = columns2.get(colName);
            if(value1==null){
                if(value2!=null){
                    return false;
                }
            }else{
                if(!value1.equals(value2)){
                    return false;
                }
            }
        }
        return true;
    }
    public static Row randomRow(Schema schema){
        long timestamp = System.currentTimeMillis();
        String s = "abcdefghijklmnopqrstuvwxyz";
        Random random = new Random();
        Map<String,ColumnValue> columns = new HashMap<>();
        byte[] vin = new byte[17];
        random.nextBytes(vin);
        for(var entry: schema.getColumnTypeMap().entrySet()){
            String colName = entry.getKey();
            ColumnValue.ColumnType t = entry.getValue();
            ColumnValue value = null;
            switch(t){
                case COLUMN_TYPE_DOUBLE_FLOAT:
                    value = new ColumnValue.DoubleFloatColumn(random.nextDouble());
                    //value = new ColumnValue.DoubleFloatColumn(234.0);
                    break;
                case COLUMN_TYPE_INTEGER:
                    value = new ColumnValue.IntegerColumn(random.nextInt());
                    //value = new ColumnValue.IntegerColumn(100);
                    break;
                case COLUMN_TYPE_STRING:
                    int len = 6+random.nextInt(20);
                    String subStr = s.substring(0,len);
                    byte[] bytes = subStr.getBytes();
                    value = new ColumnValue.StringColumn(ByteBuffer.wrap(bytes));
            }
            columns.put(colName,value);
        }
        return new Row(new Vin(vin),timestamp,columns);
    }

    public static List<Row> randomRows(int vinCount, Schema schema,long timestamp){
        String s = "abcdefghijklmnopqrstuvwxyz";
        List<Row> ret = new ArrayList<>(vinCount);
        Random random = new Random();
        for(int i = 0;i < vinCount;++i){
            Map<String,ColumnValue> columns = new HashMap<>();
            byte[] vin = new byte[17];
            random.nextBytes(vin);
            for(var entry: schema.getColumnTypeMap().entrySet()){
                String colName = entry.getKey();
                ColumnValue.ColumnType t = entry.getValue();
                ColumnValue value = null;
                switch(t){
                    case COLUMN_TYPE_DOUBLE_FLOAT:
                        value = new ColumnValue.DoubleFloatColumn(random.nextDouble());
                        //value = new ColumnValue.DoubleFloatColumn(1L);
                        break;
                    case COLUMN_TYPE_INTEGER:
                        value = new ColumnValue.IntegerColumn(random.nextInt());
                        //value = new ColumnValue.IntegerColumn(100);
                        break;
                    case COLUMN_TYPE_STRING:
                        int len = 6+random.nextInt(20);
                        String subStr = s.substring(0,len);
                        byte[] bytes = subStr.getBytes();
                        value = new ColumnValue.StringColumn(ByteBuffer.wrap(bytes));
                }
                columns.put(colName,value);
            }
            ret.add(new Row(new Vin(vin),timestamp,columns));
        }
        return ret;
    }
}