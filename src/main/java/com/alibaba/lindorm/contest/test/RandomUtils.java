package com.alibaba.lindorm.contest.test;

import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.Schema;
import com.alibaba.lindorm.contest.structs.Vin;

import java.nio.ByteBuffer;
import java.util.*;

public class RandomUtils {
    private Random random;
    public RandomUtils(){
        random = new Random(System.currentTimeMillis()+Thread.currentThread().getId()*2023);
    }
    public List<Vin> randomVins(int n){
        List<Vin> ret = new ArrayList<>(n);
        for(int i = 0;i < n;++i){
            byte[] bytes = new byte[17];
            random.nextBytes(bytes);
            ret.add(new Vin(bytes));
        }
        return ret;
    }

    public Vin randomVin(){
        byte[] bytes = new byte[17];
        random.nextBytes(bytes);
        return new Vin(bytes);
    }

    private Map<String, ColumnValue> randomColumns(Schema schema){
        Map<String,ColumnValue> columns = new HashMap<>();
        String s = "abcdefghijklmnopqrstuvwxyz";
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
                    value = new ColumnValue.IntegerColumn(random.nextInt(1000));
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
        return columns;
    }
    public List<Row> randomRows(List<Vin> vinList, long timestamp, Schema schema){
        List<Row> ret = new ArrayList<>();
        for(Vin vin: vinList){
            ret.add(new Row(vin,timestamp,randomColumns(schema)));
        }
        return ret;
    }

    public ArrayList<Row> randomRows(Vin vin,long timestamp,Schema schema,int count){
        ArrayList<Row> ret = new ArrayList<>();
        for(int i = 0;i < count;++i){
            ret.add(new Row(vin,timestamp+i*1000,randomColumns(schema)));
        }
        return ret;
    }
}
