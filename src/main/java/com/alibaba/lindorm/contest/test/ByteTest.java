//package com.alibaba.lindorm.contest.test;
//
//import com.alibaba.lindorm.contest.RowWriter;
//import com.alibaba.lindorm.contest.custom.RowWritable;
//import com.alibaba.lindorm.contest.structs.ColumnValue;
//import com.alibaba.lindorm.contest.structs.Row;
//import com.alibaba.lindorm.contest.structs.Vin;
//
//import java.io.*;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.SortedMap;
//import java.util.TreeMap;
//
//public class ByteTest {
//    public static void main(String[] args) {
//        byteTest();
//    }
//    public static void rowTest(){
//
//        Vin vin = new Vin(new byte[17]);
//        long timestamp = System.currentTimeMillis();
//
//        SortedMap<String, ColumnValue.ColumnType> columnTypeSortedMap = new TreeMap<>();
//        columnTypeSortedMap.put("col1", ColumnValue.ColumnType.COLUMN_TYPE_INTEGER);
//        columnTypeSortedMap.put("col2", ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT);
//
//        Map<String, ColumnValue> columns = new HashMap<>();
//        columns.put("col1",new ColumnValue.IntegerColumn(100));
//        columns.put("col2",new ColumnValue.DoubleFloatColumn(234.0));
//
//        Row row = new Row(vin,timestamp,columns);
//        RowWritable row1 = RowWritable.getInstance(row,columnTypeSortedMap);
//
//        try(FileOutputStream fos = new FileOutputStream("/Users/meng/playground/testdir/row.ser");
//            ObjectOutputStream oos = new ObjectOutputStream(fos)){
//            oos.writeObject(row1);
//        }catch (IOException e){
//            throw new RuntimeException(e);
//        }
//        try(FileInputStream fis = new FileInputStream("/Users/meng/playground/testdir/row.ser");
//            ObjectInputStream ois = new ObjectInputStream(fis)){
//            RowWritable row2 = (RowWritable) ois.readObject();
//            Row rowRead = row2.getRow(columnTypeSortedMap);
//            System.out.println(row);
//            System.out.println(rowRead);
//            //System.out.println(compareRow(row,rowRead));
//        }catch (Exception e){
//            throw new RuntimeException(e);
//        }
//    }
//
//    public static void byteTest(){
//        byte[] bytes = new byte[4];
//        int num = 442396;
//        for(int j = 0;j < 4;++j){
//            bytes[j] = (byte)(num & 0xff);
//            num >>= 8;
//        }
//        int num1 = 0;
//        for(int j = 3;j >= 0;--j){
//            num1 <<= 8;
//            num1 |= (bytes[j] & 0xff);
//        }
//        System.out.println(num1);
//    }
//}
