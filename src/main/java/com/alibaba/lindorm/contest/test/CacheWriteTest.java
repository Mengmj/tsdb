//package com.alibaba.lindorm.contest.test;
//
//import com.alibaba.lindorm.contest.custom.RowWritable;
//import com.alibaba.lindorm.contest.structs.Row;
//import com.alibaba.lindorm.contest.test.TestUtils.*;
//import java.io.*;
//import java.util.LinkedList;
//import java.util.List;
//import java.util.TreeMap;
//
//public class CacheWriteTest {
//    public static void main(String[] args) {
//        int vinCount = 10000;
//        List<Row> rows = TestUtils.randomRows(vinCount,TestUtils.TEST_SCHEMA,System.currentTimeMillis());
//        List<RowWritable> cache = new LinkedList<>();
//        for(Row row: rows){
//            cache.add(RowWritable.getInstance(row,new TreeMap<>(TestUtils.TEST_SCHEMA.getColumnTypeMap())));
//        }
//        long ts1 = System.currentTimeMillis();
//        try(FileOutputStream fos = new FileOutputStream("/Users/meng/playground/testdir/cache.ser");
//            ObjectOutputStream oos = new ObjectOutputStream(fos)){
//            oos.writeObject(cache);
//        }catch (IOException e){
//            throw new RuntimeException(e);
//        }
//        long ts2 = System.currentTimeMillis();
//        System.out.println(String.format("writing cache %d ms", ts2-ts1));
//        try(FileInputStream fis = new FileInputStream("/Users/meng/playground/testdir/cache.ser");
//            ObjectInputStream ois = new ObjectInputStream(fis)){
//            List<RowWritable>  chche1 = (List<RowWritable>) ois.readObject();
//            //RowWritable rowWritable = (RowWritable) ois.readObject();
//        }catch (Exception e){
//            throw new RuntimeException(e);
//        }
//        long ts3 = System.currentTimeMillis();
//        System.out.println(String.format("reading cache %d ms", ts3-ts2));
//    }
//
//}
