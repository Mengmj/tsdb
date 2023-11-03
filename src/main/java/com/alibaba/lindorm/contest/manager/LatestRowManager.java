package com.alibaba.lindorm.contest.manager;

import com.alibaba.lindorm.contest.custom.InternalSchema;
import com.alibaba.lindorm.contest.custom.RowWritable;
import com.alibaba.lindorm.contest.custom.VinWritable;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.Vin;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class LatestRowManager {
    private final ConcurrentMap<Vin, Row> rowMap;
//    private static int SEG_NUM = 32;
//    private final AtomicBoolean[] segLocks;
    private LatestRowManager(){
//        segLocks = new AtomicBoolean[SEG_NUM];
//        for(int i = 0;i < SEG_NUM;++i){
//            segLocks[i] = new AtomicBoolean(false);
//        }
        rowMap = new ConcurrentHashMap<>();
    }
    private LatestRowManager(List<RowWritable> rowWritables, InternalSchema schema){
        this();
        for(RowWritable row: rowWritables){
            rowMap.put(row.getVin(),row.getRow(schema));
        }
    }
    public static LatestRowManager getInstance(){
        return new LatestRowManager();
    }
    public static LatestRowManager getInstance(List<RowWritable> rowWritables,InternalSchema schema){
        return new LatestRowManager(rowWritables,schema);
    }
    public Row getLatestRow(Vin vin){
        Row ret = null;
        if(rowMap.containsKey(vin)){
            ret = rowMap.get(vin);
        }
        return ret;
    }
//    public void upsert(Row row){
//        VinWritable vinWritable = new VinWritable(row.getVin());
//        Row latestRow = rowMap.get(vinWritable);
//        int MOD = 1_000_000_007;
//        if(latestRow==null || latestRow.getTimestamp() < row.getTimestamp()){
//            int segNum = (((row.getVin().hashCode())%MOD)+MOD) % SEG_NUM;
//            while (segLocks[segNum].getAndSet(true)){}
//            latestRow = rowMap.get(vinWritable);
//            if(latestRow==null || latestRow.getTimestamp() < row.getTimestamp()){
//                rowMap.put(new VinWritable(row.getVin()),row);
//            }
//            segLocks[segNum].set(false);
//        }
//    }
    public void upsert(Row row){
        Vin vin = row.getVin();
        Row latestRow = rowMap.get(vin);
        if(latestRow==null || latestRow.getTimestamp() < row.getTimestamp()){
            synchronized (rowMap){
                latestRow = rowMap.get(vin);
                if(latestRow==null || latestRow.getTimestamp() < row.getTimestamp()){
                    rowMap.put(vin,row);
                }
            }
        }
    }
//    synchronized public void upsert(Row row){
//        Row latestRow = rowMap.get(row.getVin());
//        if(latestRow==null || latestRow.getTimestamp() < row.getTimestamp()){
//            rowMap.put(row.getVin(),row);
//        }
//    }
    public ArrayList<RowWritable> getRowWriteableList(InternalSchema schema){
        ArrayList<RowWritable> ret = new ArrayList<>();
        for(Row row: rowMap.values()){
            ret.add(RowWritable.getInstance(row,schema));
        }
        return ret;
    }

}
