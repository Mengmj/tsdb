package com.alibaba.lindorm.contest;

public class CommonUtils {
    public static final int PARTITION_SECONDS = 60;
    public static final int BUCKLE_SIZE = 256;
    public static int getBuckle(int id){
        return id / BUCKLE_SIZE;
    }
    public static long getPartition(long timestamp){
        return timestamp / (PARTITION_SECONDS*1000);
    }
}
