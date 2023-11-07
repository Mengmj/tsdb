package com.alibaba.lindorm.contest;

public class CommonUtils {
    public static final int PARTITION_SECONDS = 3600;
    public static final int BUCKLE_SIZE = 64;
    public static int getBuckle(int id){
        return id / BUCKLE_SIZE;
    }
    public static long getPartition(long timestamp){
        return timestamp / (PARTITION_SECONDS*1000);
    }

    public static long getPartitionBegin(long partition){
        return partition * PARTITION_SECONDS * 1000;
    }
    public static long getPartitionEnd(long partition){
        return (partition+1) * PARTITION_SECONDS * 1000;
    }
}
