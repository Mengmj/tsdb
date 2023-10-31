package com.alibaba.lindorm.contest.custom;

import com.alibaba.lindorm.contest.CommonUtils;

public class FileKey {
    public final long partition;
    public final int buckle;
    private FileKey(int buckle,long partition){
        this.partition = partition;
        this.buckle = buckle;
    }
    public static FileKey buildFromTimestamp(int id, long timestamp){
        return new FileKey(CommonUtils.getBuckle(id),CommonUtils.getPartition(timestamp));
    }
    public static FileKey buildFromPartition(int id, long partition){
        return new FileKey(CommonUtils.getBuckle(id),partition);
    }

    @Override
    public int hashCode() {
        return Long.hashCode(partition*1000+buckle);
    }

    @Override
    public boolean equals(Object obj) {
        if(obj==null || obj.getClass() != this.getClass()){
            return false;
        }
        return partition == ((FileKey) obj).partition && buckle==((FileKey) obj).buckle;
    }
}
