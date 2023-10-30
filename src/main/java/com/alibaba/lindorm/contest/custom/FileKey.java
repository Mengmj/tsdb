package com.alibaba.lindorm.contest.custom;

import com.alibaba.lindorm.contest.CommonUtils;

public class FileKey {
    public final long partition;
    public final int buckle;
    private FileKey(int id,long timestamp){
        partition = CommonUtils.getPartition(timestamp);
        buckle = CommonUtils.getBuckle(id);
    }
    public static FileKey build(int id,long timestamp){
        return new FileKey(id,timestamp);
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
