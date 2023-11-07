package com.alibaba.lindorm.contest.task;

import com.alibaba.lindorm.contest.CommonUtils;
import com.alibaba.lindorm.contest.custom.AggResult;
import com.alibaba.lindorm.contest.custom.DataFile;
import com.alibaba.lindorm.contest.custom.FileKey;
import com.alibaba.lindorm.contest.custom.RawDataFile;
import com.alibaba.lindorm.contest.manager.TSDBFileSystem;
import com.alibaba.lindorm.contest.structs.CompareExpression;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * 在单文件内进行降采样聚合
 */
public class DownsampleTask implements Callable<Map<Long,AggResult>> {
    TSDBFileSystem fileSystem;
    private int id;
    private long timeLowerBound;
    private long timeUpperBound;
    private long interval;
    private String colName;

    private CompareExpression compareExpression;

    public DownsampleTask(TSDBFileSystem fileSystem,int id,long timeLowerBound,long timeUpperBound,long interval,String colName,CompareExpression compareExpression){
        this.fileSystem = fileSystem;
        this.id = id;
        this.timeLowerBound = timeLowerBound;
        this.timeUpperBound = timeUpperBound;
        this.interval = interval;
        this.colName = colName;
        this.compareExpression = compareExpression;
    }

    @Override
    public Map<Long,AggResult> call() throws Exception {
        Map<Long,AggResult> ret = new HashMap<>();
        long partition = CommonUtils.getPartition(timeLowerBound);
        FileKey key = FileKey.buildFromPartition(id,partition);
        DataFile dataFile = fileSystem.getDataFile(key,false);
        if(dataFile==null){
            return ret;
        }
        for(long time = timeLowerBound;time+interval <= timeUpperBound;time+=interval){
            ret.put(time,dataFile.aggColumn(id,colName,time,time+interval,compareExpression));
        }
        fileSystem.derefFile(dataFile);
        return ret;
    }
}
