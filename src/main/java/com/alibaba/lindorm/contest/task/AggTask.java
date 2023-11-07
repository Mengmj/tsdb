package com.alibaba.lindorm.contest.task;

import com.alibaba.lindorm.contest.CommonUtils;
import com.alibaba.lindorm.contest.custom.AggResult;
import com.alibaba.lindorm.contest.custom.DataFile;
import com.alibaba.lindorm.contest.custom.FileKey;
import com.alibaba.lindorm.contest.custom.RawDataFile;
import com.alibaba.lindorm.contest.manager.TSDBFileSystem;
import com.alibaba.lindorm.contest.structs.CompareExpression;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public class AggTask implements Callable<AggResult> {
    TSDBFileSystem fileSystem;
    private int id;
    private long timeLowerBound;
    private long timeUpperBound;
    private String colName;

    private CompareExpression compareExpression;
    public AggTask(TSDBFileSystem fileSystem, int id, long timeLowerBound, long timeUpperBound, String colName, CompareExpression expression){
        this.fileSystem = fileSystem;
        this.id = id;
        this.timeLowerBound = timeLowerBound;
        this.timeUpperBound = timeUpperBound;
        this.colName = colName;
        this.compareExpression = expression;
    }

    @Override
    public AggResult call() throws Exception {
        long beginPartition = CommonUtils.getPartition(timeLowerBound);
        long endPartition = CommonUtils.getPartition(timeUpperBound-1);
        List<AggResult> aggResults = new ArrayList<>();
        for(long p = beginPartition;p <= endPartition;++p){
            long lower = Math.max(CommonUtils.getPartitionBegin(p),timeLowerBound);
            long upper = Math.min(CommonUtils.getPartitionEnd(p),timeUpperBound);
            FileKey fileKey = FileKey.buildFromPartition(id,p);
            AggResult aggResult;
            DataFile dataFile = fileSystem.getDataFile(fileKey,false);
            if(dataFile==null){
                continue;
            }
            aggResult = dataFile.aggColumn(id, colName, lower, upper, compareExpression);
            aggResults.add(aggResult);
            fileSystem.derefFile(dataFile);
        }
        return AggResult.merge(aggResults);
    }
}
