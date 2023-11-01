package com.alibaba.lindorm.contest.task;

import com.alibaba.lindorm.contest.CommonUtils;
import com.alibaba.lindorm.contest.custom.AggResult;
import com.alibaba.lindorm.contest.custom.FileKey;
import com.alibaba.lindorm.contest.custom.MappedFile;
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
            MappedFile mappedFile = fileSystem.getMappedFile(fileKey,false);
            if(mappedFile==null){
                continue;
            }
            if(compareExpression==null){
                aggResult = mappedFile.aggColumn(id, colName, lower, upper);
            }else if (compareExpression.getCompareOp()== CompareExpression.CompareOp.EQUAL){
                aggResult = mappedFile.aggColumnEqual(id, colName, lower, upper, compareExpression.getValue());
            }else{
                aggResult = mappedFile.aggColumnGreater(id,colName, lower, upper, compareExpression.getValue());
            }
            aggResults.add(aggResult);
            fileSystem.deRefFile(mappedFile);
        }
        return AggResult.merge(aggResults);
    }
}
