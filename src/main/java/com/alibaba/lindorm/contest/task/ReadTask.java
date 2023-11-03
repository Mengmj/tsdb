package com.alibaba.lindorm.contest.task;

import com.alibaba.lindorm.contest.CommonUtils;
import com.alibaba.lindorm.contest.custom.FileKey;
import com.alibaba.lindorm.contest.custom.MappedFile;
import com.alibaba.lindorm.contest.manager.TSDBFileSystem;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.Vin;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

public class ReadTask implements Callable<ArrayList<Row>> {
    private final TSDBFileSystem fileSystem;
    private final Vin vin;
    private final int id;
    private final long timeLowerBound;
    private final long timeUpperBound;
    private final Set<String> colNames;
    public ReadTask(TSDBFileSystem fileSystem, Vin vin, int id, long timeLowerBound, long timeUpperBound, Set<String> colNames){
        this.fileSystem = fileSystem;
        this.vin = vin;
        this.id = id;
        this.timeUpperBound = timeUpperBound;
        this.timeLowerBound = timeLowerBound;
        this.colNames = colNames;
    }
    @Override
    public ArrayList<Row> call() throws Exception {
        long beginPartition = CommonUtils.getPartition(timeLowerBound);
        long endPartition = CommonUtils.getPartition(timeUpperBound-1);
        ArrayList<Row> ret = new ArrayList<>();
        for(long p = beginPartition; p <= endPartition;++p){
            FileKey fileKey = FileKey.buildFromPartition(id,p);
            long lower = Math.max(CommonUtils.getPartitionBegin(p),timeLowerBound);
            long upper = Math.min(CommonUtils.getPartitionEnd(p),timeUpperBound);
            MappedFile mappedFile = fileSystem.getMappedFile(fileKey,false);
            if(mappedFile!=null){
                ret.addAll(mappedFile.readRows(vin,id,colNames,lower,upper));
                fileSystem.deRefFile(mappedFile);
            }
        }
        return ret;
    }
}
