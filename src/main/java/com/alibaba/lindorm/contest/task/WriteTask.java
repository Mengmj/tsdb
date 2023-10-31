package com.alibaba.lindorm.contest.task;

import com.alibaba.lindorm.contest.custom.FileKey;
import com.alibaba.lindorm.contest.custom.MappedFile;
import com.alibaba.lindorm.contest.custom.RowWrapped;
import com.alibaba.lindorm.contest.manager.TSDBFileSystem;

import java.util.List;
import java.util.ListIterator;
import java.util.Map;

public class WriteTask implements Runnable{
    private TSDBFileSystem fileSystem;
    private Map<FileKey, List<RowWrapped>> rowListMap;
    public WriteTask(TSDBFileSystem fileSystem, Map<FileKey, List<RowWrapped>> rowListMap){
        this.fileSystem = fileSystem;
        this.rowListMap = rowListMap;
    }
    @Override
    public void run() {
        for(var entry: rowListMap.entrySet()){
            FileKey fileKey = entry.getKey();
            List<RowWrapped> rowList = entry.getValue();
            MappedFile mappedFile = fileSystem.getMappedFile(fileKey,true);
            mappedFile.writeRows(rowList);
            fileSystem.deRefFile(mappedFile);
        }
    }
}
