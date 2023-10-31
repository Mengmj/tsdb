package com.alibaba.lindorm.contest.task;

import com.alibaba.lindorm.contest.custom.AggResult;
import com.alibaba.lindorm.contest.custom.MappedFile;
import com.alibaba.lindorm.contest.manager.TSDBFileSystem;
import com.alibaba.lindorm.contest.structs.Row;

import java.util.concurrent.Callable;

public class AggTask implements Callable<AggResult> {
    TSDBFileSystem fileSystem;
    private int id;

    @Override
    public AggResult call() throws Exception {

        MappedFile mappedFile = fileSystem.getMappedFile()
        return null;
    }
}
