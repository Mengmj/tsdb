package com.alibaba.lindorm.contest;

import com.alibaba.lindorm.contest.custom.InternalSchema;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.Row;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.BlockingQueue;

public class RowWriter extends Thread{
    final BlockingQueue<Row> rowBuffer;
    final InternalSchema schema;
    final File dataDirectory;
    public RowWriter(File dataDirectory, BlockingQueue<Row> rowBuffer, InternalSchema schema){
        super();
        this.rowBuffer = rowBuffer;
        this.schema = schema;
        this.dataDirectory = dataDirectory;
    }

    /**
     * 从队列中取数据并写入到硬盘
     */
    @Override
    public void run() {
        List<Row> rowList = new LinkedList<>();
        while(!isInterrupted()){
            rowBuffer.drainTo(rowList);
            if(rowList.size()>0) {
                dumpRows(rowList);
                rowList.clear();
            }else{
                //让出cpu等待数据
                Thread.yield();
            }
        }
        //interupt之后不会有新数据
        rowBuffer.drainTo(rowList);
        dumpRows(rowList);

    }

    private void dumpRows(List<Row> rows){

    }
}
