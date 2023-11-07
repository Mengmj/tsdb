package com.alibaba.lindorm.contest.custom;

import com.alibaba.lindorm.contest.structs.CompareExpression;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.Vin;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public abstract class DataFile {
    public final File file;
    public final long partition;
    public final int buckle;
    final InternalSchema schema;
    DataFile(File file, long partition, int buckle, InternalSchema schema){
        this.file = file;
        this.partition = partition;
        this.buckle = buckle;
        this.schema = schema;
    }
    abstract public void writeRows(List<RowWrapped> rows);
    abstract public ArrayList<Row> readRows(Vin vin, int id, Set<String> colNames, long timeLowerBound, long timeupperBound);
    abstract public AggResult aggColumn(int id,String colName,long timeLowerBound,long timeUpperBound);
    abstract public AggResult aggColumn(int id, String colName, long timeLowerBound, long timeUpperBound, CompareExpression expression);
    abstract public void close() throws IOException;
}
