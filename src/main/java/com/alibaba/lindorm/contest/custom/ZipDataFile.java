package com.alibaba.lindorm.contest.custom;

import com.alibaba.lindorm.contest.structs.CompareExpression;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.Vin;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class ZipDataFile extends DataFile{
    ZipDataFile(File file, long partition, int buckle, InternalSchema schema) {
        super(file, partition, buckle, schema);
    }

    @Override
    public void writeRows(List<RowWrapped> rows) {
        throw new RuntimeException("try write row into a zipped file");
    }

    @Override
    public ArrayList<Row> readRows(Vin vin, int id, Set<String> colNames, long timeLowerBound, long timeupperBound) {
        return null;
    }

    @Override
    public AggResult aggColumn(int id, String colName, long timeLowerBound, long timeUpperBound) {
        return null;
    }

    @Override
    public AggResult aggColumn(int id, String colName, long timeLowerBound, long timeUpperBound, CompareExpression expression) {
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}
