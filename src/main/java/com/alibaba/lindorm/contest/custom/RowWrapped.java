package com.alibaba.lindorm.contest.custom;

import com.alibaba.lindorm.contest.structs.Row;

public class RowWrapped {
    int id;
    Row row;
    private RowWrapped(int id, Row row){
        this.id = id;
        this.row = row;
    }
    public static RowWrapped wrap(int id,Row row){
        return new RowWrapped(id,row);
    }
}
