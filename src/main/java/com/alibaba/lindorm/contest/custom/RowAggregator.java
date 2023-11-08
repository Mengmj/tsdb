package com.alibaba.lindorm.contest.custom;

import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.Row;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class RowAggregator {
    private volatile long minTs;
    private volatile long maxTs;
    final ConcurrentMap<String ,AggResult> aggColmns;
    public RowAggregator(InternalSchema schema){
        aggColmns = new ConcurrentHashMap<>();
        for(String colName: schema.numericColumns){
            aggColmns.put(colName,new AggResult(schema.getType(colName)));
        }
        minTs = Long.MAX_VALUE;
        maxTs = Long.MIN_VALUE;
    }
    public void add(Row row){
        minTs = Math.min(row.getTimestamp(), minTs);
        maxTs = Math.max(row.getTimestamp(), maxTs);
        for(var entry: aggColmns.entrySet()){
            String colName = entry.getKey();
            AggResult aggResult = entry.getValue();
            ColumnValue value = row.getColumns().get(colName);
            switch (value.getColumnType()){
                case COLUMN_TYPE_INTEGER:
                    aggResult.addInt(value.getIntegerValue());
                    break;
                case COLUMN_TYPE_DOUBLE_FLOAT:
                    aggResult.addDouble(value.getDoubleFloatValue());
                    break;
            }
        }
    }
}
