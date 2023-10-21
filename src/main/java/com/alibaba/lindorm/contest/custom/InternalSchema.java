package com.alibaba.lindorm.contest.custom;

import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.Schema;

import java.io.Serializable;
import java.util.Map;

/**
 * 与Schema相比,InternalSchema中的列是有固定顺序的.支持序列化
 */
public class InternalSchema implements Serializable {
    private final Map<String, ColumnValue.ColumnType> columnTypeMap;
    public final String[] colNames;
    public final ColumnValue.ColumnType[] columnTypes;
    public final int columnCount;
    public final int intCount;
    public final int doubleCount;
    public final int stringCount;

    public final int rawLength;


    private InternalSchema(Map<String, ColumnValue.ColumnType> columnTypeMap){
        this.columnTypeMap = columnTypeMap;
        columnCount = columnTypeMap.size();
        colNames = new String[columnCount];
        columnTypes = new ColumnValue.ColumnType[columnCount];
        int len = 0;
        int i = 0;
        int int_count = 0,double_count = 0,string_count = 0;
        for(var entry: columnTypeMap.entrySet()){
            colNames[i] = entry.getKey();
            columnTypes[i] = entry.getValue();
            switch (columnTypes[i]){
                case COLUMN_TYPE_INTEGER:
                    int_count++;break;
                case COLUMN_TYPE_DOUBLE_FLOAT:
                    double_count++;break;
                case COLUMN_TYPE_STRING:
                    string_count++;break;
            }
            ++i;
        }
        intCount = int_count;
        doubleCount = double_count;
        stringCount = string_count;
        rawLength = intCount*4+doubleCount*8+stringCount*8;
    }

    public static InternalSchema build(Schema schema){
        return new InternalSchema(schema.getColumnTypeMap());
    }

    public static InternalSchema build(Map<String, ColumnValue.ColumnType> columnTypeMap){
        return new InternalSchema(columnTypeMap);
    }
}
