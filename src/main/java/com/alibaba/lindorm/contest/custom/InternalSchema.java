package com.alibaba.lindorm.contest.custom;

import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.Schema;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * 与Schema相比,InternalSchema中的列是有固定顺序的.支持序列化
 */
public class InternalSchema implements Serializable {
    private final Map<String, ColumnValue.ColumnType> columnTypeMap;
    private final Map<String, Integer> columnOffset;
    public final String[] colNames;
    public final ColumnValue.ColumnType[] columnTypes;
    public final int columnCount;
    public final int intCount;
    public final int doubleCount;
    public final int stringCount;

    public final int rawLength;

    public final String[] strColumns;
    public final String[] numericColumns;


    private InternalSchema(Map<String, ColumnValue.ColumnType> columnTypeMap){
        this.columnTypeMap = columnTypeMap;
        columnOffset = new HashMap<>();
        columnCount = columnTypeMap.size();
        colNames = new String[columnCount];
        columnTypes = new ColumnValue.ColumnType[columnCount];
        int offset = Byte.BYTES+Short.BYTES;
        int i = 0;
        int int_count = 0,double_count = 0,string_count = 0;
        for(var entry: columnTypeMap.entrySet()){
            colNames[i] = entry.getKey();
            columnTypes[i] = entry.getValue();
            columnOffset.put(colNames[i],offset);
            switch (columnTypes[i]){
                case COLUMN_TYPE_INTEGER:
                    int_count++;
                    offset+=Integer.BYTES;
                    break;
                case COLUMN_TYPE_DOUBLE_FLOAT:
                    double_count++;
                    offset+=Double.BYTES;
                    break;
                case COLUMN_TYPE_STRING:
                    string_count++;
                    offset+=Integer.BYTES+Short.BYTES;
                    break;
            }
            ++i;
        }
        intCount = int_count;
        doubleCount = double_count;
        stringCount = string_count;
        strColumns = new String[stringCount];
        numericColumns = new String[intCount+doubleCount];
        int strIdx = 0,numIdx = 0;
        for(int j = 0;j < columnCount;++j){
            if(columnTypes[j]== ColumnValue.ColumnType.COLUMN_TYPE_STRING){
                strColumns[strIdx] = colNames[j];
                ++strIdx;
            }else {
                numericColumns[numIdx] = colNames[j];
                ++numIdx;
            }
        }
        rawLength = Byte.BYTES+Short.BYTES+intCount*Integer.BYTES+doubleCount*Double.BYTES+stringCount*(Integer.BYTES+Short.BYTES);
    }

    public static InternalSchema build(Schema schema){
        return new InternalSchema(schema.getColumnTypeMap());
    }

    public static InternalSchema build(Map<String, ColumnValue.ColumnType> columnTypeMap){
        return new InternalSchema(columnTypeMap);
    }
    public ColumnValue.ColumnType getType(String colName){
        return columnTypeMap.get(colName);
    }
    public Set<String> getColumnNames(){
        return columnTypeMap.keySet();
    }
    public int getOffset(String colName){
        if(!columnOffset.containsKey(colName)){
            return -1;
        }
        return columnOffset.get(colName);
    }
}
