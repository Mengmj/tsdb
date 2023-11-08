package com.alibaba.lindorm.contest.custom;

import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.Schema;

import java.io.Serializable;
import java.util.*;

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

    public final ArrayList<String> strColumns;
    public final ArrayList<String> numericColumns;


    private InternalSchema(Map<String, ColumnValue.ColumnType> columnTypeMap){
        this.columnTypeMap = columnTypeMap;
        columnOffset = new HashMap<>();
        columnCount = columnTypeMap.size();
        numericColumns = new ArrayList<>();
        strColumns = new ArrayList<>();
        int int_count = 0,double_count = 0,string_count = 0;
        for(var entry: columnTypeMap.entrySet()){
            String colName = entry.getKey();
            ColumnValue.ColumnType colType = entry.getValue();
            switch (colType){
                case COLUMN_TYPE_INTEGER:
                    numericColumns.add(colName);
                    int_count++;
                    break;
                case COLUMN_TYPE_DOUBLE_FLOAT:
                    numericColumns.add(colName);
                    double_count++;
                    break;
                case COLUMN_TYPE_STRING:
                    strColumns.add(colName);
                    string_count++;
                    break;
            }
        }
        intCount = int_count;
        doubleCount = double_count;
        stringCount = string_count;
        List<String> colNamesList = new ArrayList<>();
        colNamesList.addAll(numericColumns);
        colNamesList.addAll(strColumns);
        colNames = colNamesList.toArray(new String[0]);
        columnTypes = new ColumnValue.ColumnType[colNames.length];
        int offset = Byte.BYTES + Short.BYTES;
        for(int i = 0;i < colNames.length;++i){
            columnTypes[i] = columnTypeMap.get(colNames[i]);
            columnOffset.put(colNames[i],offset);
            switch (columnTypes[i]){
                case COLUMN_TYPE_INTEGER:
                    offset+=Integer.BYTES;
                    break;
                case COLUMN_TYPE_DOUBLE_FLOAT:
                    offset += Double.BYTES;
                    break;
                case COLUMN_TYPE_STRING:
                    offset += Integer.BYTES + Short.BYTES;
                    break;
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
