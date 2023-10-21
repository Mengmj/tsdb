package com.alibaba.lindorm.contest.custom;

import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.Vin;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

public class RowWritable implements Serializable {
    private final byte[] vin;
    private final long timestamp;
    private final byte[][] rawData;

    private RowWritable(Row row, InternalSchema schema){
        this.vin = row.getVin().getVin();
        this.timestamp = row.getTimestamp();
        rawData = new byte[schema.columnCount][];
        String[] colNames = schema.colNames;
        ColumnValue.ColumnType[] columnTypes = schema.columnTypes;
        for(int i = 0;i < schema.columnCount;++i){
            switch (columnTypes[i]){
                case COLUMN_TYPE_STRING:
                    ColumnValue.StringColumn stringColumn = (ColumnValue.StringColumn) row.getColumns().get(colNames[i]);
                    rawData[i] = stringColumn.getStringValue().array();
                    break;
                case COLUMN_TYPE_INTEGER:
                    ColumnValue.IntegerColumn integerColumn = (ColumnValue.IntegerColumn) row.getColumns().get(colNames[i]);
                    int intergerValue = integerColumn.getIntegerValue();
                    rawData[i] = new byte[4];
                    for(int j = 0;j<4;++j){
                        rawData[i][j] = (byte) (intergerValue & 0xff);
                        intergerValue >>= 8;
                    }
                    break;
                case COLUMN_TYPE_DOUBLE_FLOAT:
                    ColumnValue.DoubleFloatColumn doubleFloatColumn = (ColumnValue.DoubleFloatColumn) row.getColumns().get(colNames[i]);
                    double doubleValue = doubleFloatColumn.getDoubleFloatValue();
                    long logBits = Double.doubleToRawLongBits(doubleValue);
                    rawData[i] = new byte[8];
                    for(int j = 0;j<8;++j){
                        rawData[i][j] = (byte) (logBits & 0xff);
                        logBits >>= 8;
                    }
                    break;
                default:
                    throw new RuntimeException(String.format("unexpected column type: %s",columnTypes[i].toString()));
            }
        }
    }
    public static RowWritable getInstance(Row row, InternalSchema schema){
        return new RowWritable(row,schema);
    }
    public Row getRow(InternalSchema schema) {
        Map<String, ColumnValue> columns = new HashMap<>();
        String[] colNames = schema.colNames;
        ColumnValue.ColumnType[] colTypes = schema.columnTypes;
        for(int i = 0;i < schema.columnCount;++i){
            ColumnValue columnValue;
            switch (colTypes[i]) {
                case COLUMN_TYPE_STRING:
                    columnValue = new ColumnValue.StringColumn(ByteBuffer.wrap(rawData[i]));
                    break;
                case COLUMN_TYPE_INTEGER:
                    int integerValue = 0;
                    for (int j = 3; j >= 0; --j) {
                        integerValue <<= 8;
                        integerValue |= rawData[i][j] & 0xff;
                    }
                    columnValue = new ColumnValue.IntegerColumn(integerValue);
                    break;
                case COLUMN_TYPE_DOUBLE_FLOAT:
                    long longbits = 0L;
                    for (int j = 7; j >= 0; --j) {
                        longbits <<= 8;
                        longbits |= rawData[i][j] & 0xff;
                    }
                    columnValue = new ColumnValue.DoubleFloatColumn(Double.longBitsToDouble(longbits));
                    break;
                default:
                    throw new RuntimeException(String.format("unexpected column type: %s", colTypes[i].toString()));

            }
            columns.put(colNames[i], columnValue);
        }
        return new Row(new Vin(vin),timestamp,columns);
    }
    public Vin getVin(){
        return new Vin(vin);
    }

    public long getTimestamp(){
        return timestamp;
    }

}
