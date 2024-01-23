package org.example.connector;

import com.alibaba.lindorm.contest.TSDBEngine;
import com.alibaba.lindorm.contest.TSDBEngineImpl;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.Vin;
import com.alibaba.lindorm.contest.structs.WriteRequest;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TsdbSinkFunction extends RichSinkFunction<RowData> {
    private TSDBEngine tsdb;
    private final String dbPath;
    private final String dbTable;
    private final List<LogicalType> parsingTypes;
    private final List<String> fieldNames;

    public TsdbSinkFunction(String dbPath, String dbTable, DataType rowType) {
        this.dbPath = dbPath;
        this.dbTable = dbTable;
        this.parsingTypes = rowType.getLogicalType().getChildren();
        this.fieldNames = DataType.getFieldNames(rowType);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        tsdb =new TSDBEngineImpl(new File(dbPath));
        tsdb.connect();
    }

    @Override
    public void close() throws Exception {
        tsdb.shutdown();
    }

    @Override
    public void invoke(RowData value, Context context) throws Exception {
        Vin vin = null;
        long timestamp = 0;
        Map<String, ColumnValue> columns = new HashMap<>();
        for(int i = 0;i < parsingTypes.size();++i){
            switch (fieldNames.get(i)){
                case "vin":
                    vin = new Vin(value.getString(i).toBytes());
                    break;
                case "timestamp":
                    timestamp = value.getLong(i);
                    break;
                default:
                    String columnName = fieldNames.get(i);
                    ColumnValue columnValue;
                    switch (parsingTypes.get(i).getTypeRoot()){
                        case INTEGER:
                            columnValue = new ColumnValue.IntegerColumn(value.getInt(i));
                            break;
                        case DOUBLE:
                            columnValue = new ColumnValue.DoubleFloatColumn(value.getDouble(i));
                            break;
                        case VARCHAR:
                            columnValue = new ColumnValue.StringColumn(ByteBuffer.wrap(value.getString(i).toBytes()));
                            break;
                        default:
                            throw new RuntimeException("unexpected type");
                    }
                    columns.put(columnName, columnValue);
            }
        }
        Row row = new Row(vin, timestamp, columns);
        System.out.println(row);
        List<Row> rows = new ArrayList<>();
        rows.add(row);
        WriteRequest writeRequest = new WriteRequest(dbTable, rows);
        tsdb.write(writeRequest);
    }
}
