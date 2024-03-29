package org.example.connector;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.types.DataType;

public class TsdbSink implements DynamicTableSink {
    private final String dbPath;
    private final String dbTable;
    private final DataType rowType;

    public TsdbSink(String dbPath, String dbTable, DataType rowType) {
        this.dbPath = dbPath;
        this.dbTable = dbTable;
        this.rowType = rowType;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return SinkFunctionProvider.of(new TsdbSinkFunction(dbPath, dbTable, rowType));
    }

    @Override
    public DynamicTableSink copy() {
        return new TsdbSink(dbPath, dbTable, rowType);
    }

    @Override
    public String asSummaryString() {
        return "tsdb table sink";
    }
}
