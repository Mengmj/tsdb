package org.example.connector;

import com.alibaba.lindorm.contest.TSDBEngine;
import com.alibaba.lindorm.contest.TSDBEngineImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import java.io.File;
import java.util.List;
import java.util.Set;

public class TsdbSourceFunction extends RichSourceFunction<RowData> implements ResultTypeQueryable<RowData>{
    private TSDBEngine db;
    private final String dbPath;
    private final String dbTable;
//    private final Set<String> quriedFields;
//
//    private final List<ResolvedExpression> filters;

    private final TypeInformation<RowData> producedTypeInfo;

    public TsdbSourceFunction(String dbPath, String dbTable, TypeInformation<RowData> producedTypeInfo) {
        this.dbPath = dbPath;
        this.dbTable = dbTable;
//        this.quriedFields = quriedFields;
//        this.filters = filters;
        this.producedTypeInfo = producedTypeInfo;
        db = new TSDBEngineImpl(new File(dbPath));
    }



    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        System.out.println("reading");
    }

    @Override
    public void cancel() {

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        db.connect();
    }

    @Override
    public void close() throws Exception {
        db.shutdown();
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedTypeInfo;
    }
}
