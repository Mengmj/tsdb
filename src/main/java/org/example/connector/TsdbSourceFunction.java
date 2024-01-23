package org.example.connector;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.RowData;

public class TsdbSourceFunction extends RichSourceFunction<RowData> implements ResultTypeQueryable<RowData> {
    @Override
    public TypeInformation<RowData> getProducedType() {
        return null;
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {

    }

    @Override
    public void cancel() {

    }
}
