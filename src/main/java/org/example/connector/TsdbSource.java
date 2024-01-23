package org.example.connector;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TsdbSource implements ScanTableSource {

    private final String dbPath;
    private final String dbTable;
//    private final Set<String> queriedFields;
//    private final List<ResolvedExpression> filters;

    private final DataType producedType;

    public TsdbSource(String dbPath, String dbTable, DataType producedType){
        this.dbPath = dbPath;
        this.dbTable = dbTable;
        this.producedType = producedType;
//        queriedFields = new HashSet<>();
//        filters = new ArrayList<>();
    }
    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        final TypeInformation<RowData> producedTypeInfo = runtimeProviderContext.<RowData>createTypeInformation(producedType);
        return SourceFunctionProvider.of(new TsdbSourceFunction(dbPath, dbTable,producedTypeInfo),true);
    }

    @Override
    public DynamicTableSource copy() {
        return new TsdbSource(dbPath, dbTable, producedType);
    }

    @Override
    public String asSummaryString() {
        return "tsdb table source";
    }

//    @Override
//    public Result applyFilters(List<ResolvedExpression> filters) {
//        List<ResolvedExpression> acceptedFilters = new ArrayList<>();
//        List<ResolvedExpression> remainingFilters = new ArrayList<>();
//
//        for (ResolvedExpression filter : filters) {
//            if (canPushDown(filter)) {
//                acceptedFilters.add(filter);
//            } else {
//                remainingFilters.add(filter);
//            }
//        }
//
//        this.filters.addAll(acceptedFilters);
//        return Result.of(acceptedFilters, remainingFilters);
//    }

    private boolean canPushDown(ResolvedExpression expression){
        System.out.println(expression);
        return false;
    }

//    @Override
//    public boolean supportsNestedProjection() {
//        return false;
//    }
//
//    @Override
//    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
//        List<String> fields = DataType.getFieldNames(producedDataType);
//        for(int[] f: projectedFields){
//            queriedFields.add(fields.get(f[0]));
//        }
//    }
}
