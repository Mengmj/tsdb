package org.example.connector;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;

import java.util.HashSet;
import java.util.Set;

import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

public class TsdbTableFactory implements DynamicTableSinkFactory, DynamicTableSourceFactory {

    static final String FACTORY_IDENTIFIER = "tsdb";

    static final ConfigOption<String> DB_PATH = ConfigOptions.key("db_path")
            .stringType()
            .noDefaultValue();
    static final ConfigOption<String> DB_TABLE = ConfigOptions.key("db_table")
            .stringType()
            .noDefaultValue();

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        final ReadableConfig options = helper.getOptions();
        String dbPath = options.get(DB_PATH);
        String dbTable = options.get(DB_TABLE);
        DataType rowType = context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();
        return new TsdbSink(dbPath, dbTable, rowType);
    }

    @Override
    public String factoryIdentifier() {
        return FACTORY_IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DB_PATH);
        options.add(DB_TABLE);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>();
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        final ReadableConfig options = helper.getOptions();
        String dbPath = options.get(DB_PATH);
        String dbTable = options.get(DB_TABLE);
        DataType rowType = context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();
        return new TsdbSource(dbPath, dbTable, rowType);
    }
}
