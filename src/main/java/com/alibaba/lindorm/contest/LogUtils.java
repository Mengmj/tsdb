package com.alibaba.lindorm.contest;

import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.CompareExpression;
import com.alibaba.lindorm.contest.structs.TimeRangeDownsampleRequest;

public class LogUtils {
    public static void debug(String format, Object... args){
        System.out.printf("%s DEBUG::%s\n",Thread.currentThread().getName(),String.format(format,args));
    }
    public static void error(String format, Object... args){
        System.out.printf("%s ERROR::%s\n",Thread.currentThread().getName(),String.format(format,args));
    }

    public static String toString(TimeRangeDownsampleRequest timeRangeDownsampleRequest){
        String colName = timeRangeDownsampleRequest.getColumnName();
        ColumnValue value = timeRangeDownsampleRequest.getColumnFilter().getValue();
        ColumnValue.ColumnType type = value.getColumnType();
        CompareExpression.CompareOp op = timeRangeDownsampleRequest.getColumnFilter().getCompareOp();
        String valueString = null;
        switch (type){
            case COLUMN_TYPE_INTEGER:
                valueString = String.valueOf(value.getIntegerValue());
                break;
            case COLUMN_TYPE_DOUBLE_FLOAT:
                valueString = String.valueOf(value.getDoubleFloatValue());
                break;
        }
        return String.format("downsample %s[%s] which %s %s interval %dms from %d to %d",
                colName,type,op,valueString,timeRangeDownsampleRequest.getInterval(),timeRangeDownsampleRequest.getTimeLowerBound(),timeRangeDownsampleRequest.getTimeUpperBound());
    }
}
