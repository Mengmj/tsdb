package com.alibaba.lindorm.contest.custom;

import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.test.TestUtils;

public class AggResult{
    private int count;
    private long intSum;
    private int intMax;
    private double doubleSum;
    private double doubleMax;

    private ColumnValue.ColumnType valueType;
    private AggResult(){};
    public AggResult(ColumnValue.ColumnType valueType){
        TestUtils.check(valueType == ColumnValue.ColumnType.COLUMN_TYPE_INTEGER
                || valueType == ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT);
        this.valueType = valueType;
        count = -1;
    }

    public double getAvg(){
        if(count==0){
            return Double.NEGATIVE_INFINITY;
        }
        if(valueType== ColumnValue.ColumnType.COLUMN_TYPE_INTEGER){
            return ((double) intSum) / count;
        }else{
            return doubleSum / count;
        }
    }

    public int getIntMax(){
        TestUtils.check(valueType== ColumnValue.ColumnType.COLUMN_TYPE_INTEGER);
        if(count==0){
            return 0x80000000;
        }else{
            return intMax;
        }
    }

    public double getDoubleMax(){
        TestUtils.check(valueType== ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT);
        if(count==0){
            return Double.NEGATIVE_INFINITY;
        }
        return doubleMax;
    }
    public void addInt(int num){
        TestUtils.check(valueType== ColumnValue.ColumnType.COLUMN_TYPE_INTEGER);
        if(count==-1){
            count = 0;
        }
        count++;
        intSum += num;
        intMax = Math.max(num, intMax);
    }
    public void addDouble(double num){
        TestUtils.check(valueType== ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT);
        if(count==-1){
            count = 0;
        }
        count++;
        doubleSum += num;
        doubleMax = Math.max(num,doubleMax);
    }

    public AggResult merge(AggResult other){
        TestUtils.check(valueType==other.valueType);
        if(count==-1 && other.count==-1){
            return this;
        }
        if(count==-1){
            return other;
        }
        if (other.count==-1){
            return this;
        }
        count += other.count;
        intSum += other.intSum;
        intMax = Math.max(intMax,other.intMax);
        doubleSum += other.doubleSum;
        doubleMax = Math.max(doubleMax,other.doubleMax);
        return this;
    }
}
