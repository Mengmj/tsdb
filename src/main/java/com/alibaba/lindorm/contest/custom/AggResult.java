package com.alibaba.lindorm.contest.custom;

import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.test.TestUtils;

import java.util.List;

public class AggResult{
    //有效数据条数
    private int count;
    //无效数据条数
    private int invalid;
    private long intSum;
    private int intMax;
    private double doubleSum;
    private double doubleMax;

    private ColumnValue.ColumnType valueType;
    private AggResult(){
        intMax = Integer.MIN_VALUE;
        doubleMax = Double.NEGATIVE_INFINITY;
    };
    public AggResult(ColumnValue.ColumnType valueType){
        this();
        TestUtils.check(valueType == ColumnValue.ColumnType.COLUMN_TYPE_INTEGER
                || valueType == ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT);
        this.valueType = valueType;
    }

    public Double getAvg(){
        TestUtils.check(!isEmpty());
        if(count==0){
            return Double.NEGATIVE_INFINITY;
        }
        if(valueType== ColumnValue.ColumnType.COLUMN_TYPE_INTEGER){
            return ((double) intSum) / count;
        }else{
            return doubleSum / count;
        }
    }

    public Integer getIntMax(){
        //TestUtils.check(valueType== ColumnValue.ColumnType.COLUMN_TYPE_INTEGER);
        TestUtils.check(!isEmpty());
        if(count==0){
            return 0x80000000;
        }else{
            return intMax;
        }
    }

    public Double getDoubleMax(){
        //TestUtils.check(valueType== ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT);
        TestUtils.check(!isEmpty());
        if(count==0){
            return Double.NEGATIVE_INFINITY;
        }
        return doubleMax;
    }
    public void addInt(int num){
        TestUtils.check(valueType== ColumnValue.ColumnType.COLUMN_TYPE_INTEGER);
        count++;
        intSum += num;
        intMax = Math.max(num, intMax);
    }
    public void addDouble(double num){
        TestUtils.check(valueType== ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT);
        count++;
        doubleSum += num;
        doubleMax = Math.max(num,doubleMax);
    }

    public void addInvalid(){
        invalid++;
    }

    public AggResult merge(AggResult other){
        TestUtils.check(valueType==other.valueType);
        count += other.count;
        intSum += other.intSum;
        intMax = Math.max(intMax,other.intMax);
        doubleSum += other.doubleSum;
        doubleMax = Math.max(doubleMax,other.doubleMax);
        return this;
    }

    public static AggResult merge(List<AggResult> aggResultList){
        if(aggResultList.size()==0){
            return new AggResult();
        }
        AggResult aggResult = aggResultList.get(0);
        for(int i = 1;i < aggResultList.size();++i){
            aggResult = aggResult.merge(aggResultList.get(i));
        }
        return aggResult;
    }

    public boolean isEmpty(){
        return invalid+count == 0;
    }
}
