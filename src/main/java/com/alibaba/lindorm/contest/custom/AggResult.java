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
    private int intMin;
    private double doubleSum;
    private double doubleMax;
    private double doubleMin;

    private ColumnValue.ColumnType valueType;
    private AggResult(){
        intMax = Integer.MIN_VALUE;
        intMin = Integer.MAX_VALUE;
        doubleMax = Double.NEGATIVE_INFINITY;
        doubleMin = Double.POSITIVE_INFINITY;
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
        intMin = Math.min(num, intMin);
    }
    public void addDouble(double num){
        TestUtils.check(valueType== ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT);
        count++;
        doubleSum += num;
        doubleMax = Math.max(num,doubleMax);
        doubleMin = Math.min(num,doubleMin);
    }

    public void addInvalid(){
        invalid++;
    }

    public AggResult merge(AggResult other){
        count += other.count;
        intSum += other.intSum;
        intMax = Math.max(intMax,other.intMax);
        intMin = Math.min(intMin,other.intMin);
        doubleSum += other.doubleSum;
        doubleMax = Math.max(doubleMax,other.doubleMax);
        doubleMin = Math.min(doubleMin,other.doubleMin);
        invalid += other.invalid;
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

    public ColumnValue.ColumnType getValueType(){
        return valueType;
    }

    public static AggResult newIntAgg(int cnt, long sum,int min,int max){
        AggResult aggResult = new AggResult(ColumnValue.ColumnType.COLUMN_TYPE_INTEGER);
        aggResult.intSum = sum;
        aggResult.intMin = min;
        aggResult.intMax = max;
        aggResult.count = cnt;
        return aggResult;
    }

    public static AggResult newDoubleAgg(int cnt,double sum,double min,double max){
        AggResult aggResult = new AggResult(ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT);
        aggResult.doubleSum = sum;
        aggResult.doubleMin = min;
        aggResult.doubleMax = max;
        aggResult.count = cnt;
        return aggResult;
    }
}
