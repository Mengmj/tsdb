package com.alibaba.lindorm.contest.custom;

import com.alibaba.lindorm.contest.CommonUtils;
import com.alibaba.lindorm.contest.LogUtils;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.CompareExpression;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.Vin;
import com.alibaba.lindorm.contest.test.TestUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class SubPartDataFile extends DataFile {
    private static final int SUB_PART_SECONDS = 100;
    private static final int SUB_PART_NUM = CommonUtils.PARTITION_SECONDS / SUB_PART_SECONDS;
    private static final int MAXIMUM_SIZE = 256 * 1024 * 1024;
    private static final int ID_TEXT_TAILS = 0+Integer.BYTES;

    private static final int AGG_SEG_BEGIN = Integer.BYTES+Integer.BYTES* CommonUtils.BUCKLE_SIZE;
    private MappedByteBuffer mbb;
    private final int DATA_BEGIN;
    private final int DATA_END;
    //文本数据区的起始地址
    private final int TEXT_BEGIN_ADDR;

    //文本数据区分段大小 128K
    private final int TEXT_SEGMENT_SIZE = 128 * 1024;
    private final AtomicInteger textEnd;
    private final RandomAccessFile randomAccessFile;
    private final FileChannel fc;
    private final AtomicInteger recordCount;
    private final AtomicBoolean[] locks;
    private final int[] textTails;

    //每个字段在该id聚合数据段内的偏移量
    private final ConcurrentMap<String,Integer> columnAggOffset;
    private SubPartDataFile(File file, long partition, int buckle, InternalSchema schema){
        super(file,partition,buckle,schema);
        columnAggOffset = new ConcurrentHashMap<>();
        int offset = Byte.BYTES*SUB_PART_NUM + 2*Long.BYTES*SUB_PART_NUM;
        for(String colName: schema.numericColumns){
            columnAggOffset.put(colName,offset);
            switch (schema.getType(colName)){
                case COLUMN_TYPE_INTEGER:
                    offset += (Long.BYTES+2*Integer.BYTES) * SUB_PART_NUM;
                    break;
                case COLUMN_TYPE_DOUBLE_FLOAT:
                    offset += 3 * Double.BYTES * SUB_PART_NUM;
                    break;
            }
        }
        DATA_BEGIN = AGG_SEG_BEGIN + CommonUtils.BUCKLE_SIZE * SUB_PART_NUM * schema.aggLength;
        DATA_END = DATA_BEGIN + CommonUtils.PARTITION_SECONDS * CommonUtils.BUCKLE_SIZE * schema.rawLength;
        TEXT_BEGIN_ADDR = DATA_END;
        recordCount = new AtomicInteger();
        textEnd = new AtomicInteger();
        locks = new AtomicBoolean[CommonUtils.BUCKLE_SIZE];
        textTails = new int[CommonUtils.BUCKLE_SIZE];
        for(int i = 0;i < locks.length;++i){
            locks[i] = new AtomicBoolean(false);
        }
        try{
            randomAccessFile = new RandomAccessFile(file,"rw");
            fc = randomAccessFile.getChannel();
            int prevSize = (int)fc.size();
            boolean newFile = !file.exists()||prevSize==0;
            mbb = fc.map(FileChannel.MapMode.READ_WRITE,0,MAXIMUM_SIZE);
            if(newFile){
                recordCount.set(0);
                textEnd.set(DATA_END);
                initAggSegment();
                //checkAggSegmet();
            }else {
                int cnt = mbb.getInt(0);
                recordCount.set(cnt);
                textEnd.set(prevSize);
                for(int i = 0,addr = ID_TEXT_TAILS;i < CommonUtils.BUCKLE_SIZE;++i,addr+=Integer.BYTES){
                    textTails[i] = mbb.getInt(addr);
                }
            }

        }catch (Exception e){
            throw new RuntimeException(e);
        }

    }
    public static SubPartDataFile getInstance(File file, FileKey fileKey, InternalSchema schema){
        return new SubPartDataFile(file,fileKey.partition,fileKey.buckle,schema);
    }

    public void writeRows(List<RowWrapped> rows){
        for(RowWrapped rowWrapped: rows){
            int id = rowWrapped.id;
            Row row = rowWrapped.row;
            int second = getSecond(row.getTimestamp());
            int rowBegin = getRowBegin(id,second);
            int offset = rowBegin+1;
            short milli = (short) (row.getTimestamp() % 1000);
            mbb.putShort(offset,milli);
            for(String colName: schema.numericColumns){
                ColumnValue value = row.getColumns().get(colName);
                offset = schema.getOffset(colName);
                switch (schema.getType(colName)){
                    case COLUMN_TYPE_INTEGER:
                        mbb.putInt(rowBegin+offset, value.getIntegerValue());
                        break;
                    case COLUMN_TYPE_DOUBLE_FLOAT:
                        mbb.putDouble(rowBegin+offset, value.getDoubleFloatValue());
                        break;
                    default:
                        throw new RuntimeException("unexpected column type in numeric columns");
                }
            }
            int no = id % CommonUtils.BUCKLE_SIZE;
            while (locks[no].getAndSet(true));
            for(String colName: schema.strColumns){
                ByteBuffer buffer = row.getColumns().get(colName).getStringValue();
                int addr = rowBegin+schema.getOffset(colName);
                short size = (short) buffer.remaining();
                int strAddr = textTails[no];
                if(strAddr==0 || !sameTextSegment(strAddr,strAddr+size)){ //无可用空间或者空间不足
                    strAddr = textEnd.getAndAdd(TEXT_SEGMENT_SIZE);
                }
                textTails[no] = strAddr+size;
                mbb.putInt(addr,strAddr);
                mbb.putShort(addr+Integer.BYTES,size);
                while(buffer.remaining()>0){
                    mbb.put(strAddr,buffer.get());
                    strAddr++;
                }
            }
            //更新聚合信息
            int base = getAggSegBeginByNo(no);
            int subPart = second / SUB_PART_SECONDS;
            byte count = mbb.get(base+subPart*Byte.BYTES);
            count++;
            mbb.put(base+subPart*Byte.BYTES,count);
            base += Byte.BYTES * SUB_PART_NUM;
            long minTs = mbb.getLong(base+2*Long.BYTES*subPart);
            long maxTs = mbb.getLong(base+2*Long.BYTES*subPart+Long.BYTES);
            if(row.getTimestamp()<minTs){
                mbb.putLong(base+2*Long.BYTES*subPart,row.getTimestamp());
            }
            if(row.getTimestamp()>maxTs){
                mbb.putLong(base+2*Long.BYTES*subPart+Long.BYTES,row.getTimestamp());
            }
            for(String colName: schema.numericColumns){
                switch (schema.getType(colName)){
                    case COLUMN_TYPE_INTEGER:
                        int integerValue = row.getColumns().get(colName).getIntegerValue();
                        int intSumAddr = getAggSegBeginByNo(no)+columnAggOffset.get(colName)+subPart*InternalSchema.INT_AGG_LEN;
                        int intMinAddr = intSumAddr + Long.BYTES;
                        int intMaxAddr = intMinAddr + Integer.BYTES;
                        long sum = mbb.getLong(intSumAddr);
                        sum += integerValue;
                        mbb.putLong(intSumAddr,sum);
                        int min = mbb.getInt(intMinAddr);
                        int max = mbb.getInt(intMaxAddr);
                        if(integerValue<min){
                            mbb.putInt(intMinAddr,integerValue);
                        }
                        if(integerValue>max){
                            mbb.putInt(intMaxAddr,integerValue);
                        }
                        break;
                    case COLUMN_TYPE_DOUBLE_FLOAT:
                        int doubleSumAddr = getAggSegBeginByNo(no)+columnAggOffset.get(colName)+subPart*InternalSchema.DOUBLE_AGG_LEN;
                        int doubleMinAddr = doubleSumAddr + Double.BYTES;
                        int doubleMaxAddr = doubleMinAddr + Double.BYTES;
                        double doubleValue = row.getColumns().get(colName).getDoubleFloatValue();
                        double doubleSum = mbb.getDouble(doubleSumAddr);
                        doubleSum += doubleValue;
                        mbb.putDouble(doubleSumAddr,doubleSum);
                        double doubleMin = mbb.getDouble(doubleMinAddr);
                        double doubleMax = mbb.getDouble(doubleMaxAddr);
                        if(doubleValue < doubleMin){
                            mbb.putDouble(doubleMinAddr,doubleValue);
                        }
                        if(doubleValue > doubleMax){
                            mbb.putDouble(doubleMaxAddr,doubleValue);
                        }
                        break;
                }
            }
            locks[no].set(false);
            mbb.put(rowBegin,(byte) 1);
            recordCount.incrementAndGet();
        }
    }

    public ArrayList<Row> readRows(Vin vin, int id, Set<String> colNames, long timeLowerBound, long timeupperBound){
        ArrayList<Row> ret = new ArrayList<>();
        int beginSecond = getSecond(timeLowerBound);
        int endSecond = getSecond(timeupperBound-1);
        if(beginSecond > endSecond){
            LogUtils.error("beginSecond > endSecond");
            throw new RuntimeException("beginSecond > endSecond");
        }
        for(int second = beginSecond;second <= endSecond;++second){
            int rowBegin = getRowBegin(id,second);
            byte flag = mbb.get(rowBegin);
            short milli = mbb.getShort(rowBegin+1);
            if(flag!=0){
                long timestamp = getTimestamp(second,milli);
                if(second == beginSecond && timestamp < timeLowerBound){
                    continue;
                }
                if(second == endSecond && timestamp >= timeupperBound){
                    continue;
                }
                Map<String,ColumnValue> columns = new HashMap<>();
                ColumnValue.ColumnType colType;
                int offset;
                for(String colName: colNames){
                    colType = schema.getType(colName);
                    offset = schema.getOffset(colName);
                    switch (colType){
                        case COLUMN_TYPE_INTEGER:
                            columns.put(colName,new ColumnValue.IntegerColumn(mbb.getInt(rowBegin+offset)));
                            break;
                        case COLUMN_TYPE_DOUBLE_FLOAT:
                            columns.put(colName,new ColumnValue.DoubleFloatColumn(mbb.getDouble(rowBegin+offset)));
                            break;
                        case COLUMN_TYPE_STRING:
                            int strBegin = mbb.getInt(rowBegin+offset);
                            short strSize = mbb.getShort(rowBegin+offset+Integer.BYTES);
                            byte[] bytes = new byte[strSize];
                            for(int i = 0;i < strSize;++i){
                                bytes[i] = mbb.get(strBegin+i);
                            }
                            ColumnValue value = new ColumnValue.StringColumn(ByteBuffer.wrap(bytes));
                            columns.put(colName,value);
                            break;
                    }
                }
                ret.add(new Row(vin,timestamp,columns));
            }
        }
        return ret;
    }

    public void close() throws IOException {
        mbb.putInt(0,recordCount.get());
        for(int i = 0,addr = ID_TEXT_TAILS;i < CommonUtils.BUCKLE_SIZE;++i,addr+=Integer.BYTES){
            mbb.putInt(addr,textTails[i]);
        }
        mbb = null;
        fc.truncate(textEnd.get());
        fc.close();
        randomAccessFile.close();
    }

    private int getSegNum(int id){
        return id % CommonUtils.BUCKLE_SIZE;
    }
    private int getRowBegin(int id,int second){
        return DATA_BEGIN+(getSegNum(id)*CommonUtils.PARTITION_SECONDS+second)*schema.rawLength;
    }
    private int getSecond(long timestamp) {
        return (int) ((timestamp / 1000) % CommonUtils.PARTITION_SECONDS);
    }
    private long getTimestamp(int second,short milli){
        return partition * CommonUtils.PARTITION_SECONDS*1000+second*1000 + milli;
    }

    @Override
    public AggResult aggColumn(int id, String colName, long timeLowerBound, long timeUpperBound) {
        List<AggResult> aggResults = new ArrayList<>();
        long lower = timeLowerBound;
        int no = id % CommonUtils.BUCKLE_SIZE;
        while (lower < timeUpperBound){
            int subPart = getSecond(lower) / SUB_PART_SECONDS;
            long upper = Math.min(timeUpperBound,getSubPartEndTs(subPart));
            int aggBegin = getAggSegBeginByNo(no);
            int cnt = mbb.get(aggBegin+subPart*Byte.BYTES);
            if(cnt!=0){ //子分区有记录
                //每个字段的起始地址
                int base = aggBegin + SUB_PART_NUM * Byte.BYTES;
                long minTs = mbb.getLong(base+subPart*2*Long.BYTES);
                long maxTs = mbb.getLong(base+subPart*2*Long.BYTES+Long.BYTES);
                if(lower<=minTs && maxTs < upper) {
                    base = aggBegin + columnAggOffset.get(colName);
                    switch (schema.getType(colName)) {
                        case COLUMN_TYPE_INTEGER:
                            long intSum = mbb.getLong(base + subPart * InternalSchema.INT_AGG_LEN);
                            int intMin = mbb.getInt(base + subPart * InternalSchema.INT_AGG_LEN+Long.BYTES);
                            int intMax = mbb.getInt(base + subPart * InternalSchema.INT_AGG_LEN+Long.BYTES+Integer.BYTES);
                            aggResults.add(AggResult.newIntAgg(cnt,intSum,intMin,intMax));
                            break;
                        case COLUMN_TYPE_DOUBLE_FLOAT:
                            double doubleSum = mbb.getDouble(base+subPart*InternalSchema.DOUBLE_AGG_LEN);
                            double doubleMin = mbb.getDouble(base+subPart*InternalSchema.DOUBLE_AGG_LEN+Double.BYTES);
                            double doubleMax = mbb.getDouble(base+subPart*InternalSchema.DOUBLE_AGG_LEN+Double.BYTES*2);
                            aggResults.add(AggResult.newDoubleAgg(cnt,doubleSum,doubleMin,doubleMax));
                            break;
                    }
                }else{
                    aggResults.add(aggColumn0(id,colName,lower,upper));
                }
            }
            lower = upper;
        }
        return AggResult.merge(aggResults);
    }
    private long getSubPartEndTs(int subPart){
        return (subPart+1)*SUB_PART_SECONDS*1000 + partition*CommonUtils.PARTITION_SECONDS*1000;
    }

    //不使用预聚合信息
    public AggResult aggColumn0(int id,String colName,long timeLowerBound,long timeUpperBound){
        int colOffset = schema.getOffset(colName);
        ColumnValue.ColumnType colType = schema.getType(colName);
        AggResult aggResult = new AggResult(colType);
        int beginSecond = getSecond(timeLowerBound);
        int endSecond = getSecond(timeUpperBound-1);
        TestUtils.check(beginSecond <= endSecond);
        for(int second = beginSecond;second <= endSecond;++second){
            int rowBegin = getRowBegin(id,second);
            byte flag = mbb.get(rowBegin);
            short milli = mbb.getShort(rowBegin+1);
            if(flag!=0){
                long timestamp = getTimestamp(second,milli);
                if(second == beginSecond && timestamp < timeLowerBound){
                    continue;
                }
                if(second == endSecond && timestamp >= timeUpperBound){
                    continue;
                }
                switch (colType){
                    case COLUMN_TYPE_INTEGER:
                        aggResult.addInt(mbb.getInt(rowBegin+colOffset));
                        break;
                    case COLUMN_TYPE_DOUBLE_FLOAT:
                        aggResult.addDouble(mbb.getDouble(rowBegin+colOffset));
                        break;
                    default:
                        throw new RuntimeException("not supported type for aggregation");
                }
            }
        }
        return aggResult;
    }

    private AggResult aggColumnEqual(int id,String colName,long timeLowerBound,long timeUpperBound,ColumnValue target){
        ColumnValue.ColumnType type = target.getColumnType();
        int intTarget = 0;
        double doubleTarget = 0.0;
        if(type == ColumnValue.ColumnType.COLUMN_TYPE_INTEGER){
            intTarget = target.getIntegerValue();
        }else{
            doubleTarget = target.getDoubleFloatValue();
        }
        int colOffset = schema.getOffset(colName);
        ColumnValue.ColumnType colType = schema.getType(colName);
        AggResult aggResult = new AggResult(colType);
        int beginSecond = getSecond(timeLowerBound);
        int endSecond = getSecond(timeUpperBound-1);
        TestUtils.check(beginSecond <= endSecond);

        OUT:
        for(int second = beginSecond;second <= endSecond;++second){
            int rowBegin = getRowBegin(id,second);
            byte flag = mbb.get(rowBegin);
            short milli = mbb.getShort(rowBegin+1);
            if(flag!=0){
                long timestamp = getTimestamp(second,milli);
                if(second == beginSecond && timestamp < timeLowerBound){
                    continue;
                }
                if(second == endSecond && timestamp >= timeUpperBound){
                    continue;
                }
                switch (colType){
                    case COLUMN_TYPE_INTEGER:
                        int intValue = mbb.getInt(rowBegin+colOffset);
                        if(intValue==intTarget){
                            aggResult.addInt(intValue);
                            break OUT;
                        }else {
                            aggResult.addInvalid();
                        }
                        break;
                    case COLUMN_TYPE_DOUBLE_FLOAT:
                        double doubleValue = mbb.getDouble(rowBegin+colOffset);
                        if(doubleValue==doubleTarget){
                            aggResult.addDouble(doubleValue);
                            break OUT;
                        }else {
                            aggResult.addInvalid();
                        }
                        break;
                    default:
                        throw new RuntimeException("not supported type for aggregation");
                }
            }
        }
        return aggResult;
    }

    private AggResult aggColumnGreater(int id,String colName,long timeLowerBound,long timeUpperBound,ColumnValue target){
        ColumnValue.ColumnType type = target.getColumnType();
        int intTarget = 0;
        double doubleTarget = 0.0;
        if(type == ColumnValue.ColumnType.COLUMN_TYPE_INTEGER){
            intTarget = target.getIntegerValue();
        }else{
            doubleTarget = target.getDoubleFloatValue();
        }
        int colOffset = schema.getOffset(colName);
        ColumnValue.ColumnType colType = schema.getType(colName);
        AggResult aggResult = new AggResult(colType);
        int beginSecond = getSecond(timeLowerBound);
        int endSecond = getSecond(timeUpperBound-1);
        TestUtils.check(beginSecond <= endSecond);
        for(int second = beginSecond;second <= endSecond;++second){
            int rowBegin = getRowBegin(id,second);
            byte flag = mbb.get(rowBegin);
            short milli = mbb.getShort(rowBegin+1);
            if(flag!=0){
                long timestamp = getTimestamp(second,milli);
                if(second == beginSecond && timestamp < timeLowerBound){
                    continue;
                }
                if(second == endSecond && timestamp >= timeUpperBound){
                    continue;
                }
                switch (colType){
                    case COLUMN_TYPE_INTEGER:
                        int intValue = mbb.getInt(rowBegin+colOffset);
                        if(intValue>intTarget){
                            aggResult.addInt(intValue);
                        }else {
                            aggResult.addInvalid();
                        }
                        break;
                    case COLUMN_TYPE_DOUBLE_FLOAT:
                        double doubleValue = mbb.getDouble(rowBegin+colOffset);
                        if(doubleValue>doubleTarget){
                            aggResult.addDouble(doubleValue);
                        }else {
                            aggResult.addInvalid();
                        }
                        break;
                    default:
                        throw new RuntimeException("not supported type for aggregation");
                }
            }
        }
        return aggResult;
    }

    public AggResult aggColumn(int id, String colName, long timeLowerBound, long timeUpperBound, CompareExpression expression){
        if(expression==null){
            return aggColumn(id,colName,timeLowerBound,timeUpperBound);
        }
        switch (expression.getCompareOp()){
            case EQUAL:
                return aggColumnEqual(id,colName,timeLowerBound,timeUpperBound,expression.getValue());
            case GREATER:
                return aggColumnGreater(id,colName,timeLowerBound,timeUpperBound, expression.getValue());
            default:
                throw new RuntimeException("not supported agg op");
        }
    }
    private boolean sameTextSegment(int addr1,int addr2){
        int seg1 = (addr1-TEXT_BEGIN_ADDR)/TEXT_SEGMENT_SIZE;
        int seg2 = (addr2-TEXT_BEGIN_ADDR)/TEXT_SEGMENT_SIZE;
        return seg1==seg2;
    }

    private void initAggSegment(){
        for(int no = 0; no < CommonUtils.BUCKLE_SIZE;++no){
            int segBegin = getAggSegBeginByNo(no);
            int addr =segBegin +Byte.BYTES*SUB_PART_NUM;
            for(int subPart = 0;subPart < SUB_PART_NUM;++subPart,addr+=Long.BYTES*2){
                mbb.putLong(addr,Long.MAX_VALUE);
                mbb.putLong(addr+Long.BYTES,Long.MIN_VALUE);
            }
            for(String colName: schema.numericColumns){
                ColumnValue.ColumnType type = schema.getType(colName);
                for(int subPart = 0;subPart < SUB_PART_NUM;++subPart){
                    switch (type){
                        case COLUMN_TYPE_INTEGER:
                            int intSumAddr = getAggSegBeginByNo(no)+columnAggOffset.get(colName)+subPart*InternalSchema.INT_AGG_LEN;
                            int intMinAddr = intSumAddr + Long.BYTES;
                            int intMaxAddr = intMinAddr + Integer.BYTES;
                            mbb.putLong(intSumAddr,0);
                            mbb.putInt(intMinAddr,Integer.MAX_VALUE);
                            mbb.putInt(intMaxAddr,Integer.MIN_VALUE);
                            break;
                        case COLUMN_TYPE_DOUBLE_FLOAT:
                            int doubleSumAddr = getAggSegBeginByNo(no)+columnAggOffset.get(colName)+subPart*InternalSchema.DOUBLE_AGG_LEN;
                            int doubleMinAddr = doubleSumAddr + Double.BYTES;
                            int doubleMaxAddr = doubleMinAddr + Double.BYTES;
                            mbb.putDouble(doubleSumAddr,0.0);
                            mbb.putDouble(doubleMinAddr,Double.POSITIVE_INFINITY);
                            mbb.putDouble(doubleMaxAddr,Double.NEGATIVE_INFINITY);
                            break;
                    }
                }
            }
        }
    }
    private int getAggSegBeginByNo(int no){
        return AGG_SEG_BEGIN + no * SUB_PART_NUM*schema.aggLength;
    }

    //测试方法
    private void checkAggSegmet(){
        for(int no = 0;no < CommonUtils.BUCKLE_SIZE;++no){
            int countBeginAddr = getAggSegBeginByNo(no);
            for(int subPart = 0;subPart < SUB_PART_NUM;++subPart){
                TestUtils.check(mbb.get(countBeginAddr+subPart*Byte.BYTES)==0);
            }
            int tsBeginAddr = countBeginAddr + Byte.BYTES*SUB_PART_NUM;
            for(int subPart = 0;subPart < SUB_PART_NUM;++subPart){
                TestUtils.check(mbb.getLong(tsBeginAddr+subPart*2*Long.BYTES)==Long.MAX_VALUE);
                TestUtils.check(mbb.getLong(tsBeginAddr+subPart*2*Long.BYTES+Long.BYTES)==Long.MIN_VALUE);
            }
            for(String colName:schema.numericColumns){
                int columnAggBeginAddr = getAggSegBeginByNo(no)+columnAggOffset.get(colName);
                ColumnValue.ColumnType type = schema.getType(colName);
                for(int subPart = 0;subPart < SUB_PART_NUM;++subPart){
                    switch (type){
                        case COLUMN_TYPE_INTEGER:
                            int intSumAddr = columnAggBeginAddr+subPart*InternalSchema.INT_AGG_LEN;
                            int intMinAddr = intSumAddr + Long.BYTES;
                            int intMaxAddr = intMinAddr + Integer.BYTES;
                            TestUtils.check(mbb.getLong(intSumAddr)==0);
                            TestUtils.check(mbb.getInt(intMinAddr)==Integer.MAX_VALUE);
                            TestUtils.check(mbb.getInt(intMaxAddr)==Integer.MIN_VALUE);
                            break;
                        case COLUMN_TYPE_DOUBLE_FLOAT:
                            int doubleSumAddr = columnAggBeginAddr+subPart*InternalSchema.DOUBLE_AGG_LEN;
                            int doubleMinAddr = doubleSumAddr + Double.BYTES;
                            int doubleMaxAddr = doubleMinAddr + Double.BYTES;
                            TestUtils.check(mbb.getDouble(doubleSumAddr)==0.0);
                            TestUtils.check(mbb.getDouble(doubleMinAddr)==Double.POSITIVE_INFINITY);
                            TestUtils.check(mbb.getDouble(doubleMaxAddr)==Double.NEGATIVE_INFINITY);
                            break;
                    }
                }
            }
        }
    }
}
