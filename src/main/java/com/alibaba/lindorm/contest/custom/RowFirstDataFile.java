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
import java.util.concurrent.atomic.AtomicInteger;

public class RowFirstDataFile extends DataFile{
    private static final int MAXIMUM_SIZE = 256 * 1024 * 1024;
    private static final int HEADER_SIZE = Integer.BYTES;
    private MappedByteBuffer mbb;
    private final int DATA_BEGIN = HEADER_SIZE;
    private final int DATA_END;

    private final AtomicInteger textEnd;
    private final RandomAccessFile randomAccessFile;
    private final FileChannel fc;
    private final AtomicInteger recordCount;
    private RowFirstDataFile(File file, long partition, int buckle, InternalSchema schema){
        super(file,partition,buckle,schema);
        DATA_END = HEADER_SIZE + CommonUtils.PARTITION_SECONDS * CommonUtils.BUCKLE_SIZE * schema.rawLength;
        recordCount = new AtomicInteger();
        textEnd = new AtomicInteger();
        try{
            randomAccessFile = new RandomAccessFile(file,"rw");
            fc = randomAccessFile.getChannel();
            int prevSize = (int)fc.size();
            boolean newFile = !file.exists()||prevSize==0;
            mbb = fc.map(FileChannel.MapMode.READ_WRITE,0,MAXIMUM_SIZE);
            if(newFile){
                recordCount.set(0);
                textEnd.set(DATA_END);
            }else {
                int cnt = mbb.getInt(0);
                recordCount.set(cnt);
                textEnd.set(prevSize);
            }

        }catch (Exception e){
            throw new RuntimeException(e);
        }

    }
    public static RowFirstDataFile getInstance(File file, FileKey fileKey, InternalSchema schema){
        return new RowFirstDataFile(file,fileKey.partition,fileKey.buckle,schema);
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
            offset+=Short.BYTES;
            for(String colName:schema.colNames){
                ColumnValue colValue = row.getColumns().get(colName);
                ColumnValue.ColumnType colType = schema.getType(colName);
                switch (colType){
                    case COLUMN_TYPE_INTEGER:
                        mbb.putInt(offset,colValue.getIntegerValue());
                        offset+=Integer.BYTES;
                        break;
                    case COLUMN_TYPE_DOUBLE_FLOAT:
                        mbb.putDouble(offset,colValue.getDoubleFloatValue());
                        offset+=Double.BYTES;
                        break;
                    case COLUMN_TYPE_STRING:
                        ByteBuffer buffer = colValue.getStringValue();
                        short strSize = (short)buffer.remaining();
                        int strBegin = textEnd.getAndAdd(strSize);
                        mbb.putInt(offset,strBegin);
                        offset+=Integer.BYTES;
                        mbb.putShort(offset,strSize);
                        offset+=Short.BYTES;
                        while(buffer.remaining()>0){
                            mbb.put(strBegin, buffer.get());
                            strBegin++;
                        }
                }
                TestUtils.check(offset<=DATA_END);
            }
            mbb.put(rowBegin,(byte) 1);
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

    public AggResult aggColumn(int id,String colName,long timeLowerBound,long timeUpperBound){
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

    //测试用
    public void writeBytes(int pos,byte[] bytes){
        for(byte b: bytes){
            mbb.put(pos,b);
            pos++;
        }
    }
    public void writeByte(int pos,byte b){
        byte a = mbb.get(pos);
        TestUtils.check(a==0 || a==b);
        mbb.put(pos,b);
    }
    public byte readByte(int pos){
        return mbb.get(pos);
    }
}
