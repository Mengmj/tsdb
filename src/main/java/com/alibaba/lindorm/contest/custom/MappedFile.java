package com.alibaba.lindorm.contest.custom;

import com.alibaba.lindorm.contest.CommonUtils;
import com.alibaba.lindorm.contest.LogUtils;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.Vin;
import com.alibaba.lindorm.contest.test.TestUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class MappedFile {
    private static final int MAXIMUM_SIZE = 64 * 1024 * 1024;
    private static final int HEADER_SIZE = 1024;
    private MappedByteBuffer mbb;
    private final int DATA_BEGIN = HEADER_SIZE;
    private final int DATA_END;
    private final AtomicInteger textEnd;
    private final InternalSchema schema;
    private final long partition;
    private final int buckle;
    private final RandomAccessFile randomAccessFile;
    private final FileChannel fc;
    public final File file;
    private MappedFile(File file,long partition,int buckle,InternalSchema schema){
        try{
            this.file = file;
            DATA_END = HEADER_SIZE + CommonUtils.PARTITION_SECONDS * CommonUtils.BUCKLE_SIZE * schema.rawLength;
            randomAccessFile = new RandomAccessFile(file,"rw");
            fc = randomAccessFile.getChannel();
            textEnd = new AtomicInteger(Math.max(DATA_END,(int)fc.size()));
            mbb = fc.map(FileChannel.MapMode.READ_WRITE,0,MAXIMUM_SIZE);
            this.schema = schema;
            this.partition = partition;
            this.buckle = buckle;
        }catch (Exception e){
            throw new RuntimeException(e);
        }

    }
    public static MappedFile getInstance(File file,long partition,int buckle,InternalSchema schema){
        return new MappedFile(file,partition,buckle,schema);
    }

    public void writeRows(ArrayList<Integer> ids, ArrayList<Row> rows){
        int n = ids.size();
        if(ids.size()!=rows.size()){
            LogUtils.error("ids.size()!=rows.size()");
            throw new RuntimeException("ids.size()!=rows.size()");
        }
        for(int i = 0;i < n;++i){
            int id = ids.get(i);
            Row row = rows.get(i);
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
                        int strSize = buffer.remaining();
                        int strBegin = textEnd.getAndAdd(strSize);
                        mbb.putInt(offset,strBegin);
                        offset+=Integer.BYTES;
                        mbb.putInt(offset,strSize);
                        offset+=Integer.BYTES;
                        while(buffer.remaining()>0){
                            mbb.put(strBegin,buffer.get());
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
                            int strSize = mbb.getInt(rowBegin+offset+Integer.BYTES);
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
}
