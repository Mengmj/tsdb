package com.alibaba.lindorm.contest.custom;

import com.alibaba.lindorm.contest.CommonUtils;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.CompareExpression;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.Vin;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class RawDataFile extends DataFile{
    private static final int MAXIMUM_SIZE = 256 * 1024 * 1024;

    private static final int ID_TEXT_TAILS = 0+Integer.BYTES;
    private static final int HEADER_SIZE = 4*1024;
    //header后为各id各秒的有效标志位
    private static final int FLAG_BIT_MAP_BEGIN_ADDR = HEADER_SIZE;
    private static final int FLAG_BIT_MAP_SIZE = CommonUtils.PARTITION_SECONDS * CommonUtils.BUCKLE_SIZE / 8;
    //定长数据区起始地址
    private final int DATA_BEGIN_ADDR = FLAG_BIT_MAP_BEGIN_ADDR+FLAG_BIT_MAP_SIZE;

    //文本数据区的起始地址
    private final int TEXT_BEGIN_ADDR;

    //文本数据区分段大小 128K
    private final int TEXT_SEGMENT_SIZE = 128 * 1024;

    //下一个待分配文本段的起始地址,新建文件时为指向定长数据区的尾部,读取文件时指向新文件的末尾.实际是整个文件的尾部
    private final AtomicInteger nextTextSegBegin;

    private MappedByteBuffer mbb;
    private final RandomAccessFile randomAccessFile;
    private final FileChannel fc;
    private final int MILLIS_OFFSET = 0;
    //每个id段的大小
    private final int SEGMENT_SIZE;
    //各行在id段内的偏移量
    private final Map<String,Integer> columnOffsetMap;

    //各个id的锁
    private final AtomicBoolean[] locks;
    //各个id下一个字符串起始位置,为0说明未分配内存段
    private final int[] nextStringBeginAddr;
    private RawDataFile(File file,long partition,int buckle,InternalSchema schema){
        super(file,partition,buckle,schema);
        locks = new AtomicBoolean[CommonUtils.BUCKLE_SIZE];
        for(int i = 0;i < CommonUtils.BUCKLE_SIZE;++i){
            locks[i] = new AtomicBoolean(false);
        }
        nextStringBeginAddr = new int[CommonUtils.BUCKLE_SIZE];

        //每个id块起始位置为每秒标志位,其后为毫秒数
        int prevEnd = MILLIS_OFFSET + Short.BYTES * CommonUtils.PARTITION_SECONDS;
        columnOffsetMap = new HashMap<>();
        for(String colName: schema.colNames){
            int offset = prevEnd;
            columnOffsetMap.put(colName,offset);
            switch (schema.getType(colName)){
                case COLUMN_TYPE_INTEGER:
                    prevEnd += Integer.BYTES * CommonUtils.PARTITION_SECONDS;
                    break;
                case COLUMN_TYPE_DOUBLE_FLOAT:
                    prevEnd += Double.BYTES * CommonUtils.PARTITION_SECONDS;
                    break;
                case COLUMN_TYPE_STRING:
                    prevEnd += (Integer.BYTES + Short.BYTES) * CommonUtils.PARTITION_SECONDS;
                    break;
            }
        }
        SEGMENT_SIZE = prevEnd;
        TEXT_BEGIN_ADDR = DATA_BEGIN_ADDR + SEGMENT_SIZE * CommonUtils.BUCKLE_SIZE;
        try{
            randomAccessFile = new RandomAccessFile(file,"rw");
            fc = randomAccessFile.getChannel();
            boolean newFile = !file.exists() || fc.size()==0;
            mbb = fc.map(FileChannel.MapMode.READ_WRITE,0,MAXIMUM_SIZE);
            if(newFile){
                nextTextSegBegin = new AtomicInteger(TEXT_BEGIN_ADDR);
            }else{
                nextTextSegBegin = new AtomicInteger((int)fc.size());
                //读取header中的信息
                for(int addr = ID_TEXT_TAILS,i = 0;i < CommonUtils.BUCKLE_SIZE;++i,addr+=Integer.BYTES){
                    nextStringBeginAddr[i] = mbb.getInt(addr);
                }
            }
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }
    public static RawDataFile getInstance(File file,FileKey fileKey,InternalSchema schema){
        return new RawDataFile(file,fileKey.partition,fileKey.buckle,schema);
    }

    private int getSecond(long timestamp) {
        return (int) ((timestamp / 1000) % CommonUtils.PARTITION_SECONDS);
    }
    private long getTimestamp(int id,int second){
        int idSegBeginAddr = getIdBeginAddr(id);
        short millis = mbb.getShort(idSegBeginAddr+second*Short.BYTES);
        return (partition * CommonUtils.PARTITION_SECONDS + second) * 1000 + millis;
    }

    public void writeRows(List<RowWrapped> rows){
        for(RowWrapped rowWrapped: rows){
            int id = rowWrapped.id;
            Row row = rowWrapped.row;
            int second = getSecond(row.getTimestamp());
            int no = id % CommonUtils.BUCKLE_SIZE;
            setMillis(id,row.getTimestamp());
            for(String colName: schema.numericColumns){
                ColumnValue value = row.getColumns().get(colName);
                if(value!=null){
                    setColumn(id,second,colName,value);
                }
            }
            while(locks[no].getAndSet(true));
            for(String colName: schema.strColumns){
                ColumnValue value = row.getColumns().get(colName);
                if(value!=null){
                    setColumn(id,second,colName,value);
                }
            }
            setValidFlag(id,second);
            locks[no].set(false);
        }
    }

    public ArrayList<Row> readRows(Vin vin, int id, Set<String> colNames, long timeLowerBound, long timeupperBound){
        ArrayList<Row> ret = new ArrayList<>();
        ArrayList<Long> tsList = new ArrayList<>();
        ArrayList<Integer> secondList = getValidSeconds(id,timeLowerBound,timeupperBound);
        for(int second:secondList){
            tsList.add(getTimestamp(id,second));
        }
        Map<String,ArrayList<ColumnValue>> columnListMap = new HashMap<>();
        for(String colName: colNames){
            columnListMap.put(colName,new ArrayList<>());
            for(int second: secondList){
                columnListMap.get(colName).add(readColumn(id,second,colName));
            }
        }
        for(int i = 0;i < tsList.size();++i){
            Map<String, ColumnValue> columns = new HashMap<>();
            for(String colName: colNames){
                columns.put(colName,columnListMap.get(colName).get(i));
            }
            ret.add(new Row(vin,tsList.get(i),columns));
        }
        return ret;
    }

    public AggResult aggColumn(int id,String colName,long timeLowerBound,long timeUpperBound){
        AggResult aggResult = new AggResult(schema.getType(colName));
        ArrayList<Integer> secondList = getValidSeconds(id,timeLowerBound,timeUpperBound);
        for(int second: secondList){
            ColumnValue value = readColumn(id,second,colName);
            switch (value.getColumnType()){
                case COLUMN_TYPE_INTEGER:
                    aggResult.addInt(value.getIntegerValue());
                    break;
                case COLUMN_TYPE_DOUBLE_FLOAT:
                    aggResult.addDouble(value.getDoubleFloatValue());
                    break;
            }
        }
        return aggResult;
    }

    public AggResult aggColumn(int id, String colName, long timeLowerBound, long timeUpperBound, CompareExpression expression){
        if(expression==null){
            return aggColumn(id,colName,timeLowerBound,timeUpperBound);
        }
        ColumnValue.ColumnType columnType = schema.getType(colName);
        AggResult aggResult = new AggResult(columnType);
        ArrayList<Integer> secondList = getValidSeconds(id,timeLowerBound,timeUpperBound);
        for(int second: secondList){
            ColumnValue value = readColumn(id,second,colName);
            if(expression.doCompare(value)){
                if(columnType== ColumnValue.ColumnType.COLUMN_TYPE_INTEGER){
                    aggResult.addInt(value.getIntegerValue());
                }
                if(columnType== ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT){
                    aggResult.addDouble(value.getDoubleFloatValue());
                }
                if(expression.getCompareOp()== CompareExpression.CompareOp.EQUAL){
                    return aggResult;
                }
            }else {
                aggResult.addInvalid();
            }
        }
        return aggResult;
    }
    private int getIdBeginAddr(int id){
        return (id%CommonUtils.BUCKLE_SIZE)*SEGMENT_SIZE + DATA_BEGIN_ADDR;
    }

    private boolean getValidFlag(int id,int second){
        int no = id % CommonUtils.BUCKLE_SIZE;
        int seq = no * CommonUtils.PARTITION_SECONDS + second;
        //在哪个字节
        int byteIdx = seq / 8;
        byte mask = (byte) (1 << (seq - byteIdx * 8));
        byte b = mbb.get(FLAG_BIT_MAP_BEGIN_ADDR+byteIdx);
        return (b & mask)!=0;
    }

    /**
     * 设置标志位需要持有id锁,注意不同id的标志位不能在同一个byte中,这要求分区的秒数为8的倍数
     * @param id
     * @param second
     */
    private void setValidFlag(int id,int second){
        int no = id % CommonUtils.BUCKLE_SIZE;
        int seq = no * CommonUtils.PARTITION_SECONDS + second;
        //在哪个字节
        int byteIdx = seq / 8;
        byte mask = (byte) (1 << (seq - byteIdx * 8));
        byte b = mbb.get(FLAG_BIT_MAP_BEGIN_ADDR+byteIdx);
        b |= mask;
        mbb.put(FLAG_BIT_MAP_BEGIN_ADDR+byteIdx,b);
    }

    private void setMillis(int id,long timestamp){
        int second = getSecond(timestamp);
        int idSegBeginAddr = getIdBeginAddr(id);
        short millis = (short) (timestamp % 1000);
        mbb.putShort(idSegBeginAddr+second*Short.BYTES,millis);

    }
    private int getColumnAddr(int id,int second,String colName){
        int idSegBeginAddr = getIdBeginAddr(id);
        int columnOffset = columnOffsetMap.get(colName);
        int typeLen = 0;
        switch (schema.getType(colName)){
            case COLUMN_TYPE_STRING:
                typeLen = Integer.BYTES+Short.BYTES;
                break;
            case COLUMN_TYPE_INTEGER:
                typeLen = Integer.BYTES;
                break;
            case COLUMN_TYPE_DOUBLE_FLOAT:
                typeLen = Double.BYTES;
                break;
        }
        return idSegBeginAddr + columnOffset + typeLen * second;
    }

    private void setColumn(int id,int second,String colName, ColumnValue columnValue){
        int no = id % CommonUtils.BUCKLE_SIZE;
        int addr = getColumnAddr(id,second,colName);
        switch (schema.getType(colName)){
            case COLUMN_TYPE_INTEGER:
                mbb.putInt(addr,columnValue.getIntegerValue());
                break;
            case COLUMN_TYPE_DOUBLE_FLOAT:
                mbb.putDouble(addr,columnValue.getDoubleFloatValue());
                break;
            case COLUMN_TYPE_STRING:
                ByteBuffer buffer = columnValue.getStringValue();
                short size = (short) buffer.remaining();
                int strAddr = nextStringBeginAddr[no];
                if(strAddr==0 || !sameTextSegment(strAddr,strAddr+size)){ //无可用空间或者空间不足
                    strAddr = nextTextSegBegin.getAndAdd(TEXT_SEGMENT_SIZE);
                }
                nextStringBeginAddr[no] = strAddr+size;
                mbb.putInt(addr,strAddr);
                mbb.putShort(addr+Integer.BYTES,size);
                while(buffer.remaining()>0){
                    mbb.put(strAddr,buffer.get());
                    strAddr++;
                }
                break;
        }
    }

    private boolean sameTextSegment(int addr1,int addr2){
        int seg1 = (addr1-TEXT_BEGIN_ADDR)/TEXT_SEGMENT_SIZE;
        int seg2 = (addr2-TEXT_BEGIN_ADDR)/TEXT_SEGMENT_SIZE;
        return seg1==seg2;
    }

    public void close() throws IOException {
        //修改header中的信息
        for(int addr = ID_TEXT_TAILS,i = 0;i < CommonUtils.BUCKLE_SIZE;++i,addr+=Integer.BYTES){
            mbb.putInt(addr,nextStringBeginAddr[i]);
        }
        mbb = null;
        fc.truncate(nextTextSegBegin.get());
        fc.close();
        randomAccessFile.close();
    }

    private ColumnValue readColumn(int id,int second, String colName){
        int addr = getColumnAddr(id,second,colName);
        ColumnValue value = null;
        switch (schema.getType(colName)){
            case COLUMN_TYPE_DOUBLE_FLOAT:
                value = new ColumnValue.DoubleFloatColumn(mbb.getDouble(addr));
                break;
            case COLUMN_TYPE_INTEGER:
                value = new ColumnValue.IntegerColumn(mbb.getInt(addr));
                break;
            case COLUMN_TYPE_STRING:
                int strAddr = mbb.getInt(addr);
                short size = mbb.getShort(addr+Integer.BYTES);
                ByteBuffer buffer = ByteBuffer.allocate(size);
                for(int i = 0;i < size;++i){
                    buffer.put(mbb.get(strAddr+i));
                }
                buffer.position(0);
                value = new ColumnValue.StringColumn(buffer);
                break;
        }
        return value;
    }
    private ArrayList<Integer> getValidSeconds(int id,long timeLowerBound,long timeUpperBound){
        int firstSecond = getSecond(timeLowerBound);
        int lastSecond = getSecond(timeUpperBound-1);
        ArrayList<Integer> secondList = new ArrayList<>();
        if(getValidFlag(id,firstSecond) && getTimestamp(id,firstSecond) >= timeLowerBound){
            secondList.add(firstSecond);
        }
        for(int second = firstSecond+1;second<lastSecond;++second){
            if(getValidFlag(id,second)){
                secondList.add(second);
            }
        }
        if(getValidFlag(id,lastSecond) && getTimestamp(id,lastSecond) < timeUpperBound){
            secondList.add(lastSecond);
        }
        return secondList;
    }
}
