//
// You should modify this file.
//
// Refer TSDBEngineSample.java to ensure that you have understood
// the interface semantics correctly.
//

package com.alibaba.lindorm.contest;

import com.alibaba.lindorm.contest.custom.InternalSchema;
import com.alibaba.lindorm.contest.custom.RowWritable;
import com.alibaba.lindorm.contest.structs.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TSDBEngineImpl extends TSDBEngine {
  String tableName;
  boolean connected;

  public InternalSchema schema;

  ConcurrentMap<Vin, RowWritable> rowCache;
  final File metaDirectory;
  final File dataDirectory;
  final File cacheDirectory;
  final File schemaFile;
  final File rowCacheFile;

  //按vin分桶
  final static int BUCKET_NUM = 1000;

  final static int BUFFER_QUEUE_NUM = 4;
  final List<BlockingQueue<Row>> bufferQueues;

  final List<RowWriter> writers;

  //用于记录打开的的数据文件数
  public static AtomicInteger openFileCount;
  public static final int OPEN_LIMIT = 5000;

  /**
   * 向buffer中放入row需要获取锁
   * 读取row之前需要检查对应bucket的bufferCount.如果不为0,说明有数据未写入完毕,
   * 此时锁住该bucket,等待写入完毕.
   */
  final AtomicBoolean[] bufferInputLock;

  //记录每个bucket在buffer中的记录数
  final AtomicInteger[] bufferCount;
  /**
   * This constructor's function signature should not be modified.
   * Our evaluation program will call this constructor.
   * The function's body can be modified.
   */
  public TSDBEngineImpl(File dataPath) {
    super(dataPath);
    metaDirectory = new File(dataPath,"meta");
    dataDirectory = new File(dataPath,"data");
    cacheDirectory = new File(dataPath,"cache");
    schemaFile = new File(metaDirectory,"schema.ser");
    rowCacheFile = new File(cacheDirectory,"rows.ser");
    connected = false;
    rowCache = new ConcurrentHashMap<>();

    bufferInputLock = new AtomicBoolean[BUCKET_NUM];
    for(AtomicBoolean lock: bufferInputLock){
      lock = new AtomicBoolean();
    }

    bufferCount = new AtomicInteger[BUCKET_NUM];
    for(AtomicInteger count: bufferCount){
      count = new AtomicInteger();
    }

    bufferQueues = new ArrayList<>(BUFFER_QUEUE_NUM);
    writers = new ArrayList<>(BUFFER_QUEUE_NUM);
    for(int i = 0;i < BUFFER_QUEUE_NUM;++i){
      BlockingQueue<Row> buffer = new LinkedBlockingQueue<>();
      bufferQueues.add(buffer);
      writers.add(new RowWriter(dataDirectory,buffer,schema));
    }

  }

  /**
   * shema文件存在时加载
   * @throws IOException
   */
  @Override
  public void connect() throws IOException {
    if(schemaFile.exists()){
      loadSchema();
      launchWriters();
    }
    if(rowCacheFile.exists()){
      loadCache();
    }
    connected = true;
  }

  @Override
  public void createTable(String tableName, Schema schema) throws IOException {
    this.schema = InternalSchema.build(schema);
    if(!dataDirectory.exists()){
      dataDirectory.mkdirs();
    }
    if(!metaDirectory.exists()){
      metaDirectory.mkdirs();
    }
    if(!dataDirectory.exists()){
      dataDirectory.mkdirs();
    }
    dumpSchema();
    launchWriters();

    //info
    System.out.print(String.format("MMJ_INFO: create table with %d ints, %d doubles and %d strings"
                                    ,this.schema.intCount,this.schema.doubleCount,this.schema.stringCount));
  }

  @Override
  public void shutdown() {
    dumpCache();
    terminateWriters();
  }

  /**
   * 将row存到内存的buffer中
   * @param wReq
   * @throws IOException
   */
  @Override
  public void write(WriteRequest wReq) throws IOException {
    Queue<Row> queue = new LinkedList<>(wReq.getRows());
    while(!queue.isEmpty()){
      Row row = queue.poll();
      Vin vin = row.getVin();
      int bucketNum = getVinSegNum(vin);
      int queueNum = getQueueNum(vin);
      if(bufferInputLock[bucketNum].compareAndExchange(false,true)){
        //vin所在bucket加锁成功
        try{
          //将row放入buffer
          bufferQueues.get(queueNum).put(row);
          //buffer内容数+1
          bufferCount[bucketNum].incrementAndGet();
          //维护缓存
          if(!rowCache.containsKey(vin)){
            rowCache.put(vin,RowWritable.getInstance(row,schema));
          }else if(rowCache.get(vin).getTimestamp()<row.getTimestamp()){
            rowCache.put(vin,RowWritable.getInstance(row,schema));
          }
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        } finally {
          bufferInputLock[bucketNum].set(false);
        }
      }else{
        queue.offer(row);
      }
    }
  }

  @Override
  public ArrayList<Row> executeLatestQuery(LatestQueryRequest pReadReq) throws IOException {
    ArrayList<Row> retList = new ArrayList<>();
    for(Vin vin : pReadReq.getVins()){
      if(rowCache.containsKey(vin)){
        retList.add(filterColumns(rowCache.get(vin).getRow(schema),pReadReq.getRequestedColumns()));
      }
    }
    return retList;
  }

  @Override
  public ArrayList<Row> executeTimeRangeQuery(TimeRangeQueryRequest trReadReq) throws IOException {
    return null;
  }

  @Override public ArrayList<Row> executeAggregateQuery(TimeRangeAggregationRequest aggregationReq) throws IOException {
    return null;
  }

  @Override public ArrayList<Row> executeDownsampleQuery(TimeRangeDownsampleRequest downsampleReq) throws IOException {
    return null;
  }

  void dumpCache(){
    List<RowWritable> list = new ArrayList<>(rowCache.values());
    try(ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(rowCacheFile));){
      oos.writeObject(list);
    }catch (IOException e){
      e.printStackTrace();
      throw new RuntimeException("fail to dump cache");
    }
  }
  void loadCache(){
    rowCache.clear();
    try(ObjectInputStream ois = new ObjectInputStream(new FileInputStream(rowCacheFile))){
      List<RowWritable> list = (ArrayList<RowWritable>) ois.readObject();
      for(RowWritable rowWritable: list){
        rowCache.put(rowWritable.getVin(),rowWritable);
      }
    }catch (Exception e){
      e.printStackTrace();
      throw new RuntimeException("fail to load cache");
    }
  }

  void loadSchema(){
    try(ObjectInputStream ois = new ObjectInputStream(new FileInputStream(schemaFile));){
      schema = (InternalSchema) ois.readObject();
    }catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("could not read schema object");
    }
  }

  void dumpSchema(){
    try(ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(schemaFile))){
      oos.writeObject(schema);
    }catch (IOException e){
      e.printStackTrace();
      throw new RuntimeException("fail to dump schema");
    }
  }
  private static int getVinSegNum(Vin vin){
    return vin.hashCode() % BUCKET_NUM;
  }

  private static Row filterColumns(Row row,Set<String> requested){
    if(requested.isEmpty()){
      return row;
    }
    Map<String,ColumnValue> columes = row.getColumns();
    Map<String,ColumnValue> requestedColumns = new HashMap<>();
    for(String colName: requested){
      requestedColumns.put(colName,columes.get(colName));
    }
    return new Row(row.getVin(),row.getTimestamp(),requestedColumns);
  }
  private static int getQueueNum(Vin vin){
    return vin.hashCode() % BUFFER_QUEUE_NUM;
  }

  /**
   * 获取到schema信息后才会启动写线程
   * 1. connect到已有数据库
   * 2. createTable
   */
  private void launchWriters(){
    for(Thread writer: writers){
      writer.start();
    }
  }

  /**
   * 阻塞等待缓存队列排空,关闭写线程
   */
  private void terminateWriters(){
//    int terminated = 0;
//    while(terminated < BUFFER_QUEUE_NUM){
//      terminated = 0;
//      for(RowWriter writer: writers){
//        if(writer.getState()== Thread.State.TERMINATED){
//          ++terminated;
//        }else{
//          int count = writer.rowBuffer.size();
//          if(count==0){
//            writer.interrupt();
//          }
//        }
//      }
//      Thread.yield();
//    }
    for(RowWriter writer: writers){
      writer.interrupt();
    }
  }
}
