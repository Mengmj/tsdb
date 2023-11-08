//
// You should modify this file.
//
// Refer TSDBEngineSample.java to ensure that you have understood
// the interface semantics correctly.
//

package com.alibaba.lindorm.contest;

import com.alibaba.lindorm.contest.custom.*;
import com.alibaba.lindorm.contest.manager.IdManager;
import com.alibaba.lindorm.contest.manager.LatestRowManager;
import com.alibaba.lindorm.contest.manager.TSDBFileSystem;
import com.alibaba.lindorm.contest.structs.*;
import com.alibaba.lindorm.contest.task.AggTask;
import com.alibaba.lindorm.contest.task.DownsampleTask;
import com.alibaba.lindorm.contest.task.ReadTask;
import com.alibaba.lindorm.contest.task.WriteTask;
import com.alibaba.lindorm.contest.test.Counter;
import com.alibaba.lindorm.contest.test.TestUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TSDBEngineImpl extends TSDBEngine {
  String tableName;
  public InternalSchema schema;
  private TSDBFileSystem fileSystem;
  private IdManager idManager;
  private LatestRowManager latestRowManager;
  private ExecutorService WRPool;
  private final int THREAD_POOL_SIZE = 256;
  private volatile boolean shutdown;
  private File rootPath;

  private volatile AtomicInteger choseId;

  /**
   * This constructor's function signature should not be modified.
   * Our evaluation program will call this constructor.
   * The function's body can be modified.
   */
  public TSDBEngineImpl(File dataPath) {
    super(dataPath);
    fileSystem = TSDBFileSystem.getInstance(dataPath);
    tableName = fileSystem.tableName;
    WRPool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
    shutdown = false;
    rootPath = dataPath;
    //debug
    choseId = new AtomicInteger(-1);
  }

  /**
   * shema文件存在时加载
   * @throws IOException
   */
  @Override
  public void connect() throws IOException {
    if(shutdown){
      WRPool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
      fileSystem = TSDBFileSystem.getInstance(rootPath);
      shutdown = false;
    }
    if(tableName==null){
      System.out.println("connect to empty database");
    }else{
      System.out.printf("open table %s\n",tableName);
      schema = fileSystem.loadFrom(fileSystem.schemaFile);
      fileSystem.setSchema(schema);
      idManager = fileSystem.loadFrom(fileSystem.idFile);
      ArrayList<RowWritable> rowWritables = fileSystem.loadFrom(fileSystem.rowFile);
      latestRowManager = LatestRowManager.getInstance(rowWritables,schema);
    }

  }

  @Override
  public void createTable(String tableName, Schema schema) throws IOException {
    this.schema = InternalSchema.build(schema);
    fileSystem.setSchema(schema);
    fileSystem.dumpTo(fileSystem.schemaFile,this.schema);
    idManager = IdManager.getInstance();
    latestRowManager = LatestRowManager.getInstance();
    this.tableName = tableName;
    fileSystem.dumpTo(fileSystem.nameFile,this.tableName);

    //info
    System.out.print(String.format("MMJ_INFO: create table with %d ints, %d doubles and %d strings\n"
                                    ,this.schema.intCount,this.schema.doubleCount,this.schema.stringCount));
  }

  @Override
  public void shutdown() {
    fileSystem.dumpTo(fileSystem.idFile,idManager);
    ArrayList<RowWritable> rowWritables = latestRowManager.getRowWriteableList(schema);
    fileSystem.dumpTo(fileSystem.rowFile,rowWritables);
    fileSystem.shutdown();
    WRPool.shutdown();
    shutdown = true;
    choseId.set(-1);
  }

  /**
   * 将row存到内存的buffer中
   * @param wReq
   * @throws IOException
   */
  @Override
  public void write(WriteRequest wReq) throws IOException {
    if(!wReq.getTableName().equals(tableName)){
      return;
    }
    Map<FileKey,List<RowWrapped>> rowListMap = new HashMap<>();
    for(Row row:wReq.getRows()){
      int id = idManager.getId(row.getVin(),true);
      TestUtils.check(id!=-1);
      latestRowManager.upsert(row);
      FileKey fileKey = FileKey.buildFromTimestamp(id,row.getTimestamp());
      if(!rowListMap.containsKey(fileKey)){
        rowListMap.put(fileKey,new ArrayList<>());
      }
      rowListMap.get(fileKey).add(RowWrapped.wrap(id,row));
    }
    Future<?> result = WRPool.submit(new WriteTask(fileSystem,rowListMap));
    try{
      result.get();
    }catch (Exception e){
      throw new RuntimeException(e);
    }
  }

  @Override
  public ArrayList<Row> executeLatestQuery(LatestQueryRequest pReadReq) throws IOException {
    if(!pReadReq.getTableName().equals(tableName)){
      return new ArrayList<>();
    }
    ArrayList<Row> retList = new ArrayList<>();
    for(Vin vin : pReadReq.getVins()){
      Row row = latestRowManager.getLatestRow(vin);
      if(row!=null){
        retList.add(filterColumns(row,pReadReq.getRequestedColumns()));
      }
    }
    return retList;
  }

  /**
   * 将范围查询划分为几个文件的上的查询,每个查询作为一个独立的任务放入线程池执行
   * @param trReadReq
   * @return
   * @throws IOException
   */
  @Override
  public ArrayList<Row> executeTimeRangeQuery(TimeRangeQueryRequest trReadReq) throws IOException {
    if(!trReadReq.getTableName().equals(tableName)){
      return new ArrayList<>();
    }
    int id = idManager.getId(trReadReq.getVin(),false);
    if(id == -1){
      return new ArrayList<>();
    }
    ArrayList<Row> ret = new ArrayList<>();
    Set<String> colNames = trReadReq.getRequestedColumns();
    if(colNames==null || colNames.size()==0){
      colNames = schema.getColumnNames();
    }
    //第一条记录所在的分区
    long beginPartition = CommonUtils.getPartition(trReadReq.getTimeLowerBound());
    //最后一条记录所在的分区
    long endPartition = CommonUtils.getPartition(trReadReq.getTimeUpperBound()-1);

    List<Future<ArrayList<Row>>> futures = new ArrayList<>();
    for(long p = beginPartition;p <= endPartition;++p){
      long lowerTime = Math.max(trReadReq.getTimeLowerBound(),CommonUtils.getPartitionBegin(p));
      long upperTime = Math.min(trReadReq.getTimeUpperBound(),CommonUtils.getPartitionEnd(p));
      futures.add(WRPool.submit(new ReadTask(fileSystem,trReadReq.getVin(),id,lowerTime,upperTime,colNames)));
    }
    try {
      for (var f:futures){
        ret.addAll(f.get());
      }
    }catch (Exception e){
      throw new RuntimeException(e);
    }
    return ret;
  }

  @Override public ArrayList<Row> executeAggregateQuery(TimeRangeAggregationRequest aggregationReq) throws IOException {
    ArrayList<Row> ret = new ArrayList<>();
    if(!aggregationReq.getTableName().equals(tableName)){
      return ret;
    }
    int id = idManager.getId(aggregationReq.getVin(),false);
    if(id==-1){
      return ret;
    }
    String colName = aggregationReq.getColumnName();
    //需要聚合的第一个分区
    long beginPartition = CommonUtils.getPartition(aggregationReq.getTimeLowerBound());
    //需要聚合的最后一个分区
    long endPartition = CommonUtils.getPartition(aggregationReq.getTimeUpperBound()-1);
    List<Future<AggResult>> futures = new ArrayList<>();
    for(long p = beginPartition;p <= endPartition;++p){
      long lowerTime = Math.max(aggregationReq.getTimeLowerBound(),CommonUtils.getPartitionBegin(p));
      long upperTime = Math.min(aggregationReq.getTimeUpperBound(),CommonUtils.getPartitionEnd(p));
      futures.add(WRPool.submit(new AggTask(fileSystem,id,lowerTime,upperTime,colName,null)));
    }
    ColumnValue.ColumnType colType = schema.getType(colName);
    AggResult aggResult = new AggResult(schema.getType(colName));
    try {
      for(var f:futures){
        aggResult.merge(f.get());
      }
    }catch (Exception e){
      throw new RuntimeException(e);
    }
    if(aggResult.isEmpty()){
      //聚合范围内没有数据
      return ret;
    }
    ColumnValue value;
    switch (aggregationReq.getAggregator()){
      case AVG:
        value =  new ColumnValue.DoubleFloatColumn(aggResult.getAvg());
        break;
      case MAX:
        switch (colType){
          case COLUMN_TYPE_INTEGER:
            value = new ColumnValue.IntegerColumn(aggResult.getIntMax());
            break;
          case COLUMN_TYPE_DOUBLE_FLOAT:
            value = new ColumnValue.DoubleFloatColumn(aggResult.getDoubleMax());
            break;
          default:
            throw new RuntimeException("not supported type for max aggregation");
        }
        break;
      default:
        throw new RuntimeException("not supported aggregation operator");
    }
    Map<String,ColumnValue> columns = new HashMap<>();
    columns.put(colName,value);
    ret.add(new Row(aggregationReq.getVin(),aggregationReq.getTimeLowerBound(),columns));
    return ret;
  }

  @Override public ArrayList<Row> executeDownsampleQuery(TimeRangeDownsampleRequest downsampleReq) throws IOException {
    ArrayList<Row> ret = new ArrayList<>();
    Vin vin = downsampleReq.getVin();
    int id = idManager.getId(vin,false);
    if(!downsampleReq.getTableName().equals(tableName) || id==-1){
      return ret;
    }
    List<Future<Map<Long,AggResult>>> singleFileAggs = new ArrayList<>();
    Map<Long,Future<AggResult>> twoFileAggs = new HashMap<>();
    long lower = downsampleReq.getTimeLowerBound();
    long interval = downsampleReq.getInterval();
    String colName = downsampleReq.getColumnName();
    CompareExpression expression = downsampleReq.getColumnFilter();
    while(lower < downsampleReq.getTimeUpperBound()){
      long partition = CommonUtils.getPartition(lower);
      long partitionEnd = CommonUtils.getPartitionEnd(partition);
      if(lower+interval > partitionEnd){
        //当前interval会跨分区文件
        twoFileAggs.put(lower,WRPool.submit(new AggTask(fileSystem,id,lower,lower+interval,colName,expression)));
        lower += interval;
      }else{
        //当前时间分区还有几个interval
        long remainInterval = (Math.min(partitionEnd,downsampleReq.getTimeUpperBound()) - lower)/interval;
        if(remainInterval==0){
          break;
        }
        long nextLower = lower + remainInterval * interval;
        //提交单文件降采样任务
        singleFileAggs.add(WRPool.submit(new DownsampleTask(fileSystem,id,lower,nextLower,interval,colName,expression)));
        lower = nextLower;
      }
    }
    Aggregator aggOp = downsampleReq.getAggregator();
    try{
      for(var f: singleFileAggs){
        Map<Long,AggResult> aggResults = f.get();
        for(var entry: aggResults.entrySet()){
          long timestamp = entry.getKey();
          AggResult result = entry.getValue();
          if(result==null || result.isEmpty()){
            continue;
          }
          ret.add(buildRowFromAggResult(vin,timestamp,colName,result,aggOp));
        }
      }
      for(var entry: twoFileAggs.entrySet()){
        long timestamp = entry.getKey();
        AggResult result = entry.getValue().get();
        if(result==null || result.isEmpty()){
          continue;
        }
        ret.add(buildRowFromAggResult(vin,timestamp,colName,result,aggOp));
      }
    }catch (Exception e){
      throw new RuntimeException(e);
    }
    Collections.sort(ret,TSDBEngineImpl::compareRows);
    return ret;
  }

  private static Row filterColumns(Row row,Set<String> requested){
    if(requested==null||requested.isEmpty()){
      return row;
    }
    Map<String,ColumnValue> columes = row.getColumns();
    Map<String,ColumnValue> requestedColumns = new HashMap<>();
    for(String colName: requested){
      requestedColumns.put(colName,columes.get(colName));
    }
    return new Row(row.getVin(),row.getTimestamp(),requestedColumns);
  }

  private Row buildRowFromAggResult(Vin vin, long timestamp, String colName, AggResult aggResult, Aggregator aggregator){
    if(aggResult.isEmpty()){
      return null;
    }
    Map<String,ColumnValue> columns = new HashMap<>();
    ColumnValue value;
    ColumnValue.ColumnType colType = schema.getType(colName);
    switch (aggregator){
      case AVG:
        value = new ColumnValue.DoubleFloatColumn(aggResult.getAvg());
        break;
      case MAX:
        switch (colType){
          case COLUMN_TYPE_INTEGER:
            value = new ColumnValue.IntegerColumn(aggResult.getIntMax());
            break;
          case COLUMN_TYPE_DOUBLE_FLOAT:
            value = new ColumnValue.DoubleFloatColumn(aggResult.getDoubleMax());
            break;
          default:
            throw new RuntimeException("not supported type for max aggregation");
        }
        break;
      default:
        throw new RuntimeException("not supported aggregation operator");
    }
    columns.put(colName,value);
    return new Row(vin,timestamp,columns);
  }

  public static int compareRows(Row r1,Row r2){
    return (int)(r1.getTimestamp()-r2.getTimestamp());
  }

}
