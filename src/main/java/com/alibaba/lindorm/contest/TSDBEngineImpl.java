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
import com.alibaba.lindorm.contest.task.ReadTask;
import com.alibaba.lindorm.contest.task.WriteTask;
import com.alibaba.lindorm.contest.test.TestUtils;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class TSDBEngineImpl extends TSDBEngine {
  String tableName;
  public InternalSchema schema;
  private TSDBFileSystem fileSystem;
  private IdManager idManager;
  private LatestRowManager latestRowManager;
  private ExecutorService WRPool;
  private final int THREAD_POOL_SIZE = 8;
  private volatile boolean shutdown;
  private File rootPath;

  
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

  @Override
  public ArrayList<Row> executeTimeRangeQuery(TimeRangeQueryRequest trReadReq) throws IOException {
    if(!trReadReq.getTableName().equals(tableName)){
      return new ArrayList<>();
    }
    int id = idManager.getId(trReadReq.getVin(),false);
    if(id == -1){
      return new ArrayList<Row>();
    }
    ArrayList<Row> ret;
    Set<String> colNames = trReadReq.getRequestedColumns();
    if(colNames==null || colNames.size()==0){
      colNames = schema.getColumnNames();
    }
    Future<ArrayList<Row>> future = WRPool.submit(new ReadTask(fileSystem,trReadReq.getVin(),id, trReadReq.getTimeLowerBound(), trReadReq.getTimeUpperBound(), colNames));
    try {
      ret = future.get();
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
    Future<AggResult> future = WRPool.submit(new AggTask(fileSystem,id,aggregationReq.getTimeLowerBound(),
            aggregationReq.getTimeUpperBound(),aggregationReq.getColumnName(),null));
    try {
      AggResult aggResult = future.get();
      String colName = aggregationReq.getColumnName();
      ColumnValue.ColumnType colType = schema.getType(colName);
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
    }catch (Exception e){
      throw new RuntimeException(e);
    }
    return ret;
  }

  @Override public ArrayList<Row> executeDownsampleQuery(TimeRangeDownsampleRequest downsampleReq) throws IOException {
    ArrayList<Row> ret = new ArrayList<>();
    Vin vin = downsampleReq.getVin();
    int id = idManager.getId(vin,false);
    if(!downsampleReq.getTableName().equals(tableName) || id==-1){
      return ret;
    }
    List<Future<AggResult>> futures = new ArrayList<>();
    Queue<Long> timestamps = new LinkedList<>();
    for(long p = downsampleReq.getTimeLowerBound();p < downsampleReq.getTimeUpperBound();p+=downsampleReq.getInterval()){
      futures.add(WRPool.submit(new AggTask(fileSystem,id,p,p+downsampleReq.getInterval(),
              downsampleReq.getColumnName(), downsampleReq.getColumnFilter())));
      timestamps.offer(p);
    }
    try {
      for(Future<AggResult> future:futures){
        AggResult aggResult = future.get();
        if(aggResult.isEmpty()){
          continue;
        }
        Map<String,ColumnValue> columns = new HashMap<>();
        ColumnValue value;
        switch (downsampleReq.getAggregator()){
          case AVG:
            value = new ColumnValue.DoubleFloatColumn(aggResult.getAvg());
            break;
          case MAX:
            switch (schema.getType(downsampleReq.getColumnName())){
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
        columns.put(downsampleReq.getColumnName(),value);
        ret.add(new Row(vin,timestamps.poll(),columns));
      }
    }catch (Exception e){
      throw new RuntimeException(e);
    }
    return ret;
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

}
