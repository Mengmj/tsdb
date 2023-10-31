//
// You should modify this file.
//
// Refer TSDBEngineSample.java to ensure that you have understood
// the interface semantics correctly.
//

package com.alibaba.lindorm.contest;

import com.alibaba.lindorm.contest.custom.FileKey;
import com.alibaba.lindorm.contest.custom.InternalSchema;
import com.alibaba.lindorm.contest.custom.RowWrapped;
import com.alibaba.lindorm.contest.custom.RowWritable;
import com.alibaba.lindorm.contest.manager.IdManager;
import com.alibaba.lindorm.contest.manager.LatestRowManager;
import com.alibaba.lindorm.contest.manager.TSDBFileSystem;
import com.alibaba.lindorm.contest.structs.*;
import com.alibaba.lindorm.contest.task.ReadTask;
import com.alibaba.lindorm.contest.task.WriteTask;

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

  
  /**
   * This constructor's function signature should not be modified.
   * Our evaluation program will call this constructor.
   * The function's body can be modified.
   */
  public TSDBEngineImpl(File dataPath) {
    super(dataPath);
    fileSystem = TSDBFileSystem.getInstance(dataPath);
    tableName = fileSystem.tableName;
    WRPool = Executors.newFixedThreadPool(8);
  }

  /**
   * shema文件存在时加载
   * @throws IOException
   */
  @Override
  public void connect() throws IOException {
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
  }

  /**
   * 将row存到内存的buffer中
   * @param wReq
   * @throws IOException
   */
  @Override
  public void write(WriteRequest wReq) throws IOException {
    if(wReq.getTableName()!=tableName){
      return;
    }
    Map<FileKey,List<RowWrapped>> rowListMap = new HashMap<>();
    for(Row row:wReq.getRows()){
      int id = idManager.getId(row.getVin(),true);
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
    ArrayList<Row> ret;
    int id = idManager.getId(trReadReq.getVin(),false);
    if(id == -1){
      return null;
    }
    Future<ArrayList<Row>> future = WRPool.submit(new ReadTask(fileSystem,trReadReq.getVin(),id, trReadReq.getTimeLowerBound(), trReadReq.getTimeUpperBound(), trReadReq.getRequestedColumns()));
    try {
      ret = future.get();
    }catch (Exception e){
      throw new RuntimeException(e);
    }
    return ret;
  }

  @Override public ArrayList<Row> executeAggregateQuery(TimeRangeAggregationRequest aggregationReq) throws IOException {
    return null;
  }

  @Override public ArrayList<Row> executeDownsampleQuery(TimeRangeDownsampleRequest downsampleReq) throws IOException {
    return null;
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
