//
// You should modify this file.
//
// Refer TSDBEngineSample.java to ensure that you have understood
// the interface semantics correctly.
//

package com.alibaba.lindorm.contest;

import com.alibaba.lindorm.contest.custom.FileKey;
import com.alibaba.lindorm.contest.custom.InternalSchema;
import com.alibaba.lindorm.contest.custom.RowWritable;
import com.alibaba.lindorm.contest.manager.IdManager;
import com.alibaba.lindorm.contest.manager.LatestRowManager;
import com.alibaba.lindorm.contest.manager.TSDBFileSystem;
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
  public InternalSchema schema;
  private TSDBFileSystem fileSystem;
  private IdManager idManager;
  private LatestRowManager latestRowManager;

  
  /**
   * This constructor's function signature should not be modified.
   * Our evaluation program will call this constructor.
   * The function's body can be modified.
   */
  public TSDBEngineImpl(File dataPath) {
    super(dataPath);
    fileSystem = TSDBFileSystem.getInstance(dataPath);
    tableName = fileSystem.tableName;
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
    Map<FileKey,Row>
    for(Row row:wReq.getRows()){
      int id = idManager.getId(row.getVin(),true);
      latestRowManager.upsert(row);
      FileKey fileKey = FileKey.build(id,row.getTimestamp());

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
