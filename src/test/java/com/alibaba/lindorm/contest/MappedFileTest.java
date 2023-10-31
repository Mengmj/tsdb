//package com.alibaba.lindorm.contest;
//
//import com.alibaba.lindorm.contest.custom.InternalSchema;
//import com.alibaba.lindorm.contest.custom.MappedFile;
//import com.alibaba.lindorm.contest.structs.Row;
//import com.alibaba.lindorm.contest.structs.Vin;
//import com.alibaba.lindorm.contest.test.TestUtils;
//
//import java.io.File;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;
//import java.util.concurrent.ConcurrentMap;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.Future;
//
//public class MappedFileTest {
//    static class Task implements Runnable{
//        private MappedFile mappedFile;
//        private ArrayList<Integer> ids;
//        private ArrayList<Row> rows;
//        public Task(MappedFile mappedFile,ArrayList<Integer> ids,ArrayList<Row> rows){
//            this.mappedFile = mappedFile;
//            this.ids = ids;
//            this.rows = rows;
//        }
//        @Override
//        public void run() {
//            mappedFile.writeRows(ids,rows);
//        }
//    }
//    public static void main(String[] args) throws Exception{
//        multiThreadTest();
//    }
//    private static void writeAndRead() throws IOException {
//        File file = new File(TestUtils.TEST_DIR,"mappedFileWriteAndRead");
//        long timestamp = 1698627840000L;
//        long partition = CommonUtils.getPartition(timestamp);
//        List<Vin> vins = TestUtils.randomVins(CommonUtils.BUCKLE_SIZE);
//        List<ArrayList<Row>> rowsList = new ArrayList<>();
//        for(int i = 0;i < CommonUtils.BUCKLE_SIZE;++i){
//            rowsList.add(TestUtils.randomRows(vins.get(i),timestamp,TestUtils.TEST_SCHEMA,CommonUtils.PARTITION_SECONDS));
//        }
//        InternalSchema schema = InternalSchema.build(TestUtils.TEST_SCHEMA);
//        MappedFile mappedFile = MappedFile.getInstance(file,partition,0,schema);
//        long t1 = System.currentTimeMillis();
//        for(int i = 0;i < CommonUtils.BUCKLE_SIZE/2;++i){
//            ArrayList<Integer> ids = new ArrayList<>();
//            for(int j = 0;j < CommonUtils.PARTITION_SECONDS;++j){
//                ids.add(i);
//            }
//            mappedFile.writeRows(ids,rowsList.get(i));
//        }
//        mappedFile.close();
//        mappedFile = MappedFile.getInstance(file,partition,0,schema);
//        for(int i = CommonUtils.BUCKLE_SIZE/2;i < CommonUtils.BUCKLE_SIZE;++i){
//            ArrayList<Integer> ids = new ArrayList<>();
//            for(int j = 0;j < CommonUtils.PARTITION_SECONDS;++j){
//                ids.add(i);
//            }
//            mappedFile.writeRows(ids,rowsList.get(i));
//        }
//        mappedFile.close();
//        long t2 = System.currentTimeMillis();
//        mappedFile = MappedFile.getInstance(file,partition,0,schema);
//        for(int i = 0;i < CommonUtils.BUCKLE_SIZE;++i){
//            ArrayList<Row> rows = mappedFile.readRows(vins.get(i),i,schema.getColumnNames(),timestamp,timestamp+CommonUtils.PARTITION_SECONDS*1000);
//            //assert rows.size() == rowsList.get(i).size();
//            for(int j = 0;j < rows.size();++j){
//                //System.out.println(rows.get(j));
//                //System.out.println(rowsList.get(i).get(j));
//                //TestUtils.check(TestUtils.compareRow(rows.get(j),rowsList.get(i).get(j)));
//            }
//        }
//        mappedFile.close();
//        long t3 = System.currentTimeMillis();
//        System.out.printf("write %dms, read %dms\n",t2-t1,t3-t2);
//
//    }
//    public static void multiThreadTest() throws Exception{
//        File file = new File(TestUtils.TEST_DIR,"mappedFileWriteAndRead");
//        long timestamp = 1698627840000L;
//        long partition = CommonUtils.getPartition(timestamp);
//        List<Vin> vins = TestUtils.randomVins(CommonUtils.BUCKLE_SIZE);
//        List<ArrayList<Row>> rowsList = new ArrayList<>();
//        for(int i = 0;i < CommonUtils.BUCKLE_SIZE;++i){
//            rowsList.add(TestUtils.randomRows(vins.get(i),timestamp,TestUtils.TEST_SCHEMA,CommonUtils.PARTITION_SECONDS));
//        }
//        InternalSchema schema = InternalSchema.build(TestUtils.TEST_SCHEMA);
//        MappedFile mappedFile = MappedFile.getInstance(file,partition,0,schema);
//        ExecutorService pool = Executors.newFixedThreadPool(16);
//        List<Future<?>> futures = new ArrayList<>();
//        long t1 = System.currentTimeMillis();
//        for(int i = 0;i < CommonUtils.BUCKLE_SIZE;++i){
//            ArrayList<Integer> ids = new ArrayList<>();
//            for(int j = 0;j < CommonUtils.PARTITION_SECONDS;++j){
//                ids.add(i);
//            }
//            futures.add(pool.submit(new Task(mappedFile,ids,rowsList.get(i))));
//        }
//        for(var f: futures){
//            f.get();
//        }
//        mappedFile.close();
//        long t2 = System.currentTimeMillis();
//        mappedFile = MappedFile.getInstance(file,partition,0,schema);
//        for(int i = 0;i < CommonUtils.BUCKLE_SIZE;++i){
//            ArrayList<Row> rows = mappedFile.readRows(vins.get(i),i,schema.getColumnNames(),timestamp,timestamp+CommonUtils.PARTITION_SECONDS*1000);
//            //assert rows.size() == rowsList.get(i).size();
//            for(int j = 0;j < rows.size();++j){
//                //System.out.println(rows.get(j));
//                //System.out.println(rowsList.get(i).get(j));
//                TestUtils.check(TestUtils.compareRow(rows.get(j),rowsList.get(i).get(j)));
//            }
//        }
//        mappedFile.close();
//        long t3 = System.currentTimeMillis();
//        pool.shutdown();
//        System.out.printf("write %dms, read %dms\n",t2-t1,t3-t2);
//    }
//}
