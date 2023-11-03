package com.alibaba.lindorm.contest;

import com.alibaba.lindorm.contest.custom.FileKey;
import com.alibaba.lindorm.contest.custom.MappedFile;
import com.alibaba.lindorm.contest.manager.TSDBFileSystem;
import com.alibaba.lindorm.contest.structs.Schema;
import com.alibaba.lindorm.contest.test.TestUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TSDBFileSystemTest {
    private static class WriteTask implements Runnable{
        private final TSDBFileSystem fileSystem;
        public WriteTask(TSDBFileSystem fileSystem){
            this.fileSystem = fileSystem;
        }
        @Override
        public void run() {
            long beginPartition = CommonUtils.getPartition(1698905700000L);
            for(int i = 0;i < 100;++i){
                for(int j = 0;j < 100;++j){
                    MappedFile mappedFile = fileSystem.getMappedFile(FileKey.buildFromPB(i,j+beginPartition),true);
                    mappedFile.writeByte(0,(byte) i);
                    mappedFile.writeByte(1,(byte) j);
                    fileSystem.deRefFile(mappedFile);
                    mappedFile = null;
                }
            }
        }
    }

    private static class ReadTask implements Runnable{
        private final TSDBFileSystem fileSystem;
        public ReadTask(TSDBFileSystem fileSystem){
            this.fileSystem = fileSystem;
        }
        @Override
        public void run() {
            long beginPartition = CommonUtils.getPartition(1698905700000L);
            for(int i = 0;i < 100;++i){
                for(int j = 0;j < 100;++j){
                    MappedFile mappedFile = fileSystem.getMappedFile(FileKey.buildFromPB(i,j+beginPartition),false);
                    int a = mappedFile.readByte(0);
                    int b = mappedFile.readByte(1);
                    TestUtils.check(a==i && b==j);
                    fileSystem.deRefFile(mappedFile);
                    LogUtils.debug("i:%d,j:%d",i,j);
                    mappedFile = null;
                }
            }
        }
    }

    public static void main(String[] args) {
        writeTest();
        readTest();
    }
    public static void writeTest(){
        File rootPath = new File(TestUtils.TEST_DIR,"filesystem_test");
        if(!rootPath.exists()){
            rootPath.mkdirs();
        }else{
            System.out.println("清理文件");
            TestUtils.cleanDir(rootPath);
            System.out.println("清理文件完毕");
        }
        TSDBFileSystem fileSystem = TSDBFileSystem.getInstance(rootPath);
        Schema schema = TestUtils.TEST_SCHEMA;
        fileSystem.setSchema(schema);
        ExecutorService pool = Executors.newFixedThreadPool(8);
        List<Future<?>> futures = new ArrayList<>();
        int n = 1;
        long t1 = System.currentTimeMillis();
        for(int i = 0;i < n;++i){
            futures.add(pool.submit(new WriteTask(fileSystem)));
        }
        try {
            for (int i = 0;i < n;++i){
                futures.get(i).get();
            }
        }catch (Exception e){
            throw new RuntimeException(e);
        }
        fileSystem.shutdown();
        long t2 = System.currentTimeMillis();
        System.out.printf("写入测试完成,用时%dms\n",t2-t1);
        pool.shutdown();
    }

    public static void readTest(){
        File rootPath = new File(TestUtils.TEST_DIR,"filesystem_test");
        TSDBFileSystem fileSystem = TSDBFileSystem.getInstance(rootPath);
        Schema schema = TestUtils.TEST_SCHEMA;
        fileSystem.setSchema(schema);
        ExecutorService pool = Executors.newFixedThreadPool(8);
        List<Future<?>> futures = new ArrayList<>();
        int n = 8;
        long t1 = System.currentTimeMillis();
        for(int i = 0;i < n;++i){
            futures.add(pool.submit(new ReadTask(fileSystem)));
        }
        try {
            for (int i = 0;i < n;++i){
                futures.get(i).get();
            }
        }catch (Exception e){
            throw new RuntimeException(e);
        }
        fileSystem.shutdown();
        long t2 = System.currentTimeMillis();
        System.out.printf("读取测试完成,用时%dms\n",t2-t1);
        pool.shutdown();
    }

}
