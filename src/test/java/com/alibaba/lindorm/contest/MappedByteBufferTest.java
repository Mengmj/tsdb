package com.alibaba.lindorm.contest;

import com.alibaba.lindorm.contest.test.TestUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class MappedByteBufferTest {
    public static void main(String[] args) throws IOException {
        File file = new File(TestUtils.TEST_DIR,"mappedfile");
        RandomAccessFile raf = new RandomAccessFile(file,"rw");
        FileChannel fc = raf.getChannel();
        long size = 32*1024*1024;
        long t1 = System.currentTimeMillis();
        MappedByteBuffer mappedByteBuffer = fc.map(FileChannel.MapMode.READ_WRITE,0,size);
        for(int i = 0;i < size;++i){
            mappedByteBuffer.put(i,(byte) i);
        }
        mappedByteBuffer = null;
        fc.truncate(4*1024*1024);
        fc.close();
        raf.close();
        long t2 = System.currentTimeMillis();
        System.out.printf("%dms",t2-t1);
    }
}
