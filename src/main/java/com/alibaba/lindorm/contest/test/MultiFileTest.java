package com.alibaba.lindorm.contest.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class MultiFileTest {

    public static void main(String[] args) throws IOException {
        multiMap();
    }
    public static void writeTest() throws IOException{
        File testFile = new File(TestUtils.TEST_DIR,"mmap");
        FileChannel channel = new RandomAccessFile(testFile,"rw").getChannel();

        MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE,0,4096);
        mappedByteBuffer.put(new byte[]{1,2,3,4,5});
        channel.close();
    }
    public static void readTest() throws IOException{
        File testFile = new File(TestUtils.TEST_DIR,"mmap");
        FileChannel channel = new RandomAccessFile(testFile,"rw").getChannel();

        MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE,0,4096);
        byte[] array = new byte[5];
        mappedByteBuffer.get(array);
        channel.close();
        System.out.println(array);
        for (byte b: array){
            System.out.println(b);
        }
    }

    public static void multiMap() throws IOException{
        int num = 10000*60*2;
        int PAGE_SIZE = 3600*24*4;
        MappedByteBuffer[] mbbs = new MappedByteBuffer[num];
        for(int i = 0;i < num;++i){
            RandomAccessFile randomAccessFile = new RandomAccessFile(new File(TestUtils.TEST_DIR,"mmap"+i),"rw");
            mbbs[i] = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE,0,PAGE_SIZE);
        }
    }
}
