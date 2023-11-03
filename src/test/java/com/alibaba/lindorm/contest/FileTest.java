package com.alibaba.lindorm.contest;

import com.alibaba.lindorm.contest.test.TestUtils;

import java.io.File;

public class FileTest {
    public static void main(String[] args) throws Exception{
        File file = new File(TestUtils.TEST_DIR,"filetest/test.txt");
        System.out.println(file);
    }
}
