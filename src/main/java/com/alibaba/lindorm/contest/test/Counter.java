package com.alibaba.lindorm.contest.test;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Counter {
    public static volatile AtomicLong queryCount = new AtomicLong(0);
    public static volatile AtomicLong totalColumns = new AtomicLong(0);
//    public static volatile AtomicLong[] charCount = new AtomicLong[256];
//    static {
//        for(int i = 0;i < 256;++i){
//            charCount[i] = new AtomicLong(0);
//        }
//    }
    public static volatile AtomicInteger outputCount = new AtomicInteger(0);
    public static volatile AtomicBoolean outputFlag = new AtomicBoolean(true);
    public static volatile AtomicInteger choseId = new AtomicInteger(-1);
}
