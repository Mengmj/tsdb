package com.alibaba.lindorm.contest;

public class LogUtils {
    public static void debug(String format, Object... args){
        System.out.printf("%s DEBUG::%s\n",Thread.currentThread().getName(),String.format(format,args));
    }
    public static void error(String format, Object... args){
        System.out.printf("%s ERROR::%s\n",Thread.currentThread().getName(),String.format(format,args));
    }
}
