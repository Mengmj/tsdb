package com.alibaba.lindorm.contest;

import java.util.ArrayList;

public class ArrayListTest {
    public static void main(String[] args){
        int size = 100_000;
        ArrayList<Integer> list2 = new ArrayList<>();
        ArrayList<Integer> list1 = new ArrayList<>(size);
        long t1 = System.currentTimeMillis();
        for(int i = 0;i < size;++i){
            list1.add(i);
        }
        long t2 = System.currentTimeMillis();
//        for(int i = 0;i < size;++i){
//            list2.add(i);
//        }
        long t3 = System.currentTimeMillis();
        System.out.printf("%dms,%dms",t2-t1,t3-t2);
    }
}
