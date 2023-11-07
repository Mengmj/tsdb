package com.alibaba.lindorm.contest;

import java.util.HashSet;
import java.util.Set;

public class SetTest {
    public static void main(String[] args) {
        Set<Long> intervals = new HashSet<>();
        intervals.add(100L);
        intervals.add(200L);
        System.out.println(intervals);
    }
}
