package com.alibaba.lindorm.contest;

import com.alibaba.lindorm.contest.structs.Vin;
import com.alibaba.lindorm.contest.test.TestUtils;

import javax.print.attribute.standard.RequestingUserName;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class RandomTest {
    public static void main(String[] args) {
        Random random = new Random();
        byte[] a = new byte[17];
        byte[] b = new byte[17];
        List<Vin> vins = TestUtils.randomVins(100);
        for(Vin vin:vins){

            System.out.println(vin.getVin());
        }
    }
}
