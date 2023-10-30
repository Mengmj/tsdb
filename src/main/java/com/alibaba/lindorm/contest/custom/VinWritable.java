package com.alibaba.lindorm.contest.custom;

import com.alibaba.lindorm.contest.structs.Vin;

import java.io.Serializable;
import java.util.Arrays;

public class VinWritable implements Serializable {
    private final byte[] vin;
    private transient Vin vinStruct;
    public VinWritable(Vin obj){
        this.vin = obj.getVin();
        vinStruct = new Vin(this.vin);
    }
    public VinWritable(byte[] vin){
        this.vin = vin;
        vinStruct = new Vin(this.vin);
    }
    public Vin getVinStruct(){
        if(vinStruct == null){
            vinStruct = new Vin(vin);
        }
        return vinStruct;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(vin);
    }

    @Override
    public boolean equals(Object obj) {
        if(obj==null || obj.getClass()!=this.getClass()){
            return false;
        }
        return Arrays.equals(vin,((VinWritable) obj).getVinStruct().getVin());
    }
}
