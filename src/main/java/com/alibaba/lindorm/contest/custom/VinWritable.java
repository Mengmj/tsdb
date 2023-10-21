package com.alibaba.lindorm.contest.custom;

import com.alibaba.lindorm.contest.structs.Vin;

import java.io.Serializable;

public class VinWritable implements Serializable {
    private final byte[] vin;
    private final transient Vin vinStruct;
    public VinWritable(Vin obj){
        this.vin = obj.getVin();
        vinStruct = new Vin(this.vin);
    }
    public VinWritable(byte[] vin){
        this.vin = vin;
        vinStruct = new Vin(this.vin);
    }
    public Vin getVinStruct(){
        return vinStruct;
    }
}
