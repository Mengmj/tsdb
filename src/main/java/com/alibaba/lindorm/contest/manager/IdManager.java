package com.alibaba.lindorm.contest.manager;

import com.alibaba.lindorm.contest.custom.VinWritable;
import com.alibaba.lindorm.contest.structs.Vin;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class IdManager implements Serializable {
    private final ConcurrentMap<VinWritable,Integer> vinMap;
    private final AtomicInteger counter;
    private final AtomicBoolean mapLock;

    private IdManager(){
        vinMap = new ConcurrentHashMap<>();
        counter = new AtomicInteger(0);
        mapLock = new AtomicBoolean(false);
    }
    public static IdManager getInstance(){
        return new IdManager();
    }

//    public int getId(Vin vin, boolean allocate){
//        VinWritable vinWritable = new VinWritable(vin);
//        int id = -1;
//        if(!vinMap.containsKey(vinWritable)){
//            if(allocate){
//                while(mapLock.getAndSet(true)){};
//                if(!vinMap.containsKey(vinWritable)){
//                    id = counter.get();
//                    vinMap.put(vinWritable,id);
//                    counter.incrementAndGet();
//                }
//                mapLock.set(false);
//            }
//        }else{
//            id = vinMap.get(vinWritable);
//        }
//        return id;
//    }
    public int getId(Vin vin, boolean allocate){
        VinWritable vinWritable = new VinWritable(vin);
        int id = -1;
        if(!vinMap.containsKey(vinWritable)){
            if (allocate){
                synchronized (vinMap){
                    if(!vinMap.containsKey(vinWritable)){
                        id = counter.get();
                        vinMap.put(vinWritable,id);
                        counter.incrementAndGet();
                    }{
                        id = vinMap.get(vinWritable);
                    }
                }
            }
        }else{
            id = vinMap.get(vinWritable);
        }
        return id;
    }
}
