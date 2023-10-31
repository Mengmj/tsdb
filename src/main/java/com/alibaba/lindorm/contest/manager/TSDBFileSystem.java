package com.alibaba.lindorm.contest.manager;

import com.alibaba.lindorm.contest.CommonUtils;
import com.alibaba.lindorm.contest.custom.FileKey;
import com.alibaba.lindorm.contest.custom.InternalSchema;
import com.alibaba.lindorm.contest.custom.MappedFile;
import com.alibaba.lindorm.contest.structs.Schema;
import com.alibaba.lindorm.contest.test.TestUtils;

import java.io.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class TSDBFileSystem {
    private final int BUFFER_POOL_SIZE = 300;
    private MappedFile[] bufferPool;
    private AtomicInteger[] refCount;
    private int next;
    private AtomicInteger zeroRef;
    private int bufferUsed;
    private ConcurrentMap<String,Integer> bufferIndex;
    private InternalSchema schema;

    public final File rootPath;
    public final File dataPath;
    public final File metaPath;
    public final File schemaFile;
    public final File idFile;
    public final File rowFile;
    public final File nameFile;
    public String tableName;


    private TSDBFileSystem(File rootPath){
        this.rootPath = rootPath;
        dataPath = new File(rootPath,"data");
        if(!dataPath.exists()){
            dataPath.mkdirs();
        }
        metaPath = new File(rootPath,"meta");
        if(!metaPath.exists()){
            metaPath.mkdirs();
        }
        schemaFile = new File(metaPath,"schema.ser");
        idFile = new File(metaPath,"idManager.ser");
        rowFile = new File(metaPath,"rows.ser");
        nameFile = new File(metaPath,"name.ser");
        if(nameFile.exists()){
            tableName = loadFrom(nameFile);
        }

        bufferPool = new MappedFile[BUFFER_POOL_SIZE];
        refCount = new AtomicInteger[BUFFER_POOL_SIZE];
        bufferIndex = new ConcurrentHashMap<>();
        for(int i = 0;i < BUFFER_POOL_SIZE;++i){
            refCount[i] = new AtomicInteger(-1);
        }
        next = 0;
        zeroRef = new AtomicInteger(0);
        bufferUsed = 0;
    }
    public static TSDBFileSystem getInstance(File rootPath){
        return new TSDBFileSystem(rootPath);
    }
    public void setSchema(Schema schema){
        this.schema = InternalSchema.build(schema);
    }
    public void setSchema(InternalSchema schema){
        this.schema = schema;
    }
    public MappedFile getMappedFile(FileKey fileKey, boolean allocate){
        File file = getFile(fileKey,allocate);
        if(!file.exists()){
            return null;
        }
        MappedFile ret;
        synchronized (bufferPool){
            if(bufferIndex.containsKey(file.getName())){
                int idx = bufferIndex.get(file.getName());
                if(refCount[idx].get()==0){
                    zeroRef.decrementAndGet();
                }
                refCount[idx].incrementAndGet();
                ret = bufferPool[idx];
            }else{
                while(bufferUsed==BUFFER_POOL_SIZE && zeroRef.get() == 0){
                    try{
                        bufferPool.wait();
                    }catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                if(bufferUsed<BUFFER_POOL_SIZE){
                    while (refCount[next].get()>=0){
                        next = (next+1)%BUFFER_POOL_SIZE;
                    }
                }else {
                    while (refCount[next].get()>0){
                        next = (next+1)%BUFFER_POOL_SIZE;
                    }
                }
                if(refCount[next].get()==-1){
                    bufferUsed++;
                }else {
                    zeroRef.decrementAndGet();
                    String removed = bufferPool[next].file.getName();
                    bufferIndex.remove(removed);
                    try {
                        bufferPool[next].close();
                    }catch (IOException e){
                        throw new RuntimeException(e);
                    }
                }
                refCount[next].set(1);
                bufferIndex.put(file.getName(), next);
                bufferPool[next] = MappedFile.getInstance(file,fileKey, schema);
                ret = bufferPool[next];
                next = (next+1)%BUFFER_POOL_SIZE;
            }
        }
        return ret;
    }

    public void deRefFile(MappedFile mappedFile){
        TestUtils.check(bufferIndex.containsKey(mappedFile.file.getName()));
        int idx = bufferIndex.get(mappedFile.file.getName());
        TestUtils.check(refCount[idx].get()>0);
        int count = refCount[idx].decrementAndGet();
        if(count==0){
            zeroRef.incrementAndGet();
        }
    }
    public File getFile(FileKey fileKey,boolean allocate) {
        File partitionDir = new File(dataPath, String.valueOf(fileKey.partition));
        File file = new File(partitionDir, String.valueOf(fileKey.buckle));
        if (allocate) {
            if (!partitionDir.exists()) {
                partitionDir.mkdirs();
            }
            if (!file.exists()) {
                try {
                    file.createNewFile();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return file;
    }
    /**调用前要保证bufferPool已释放
     * shutdown不考虑未完成的get或deRef
     */
    public void shutdown(){
        TestUtils.check(bufferUsed== zeroRef.get());
        for(int i = 0;i < BUFFER_POOL_SIZE;++i){
            if(refCount[i].get()>=0){
                try {
                    bufferPool[i].close();
                }catch (IOException e){
                    throw new RuntimeException(e);
                }
            }
        }
    }
    public  <T> T loadFrom(File file){
        T ret;
        try(FileInputStream fis = new FileInputStream(file);
            ObjectInputStream ois = new ObjectInputStream(fis);){
            ret = (T) ois.readObject();
        }catch (Exception e){
            throw new RuntimeException(e);
        }
        return ret;
    }
    public void dumpTo(File file, Object object){
        try(FileOutputStream fos = new FileOutputStream(file);
            ObjectOutputStream oos = new ObjectOutputStream(fos);){
            oos.writeObject(object);
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }
}
