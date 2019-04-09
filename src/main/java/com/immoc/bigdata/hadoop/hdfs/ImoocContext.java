package com.immoc.bigdata.hadoop.hdfs;

import com.amazonaws.services.devicefarm.model.transform.OfferingJsonUnmarshaller;

import java.util.HashMap;
import java.util.Map;

/**
 * 自定义上下文，其实就是缓存
 */
public class ImoocContext {
    private Map<Object,Object> cacheMap = new HashMap<Object,Object>();

    public Map<Object,Object> getCacheMap(){
        return cacheMap;
    }
    public void write(Object key, Object value){
        cacheMap.put(key, value);
    }
    public Object get(Object key){
        return cacheMap.get(key);
    }
}
