package com.immoc.bigdata.hadoop.hdfs;

import java.io.IOException;
import java.util.Properties;

/**
 * 读取属性配置文件
 */
public class ParamUtils {
    private static Properties properties = new Properties();
    static{
        try {
            properties.load(ParamUtils.class.getClassLoader().getResourceAsStream("wc.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static Properties getProperties(){
        return properties;
    }

    public static void main(String[] args) {
        System.out.println(ParamUtils.getProperties().getProperty(Constants.HDFS_URI));
    }
}
