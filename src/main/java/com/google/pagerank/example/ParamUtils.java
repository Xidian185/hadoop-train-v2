package com.google.pagerank.example;

import java.io.IOException;
import java.util.Properties;

public class ParamUtils {
    private static Properties properties = new Properties();
    static {
        try {
            properties.load(ParamUtils.class.getClassLoader().getResourceAsStream("pagerank.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static Properties getProperties(){
        return properties;
    }
}
