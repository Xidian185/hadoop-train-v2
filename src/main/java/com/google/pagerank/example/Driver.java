package com.google.pagerank.example;

import org.apache.hadoop.conf.Configuration;


/**
 * 此类为整个项目的入口，完成参数设置和程序启动
 */
public class Driver {
    public static void main(String[] args) throws Exception {
        //初始化windows环境下运行hadoop实例环境
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://192.168.43.26:8020");
        System.setProperty("HADOOP_USER_NAME","hadoop");
        System.setProperty("hadoop.home.dir", "E:\\BaiduNetdiskDownload\\新建文件夹\\hadoop-2.6.0-cdh5.15.1");
        //Job job = Job.getInstance(configuration);

        //PreDealer preDealer = new PreDealer(4, 0.85f, configuration);
        //preDealer.runPre();

        //Cycler cycler = new Cycler(4, configuration);
        //cycler.runCycler();

        Formatter formatter = new Formatter(configuration);
        formatter.runFormat();
    }
}
