package com.immoc.bigdata.hadoop.mr.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

public class WordCountApp {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME","hadoop");
        System.setProperty("hadoop.home.dir", "E:\\BaiduNetdiskDownload\\新建文件夹\\hadoop-2.6.0-cdh5.15.1");
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://192.168.43.26:8020");
        //创建一个job
        Job job = Job.getInstance(configuration);
        //设置job对应的参数,主类
        job.setJarByClass(WordCountApp.class);
        //设置自Job对应的参数，设置自定义的Mapper和Reducer处理类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setCombinerClass(WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);//设置reduce的输出
        job.setOutputValueClass(IntWritable.class);//设置reduce的输出

        //如果输出目录已经存在，则先删除
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.43.26:8020"),configuration,"hadoop");
        Path path = new Path("/wordcount/output");
        if(fileSystem.exists(path)){
            fileSystem.delete(path,true);
        }
        //设置作业输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));
        FileOutputFormat.setOutputPath(job, path);
        //提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : -1);
    }
}
