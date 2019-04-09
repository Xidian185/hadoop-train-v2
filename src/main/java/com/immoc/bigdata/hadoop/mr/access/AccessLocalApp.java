package com.immoc.bigdata.hadoop.mr.access;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

public class AccessLocalApp {
    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://192.168.43.26:8020");
        System.setProperty("HADOOP_USER_NAME","hadoop");
        System.setProperty("hadoop.home.dir", "E:\\BaiduNetdiskDownload\\新建文件夹\\hadoop-2.6.0-cdh5.15.1");
        Job job = Job.getInstance(configuration);

        job.setJarByClass(AccessLocalApp.class);
        job.setMapperClass(AccessMapper.class);
        job.setReducerClass(AccessReducer.class);
        //设置自定义分区规则
        job.setPartitionerClass(AccessPartitioner.class);
        //设置分区个数
        job.setNumReduceTasks(3);
        job.setMapOutputValueClass(Access.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Access.class);

        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.43.26:8020"),configuration,"hadoop");
        Path path = new Path("/access/output");
        if(fileSystem.exists(path)){
            fileSystem.delete(path, true);
        }
        FileInputFormat.setInputPaths(job, new Path("/access/input"));
        FileOutputFormat.setOutputPath(job, path);

        boolean mark = job.waitForCompletion(true);
        System.exit(mark ? 0 : 1);
    }
}
