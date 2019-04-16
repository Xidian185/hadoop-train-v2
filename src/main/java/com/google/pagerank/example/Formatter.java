package com.google.pagerank.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.util.*;

/**
 * 此类用于对循环计算的PageRank进行规范化，是他们和为1
 */
public class Formatter {
    private static Job job;
    private static Configuration configuration;
    public Formatter(Configuration configuration) throws Exception{
        this.configuration = configuration;
        this.job = Job.getInstance(configuration);
    }
    public static class FormatMapper extends Mapper<LongWritable, Text, Text, Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //System.out.println(value.toString());
            context.write(new Text("1"), value);
        }
    }
    public static class FormatReducer extends Reducer<Text, Text, Text, Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> itr = values.iterator();
            float sum = 0;
            Map<String,String> mapV = new HashMap<String,String>();
            while (itr.hasNext()){
                String[] valuess = itr.next().toString().split("\t");
                mapV.put(valuess[0], valuess[1]);
                sum += Float.parseFloat(valuess[1]);
            }
            Set<String> setK = mapV.keySet();
            Iterator<String> itr2 = setK.iterator();
            while(itr2.hasNext()){
                String k = itr2.next();
                String v = mapV.get(k);
                context.write(new Text(k), new Text(Float.parseFloat(v)/sum+""));
            }
        }
    }
    public static boolean runFormat() throws Exception{

        job.setJarByClass(Formatter.class);
        job.setMapperClass(FormatMapper.class);
        job.setReducerClass(FormatReducer.class);
        //设置自定义分区规则
        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Properties properties = ParamUtils.getProperties();
        FileSystem fileSystem = FileSystem.get(new URI(properties.getProperty(Constants.HDFS_URI)),
                configuration,"hadoop");
        Path path = new Path(properties.getProperty(Constants.FORMAT_OUTPUT_PATH) +
                properties.getProperty(Constants.FORMAT_OUTPUT_FILE));
        if(fileSystem.exists(path)){
            fileSystem.delete(path, true);
        }
        FileInputFormat.setInputPaths(job, new Path(properties.getProperty(Constants.FORMAT_INPUT_PATH)));
        FileOutputFormat.setOutputPath(job, path);

        boolean mark = job.waitForCompletion(true);
        return mark;
        //System.exit(mark ? 0 : 1);
    }
}
