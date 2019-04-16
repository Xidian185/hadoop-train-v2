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
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

/**
 * 此函数为数据的预处理，最终输出结果为概率矩阵
 */
public class PreDealer {
    private static int pageNum;//试验页面的总数
    private static float d;//阻尼系数
    private static Job job;
    private static Configuration configuration;
    public PreDealer(int pageNum, float d, Configuration configuration)throws Exception{
        this.pageNum = pageNum;
        this.d = d;
        this.configuration = configuration;
        this.job = Job.getInstance(configuration);
    }
    public static class PreMapper extends Mapper<LongWritable, Text, Text, Text>{
        /**
         *
         * @param key
         * @param value  每一行的格式是：1,2,以逗号分隔，代表1到2有链接
         * @param context  返回的数据格式是：1   2，代表一个链接的起止
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(",");//逗号分隔一行数据
            context.write(new Text(values[0]),new Text(values[1]));
        }
    }
    public static class PreReducer extends Reducer<Text, Text, Text, Text>{
        /**
         *
         * @param key  key代表有发出链接的网页代号
         * @param values  可以链接到的页面组成的串
         * @param context  输出目标是以网页代号为key，可达页面的概率组成的以逗号分隔的串
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //首先，初始化一行float组成的数组，容量为页面总数
            float[] outArray = new float[pageNum];
            Arrays.fill(outArray, (1-d)/pageNum);//自由跳转产生的概率
            int outNum = 0;//链出网页数
            float[] outArrayTrue = new float[pageNum];//存放遍历的values中的链出的网页
            Iterator<Text> itr = values.iterator();
            while(itr.hasNext()){
                int pageIndex = Integer.parseInt(itr.next().toString());
                outArrayTrue[pageIndex-1] = 1;//对应编号设置为1
                outNum ++;//链出总数加一
            }
            StringBuilder stringBuilder = new StringBuilder();
            for(int i=0; i<outArrayTrue.length; i++){//拼接结果
                stringBuilder.append(",").append((outArrayTrue[i]/outNum)*d + outArray[i]);//链出项加上随机量
            }
            context.write(key, new Text(stringBuilder.toString().substring(1)));
        }
    }
    public static boolean runPre() throws Exception{
        job.setJarByClass(PreDealer.class);
        job.setMapperClass(PreMapper.class);
        job.setReducerClass(PreReducer.class);
        //设置自定义分区规则
        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Properties properties = ParamUtils.getProperties();
        FileSystem fileSystem = FileSystem.get(new URI(properties.getProperty(Constants.HDFS_URI)),
                configuration,"hadoop");
        Path path = new Path(properties.getProperty(Constants.MATRIX_OUTPUT_PATH) +
                properties.getProperty(Constants.MATRIX_OUTPUT_FILE));
        if(fileSystem.exists(path)){
            fileSystem.delete(path, true);
        }
        FileInputFormat.setInputPaths(job, new Path(properties.getProperty(Constants.PAGE_INPUT_PATH)));
        FileOutputFormat.setOutputPath(job, path);

        boolean mark = job.waitForCompletion(true);
        return mark;
        //System.exit(mark ? 0 : 1);
    }
}
