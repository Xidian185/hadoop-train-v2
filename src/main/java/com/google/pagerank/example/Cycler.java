package com.google.pagerank.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URI;
import java.util.*;

/**
 * 此类代表循环体
 */
public class Cycler {
    private static int pageNum;//试验页面的总数
    private static Configuration configuration;
    public Cycler(int pageNum, Configuration configuration){
        this.pageNum = pageNum;
        this.configuration = configuration;
    }
    public static class CycleMapper extends Mapper<LongWritable, Text, Text, Text>{
        private String filePath = "";
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            filePath =  fileSplit.getPath().toString();
        }

        /**
         *
         * @param key
         * @param value  概率矩阵的一行、或中间计算结果的PageRank（或初始值）
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if(filePath.contains("out")){//概率矩阵
                //value的形式如：1   0.01，0.27,0.27,0.27..
                String[] values = value.toString().split("\t");
                String[] vvs = values[1].split(",");
                for(int i=1; i<=vvs.length; i++){
                    String k = i + "";
                    String v = "M:" + values[0] + "," + vvs[i-1];//代表第values[0]个页面对第k个页面的权重贡献
                    context.write(new Text(k), new Text(v));
                }
            }else if(filePath.contains("pr")){//pagerank值
                //value的形式：1    1，第一个页面的pagerank为1
                String[] values = value.toString().split("\t");
                for(int i=1; i<=pageNum; i++){
                    context.write(new Text(i+""), new Text("P:" +values[0] + "," +values[1]));
                }
            }
        }
    }
    public static class CycleReducer extends Reducer<Text, Text, Text, Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();
            Map<String, String> mapM = new HashMap<String, String>();
            Map<String, String> mapP = new HashMap<String, String>();
            while (iterator.hasNext()){
                String value = iterator.next().toString();

                String[] valuess = value.split(":");
                String[] vvs = valuess[1].split(",");//若为M，vvs[0]代表对哪个页面做贡献，vvs[1]代表贡献值；
                                                            //若为P，vvs[0]代表哪个页面做贡献，vvs[1]代表贡献值；
                if(value.startsWith("M")){//概率贡献
                    mapM.put(vvs[0], vvs[1]);
                }else if(value.startsWith("P")){//上一阶段的Pangerank
                    mapP.put(vvs[0], vvs[1]);
                }
            }
            float sum = 0f;
            for(int i=1; i<=pageNum; i++){
                float m = Float.parseFloat(mapM.get(i+""));
                float p = Float.parseFloat(mapP.get(i+""));
                sum += m * p;
            }
            context.write(key, new Text(sum + ""));
        }
    }
    public static boolean runCycler() throws Exception{
        //不停进行循环，知道计算两次计算结果差值课忽略
        boolean mark = false;
        while(true) {
            Job job = Job.getInstance(configuration);
            job.setJarByClass(Cycler.class);
            job.setMapperClass(CycleMapper.class);
            job.setReducerClass(CycleReducer.class);
            //设置自定义分区规则
            job.setMapOutputValueClass(Text.class);
            job.setMapOutputKeyClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            Properties properties = ParamUtils.getProperties();
            FileSystem fileSystem = FileSystem.get(new URI(properties.getProperty(Constants.HDFS_URI)),
                    configuration, "hadoop");
            Path path = new Path(properties.getProperty(Constants.PAGERANK_OUTPUT_PATH) +
                    properties.getProperty(Constants.PAGERANK_OUTPUT_FILE));
            if (fileSystem.exists(path)) {
                fileSystem.delete(path, true);
            }
            FileOutputFormat.setOutputPath(job, path);
            FileInputFormat.setInputPaths(job, new Path(properties.getProperty(Constants.PAGERANK_INPUT_PATH)),
                    new Path(properties.getProperty(Constants.MATRIX_INPUT_PATH)));

            mark = job.waitForCompletion(true);
            //System.exit(mark ? 0 : 1);

            //读取 /pagerank/pagerank/pagerank.out  和 /pagerank/pr/pagerank.txt，对比每个页面pagerank差值，如果都小于0.01
            //  则停止计算；否则，使用/pagerank/pagerank/pagerank.out替换/pagerank/pr/pagerank.txt
            System.out.println("--------------------"+mark);
            boolean stop = false;
            while (true) {
                if (mark) {
                    FSDataInputStream outFile = fileSystem.open(new Path(properties.getProperty(Constants.PAGERANK_OUTPUT_PATH)
                            +properties.getProperty(Constants.PAGERANK_OUTPUT_FILE)+"/part-r-00000"));
                    FSDataInputStream prFile = fileSystem.open(new Path(properties.getProperty(Constants.PAGERANK_INPUT_PATH)));

                    BufferedReader readerOut = new BufferedReader(new InputStreamReader(outFile));
                    BufferedReader readerPr = new BufferedReader(new InputStreamReader(prFile));
                    Map<String, String> mapM = new HashMap<String, String>();//存储M文件的内容映射
                    Map<String, String> mapP = new HashMap<String, String>();//存储pr文件的内容映射
                    String lineOut = "";
                    String linePr = "";
                    while ((lineOut = readerOut.readLine()) != null) {
                        String[] outS = lineOut.split("\t");
                        mapM.put(outS[0], outS[1]);
                    }
                    while ((linePr = readerPr.readLine()) != null) {
                        String[] outP = linePr.split("\t");
                        mapP.put(outP[0], outP[1]);
                    }
                    boolean tag = true;
                    for (int i = 1; i <= pageNum; i++) {
                        if (Math.abs(Float.parseFloat(mapM.get(i+"")) - Float.parseFloat(mapP.get(i+""))) > 0.1) {
                            tag = false;
                            break;
                        }
                    }
                    if (tag) {//如果不存在差值大于0.01的，那么结束总循环
                        stop = true;
                        break;
                    } else {//替换文件
                        FSDataOutputStream writeFile = fileSystem.create(new Path(properties.getProperty(Constants.PAGERANK_INPUT_PATH)));//如果文件存在，直接覆盖
                        Set<String> keySet = mapM.keySet();
                        Iterator<String> keyItr = keySet.iterator();//pr集的key
                        while (keyItr.hasNext()) {
                            String key = keyItr.next();
                            String value = mapM.get(key);
                            System.out.println("key:"+key+";value:"+value);
                            writeFile.write((key + "\t" + value + "\n").getBytes());
                        }
                        writeFile.close();
                        break;
                    }
                }
            }
            if(stop)
                break;
        }
        return mark;
    }
}
