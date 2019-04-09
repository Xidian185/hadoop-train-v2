package com.immoc.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class HDFSApp01 {
    public static void main(String[] args) throws Exception{
        Properties properties = ParamUtils.getProperties();
        //1)读取hdfs上面的文件==> HDFS API
        Path input = new Path(properties.getProperty(Constants.INPUT_PATH));
        //获取HDFS文件系统
        FileSystem fileSystem = FileSystem.get(new URI(properties.getProperty(Constants.HDFS_URI)), new Configuration(),"hadoop");
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(input,false);

        //ImoocMapper wordCountMapper = new WordCountMapper();
        //使用反射创建类
        Class<?> clazz = Class.forName(properties.getProperty(Constants.MAPPER_CLASS));
        ImoocMapper wordCountMapper = (WordCountMapper)clazz.newInstance();

        ImoocContext context = new ImoocContext();
        while(iterator.hasNext()){
            LocatedFileStatus file = iterator.next();
            FSDataInputStream fsDataInputStream = fileSystem.open(file.getPath());
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fsDataInputStream));

            String line = "";
            while((line = bufferedReader.readLine()) != null){
                //2)进行词频处理
                wordCountMapper.map(line,context);
            }
            bufferedReader.close();
            fsDataInputStream.close();
        }
        //3)将内容缓存
        Map<Object,Object> contextMap = context.getCacheMap();
        //4)将结果写回hdfs == > hdfs api
        Path output = new Path(properties.getProperty(Constants.OUTPUT_PATH));
        FSDataOutputStream out = fileSystem.create(new Path(output, new Path(properties.getProperty(Constants.OUPUT_FILE))));
        //将第三部缓存的东西写进去
        Set<Map.Entry<Object, Object>> mapSet =  contextMap.entrySet();
        for(Map.Entry<Object, Object> set : mapSet){
            out.write((set.getKey() + "\t" + set.getValue() + "\n").getBytes());
        }
        out.close();
        fileSystem.close();

        System.out.println("统计结束！");
    }
}
