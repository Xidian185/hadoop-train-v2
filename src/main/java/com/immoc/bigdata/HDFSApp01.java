package com.immoc.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;
import java.util.Set;

public class HDFSApp01 {
    public static void main(String[] args) throws Exception{
        //1)读取hdfs上面的文件==> HDFS API
        Path input = new Path("/hdfsapi/hello.txt");
        //获取HDFS文件系统
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.43.26:8020"), new Configuration(),"hadoop");
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(input,false);
        ImoocMapper wordCountMapper = new WordCountMapper();
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
        Path output = new Path("/hdfsapi/output/");
        FSDataOutputStream out = fileSystem.create(new Path(output, new Path("wc.out")));
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
