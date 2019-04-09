package com.immoc.bigdata.hadoop.mr.access;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper<LongWritable, Text, Text, Access>中的参数代表的是map处理的输入输出。例如，context写入的只能是一个Text对一个Access
 */
public class AccessMapper extends Mapper<LongWritable, Text, Text, Access> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split("\t");
        String phone = words[1];
        long up = Long.parseLong(words[words.length - 3]);
        long down = Long.parseLong(words[words.length - 2]);

        Access access = new Access(phone,up,down);
        context.write(new Text(phone), access);
    }
}
