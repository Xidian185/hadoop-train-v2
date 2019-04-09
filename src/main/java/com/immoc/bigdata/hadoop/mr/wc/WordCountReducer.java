package com.immoc.bigdata.hadoop.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * reduce1:(hello,1)(hello,1)(hello,1)
     * reduce2:(world,1)(world,1)
     * reduce3:(welcome,1)
     *
     * map的输出到reduce端，是按照相同的key分发到reduce上去执行
     *
     *reduce1:(hello,1)(hello,1)(hello,1) ==>(hello,<1,1,1>)
     * reduce2:(world,1)(world,1) ==> (world,<1,1>)
     * reduce3:(welcome,1) ==>(welcome,<1>)
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        Iterator<IntWritable> itr = values.iterator();
        while (itr.hasNext()){
            IntWritable value = itr.next();
            count += value.get();
        }
        context.write(key, new IntWritable(count));
    }
}
