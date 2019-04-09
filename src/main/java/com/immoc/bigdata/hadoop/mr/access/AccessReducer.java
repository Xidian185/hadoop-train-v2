package com.immoc.bigdata.hadoop.mr.access;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class AccessReducer extends Reducer<Text,Access,Text,Access> {
    @Override
    protected void reduce(Text key, Iterable<Access> values, Context context) throws IOException, InterruptedException {
        long ups = 0;
        long downs = 0;
        Iterator<Access> itr = values.iterator();
        while (itr.hasNext()){
            Access access = itr.next();
            ups += access.getUp();
            downs += access.getDown();
        }
        Access access = new Access(key.toString(),ups,downs);
        context.write(key,access);
    }
}
