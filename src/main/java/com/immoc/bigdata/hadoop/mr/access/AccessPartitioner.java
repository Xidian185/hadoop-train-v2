package com.immoc.bigdata.hadoop.mr.access;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 将Mapper处理的结果，根据规则进行分发，分发到不同的reducer中去。
 * 至于分发到哪个reducer中去，是getPartition函数的返回值和reducer个数取模
 */
public class AccessPartitioner extends Partitioner<Text,Access> {

    /**
     *
     * @param text 手机号
     * @param access
     * @param i
     * @return
     */
    @Override
    public int getPartition(Text text, Access access, int i) {
        if(text.toString().startsWith("13")){
            return 0;
        }else if(text.toString().startsWith("15")){
            return 1;
        }else{
            return 2;
        }
    }
}
