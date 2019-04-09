package com.immoc.bigdata.hadoop.hdfs;

public class WordCountMapper implements ImoocMapper{

    @Override
    public void map(String line, ImoocContext context) {
        String[] words = line.trim().split(" ");

        for(String word : words){
            word = word.toLowerCase();
            Object value = context.get(word);
            if(value == null){
                context.write(word, 1);
            }else{
                int v = Integer.parseInt(value.toString());
                context.write(word, ++v);
            }
        }
    }
}
