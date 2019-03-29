package com.immoc.bigdata;

public class WordCountMapper implements ImoocMapper{

    @Override
    public void map(String line, ImoocContext context) {
        String[] words = line.trim().split(" ");

        for(String word : words){
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
