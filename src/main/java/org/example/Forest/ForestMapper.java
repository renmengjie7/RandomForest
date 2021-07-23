package org.example.Forest;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Random;

public class ForestMapper  extends Mapper<Object, Text, Text, Text> {

    private Text word = new Text();
    private Text outVal = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        Random random=new Random();
        //最大属性值
        int num=Integer.parseInt(context.getConfiguration().get("num"));
        int randNum=Integer.parseInt(context.getConfiguration().get("randNum"));

        HashMap<Integer,Text> hashMap=new HashMap<>();
        //生成随机数
        for (int i=0;i<randNum;i++){
            hashMap.put(random.nextInt(num)+1,value);
        }

        //一行数据
        for (Integer i:hashMap.keySet()){
            word.set(i+"");
            outVal.set(hashMap.get(i));
            context.write(word,outVal);
        }
    }

}
