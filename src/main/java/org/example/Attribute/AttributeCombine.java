package org.example.Attribute;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

import static java.lang.Math.log;

public class AttributeCombine extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        //这边只拿到了一个key属性，对于同一个属性值，类别的总数，然后得到信息熵
        HashMap<String,HashMap<String,Integer>> result=new HashMap<>();

        //<属性名称，属性取值+分类结果+1>
        //算一下本身的信息熵
        //计数
        int count=0;
        HashMap<String,Integer> hashMap=new HashMap<>();


        for(Text text : values) {
            if(hashMap.containsKey(text.toString().split("\\+")[1])){
                hashMap.put(text.toString().split("\\+")[1],1+hashMap.get(text.toString().split("\\+")[1]));
            }
            else {
                hashMap.put(text.toString().split("\\+")[1],1);
            }
            count++;

//            System.out.println(text.toString());
            String temp[]=text.toString().split("\\+");
            //判断是否有这个分类结果
            if(!result.containsKey(temp[0])){
                HashMap<String,Integer> temp1=new HashMap<>();
                temp1.put(temp[1],1);
                result.put(temp[0],temp1);
            }
            else {
                if(!result.get(temp[0]).containsKey(temp[1])){
                    HashMap<String,Integer> temp1=result.get(temp[0]);
                    temp1.put(temp[1],1);
                    result.put(temp[0],temp1);
                }
                else {
                    HashMap<String,Integer> temp1=result.get(temp[0]);
                    temp1.put(temp[1],1+temp1.get(temp[1]));
                    result.put(temp[0],temp1);
                }
            }
        }

        double original=0;
        for (String s : hashMap.keySet()) {
            original-=hashMap.get(s)*1.0/count*log(hashMap.get(s)*1.0/count)/log(2);
        }

        double sum=0;
        int count1;
        for (String s : result.keySet()) {
            double comentropy=0;
            count1=0;
            //小类数量
            for (String classId: result.get(s).keySet()){
                count1+=result.get(s).get(classId);
            }
            //小类信息熵
            for (String classId: result.get(s).keySet()){
                comentropy-=result.get(s).get(classId)*1.0/count1*log(result.get(s).get(classId)*1.0/count1)/log(2);
            }
            sum+=comentropy*count1/count;
        }

        double gain=original-sum;
        context.write(new Text("comentropy"), new Text(key+"+"+gain));
    }

}