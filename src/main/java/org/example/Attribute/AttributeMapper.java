package org.example.Attribute;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//第一层mapreduce的mapper——用来计算  根据每个属性分类得到  统计信息   （最终得到最佳分裂属性）
public class AttributeMapper extends Mapper<Object, Text, Text, Text> {

    private Text word = new Text();
    private Text outVal = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        //一行数据
        String[] results=value.toString().split(" ");
        for (int i=0;i<results.length-1;i++){
            word.set(i+"");
            outVal.set(results[i]+"+"+results[results.length-1]+"+1");
            //<属性名称，属性取值+分类结果+1>
            context.write(word,outVal);
        }
    }


}
