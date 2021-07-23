package org.example.Split;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SplitMapper extends Mapper<Object, Text, Text, Text> {

    private Text word = new Text();
    private Text outVal = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        //最大属性值
        int index=Integer.parseInt(context.getConfiguration().get("index"));
        //一行数据
        String[] temp=value.toString().split(" ");
        word.set(temp[index]);
        outVal.set(value);

        context.write(word,outVal);
    }

}
