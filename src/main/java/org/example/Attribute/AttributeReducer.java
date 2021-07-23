package org.example.Attribute;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


//第一层mapreduce的mapper——直接计算信息熵，找到最小的信息熵--->得到最佳裂变属性值
public class AttributeReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double max=0;
        String index="";
        //这边拆分字符串，拿到最小的
        for (Text text:values){
            String[] temp=text.toString().split("\\+");
            if(max<=Double.parseDouble(temp[1])){
                index=temp[0];
                max=Double.parseDouble(temp[1]);
            }
        }
        context.write(new Text(index), new Text(max+""));
    }

}
