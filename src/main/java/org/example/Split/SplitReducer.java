package org.example.Split;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class SplitReducer extends Reducer<Text, Text, Text, Text> {


    private MultipleOutputs<NullWritable, Text> mos;

    public void setup(Context context) throws IOException, InterruptedException {
        mos = new MultipleOutputs(context);
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }

    //这边相当于把每一行写入文件，问题是我怎么写入文件
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //这边拆分字符串，拿到最小的
        for (Text text : values) {
            mos.write("abc", NullWritable.get(), text,key.toString());
        }
    }

}