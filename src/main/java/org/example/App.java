package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.example.Attribute.AttributeCombine;
import org.example.Attribute.AttributeMapper;
import org.example.Attribute.AttributeReducer;
import org.example.Forest.ForestMapper;
import org.example.Forest.ForestReducer;
import org.example.Main.ID3;
import org.example.Main.treeNode;
import org.example.Split.SplitMapper;
import org.example.Split.SplitReducer;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;


public class App 
{

    public static String[] otherArgs;
    public static Configuration configuration;
    public static String[] args;
    public static int num;
    public static int randNum;
    public static FileSystem fileSystem;

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        init(args);

        generateRandomFile();

        ArrayList<ID3> arrayList=new ArrayList<>();
        ArrayList<treeNode> treeNodeArrayList=new ArrayList<>();
        for (int i=1;i<num+1;i++){
            ID3 id=new ID3();
            id.init();
            otherArgs[0]=args[1]+"/input/"+i+"-r-00000";
            otherArgs[1]=args[1]+"/output"+i;
            treeNodeArrayList.add(id.createTree(otherArgs));
            arrayList.add(id);
        }

        ArrayList<ArrayList<String>> arrayLists=readTestData(args[3]);
        for (int i=0;i<num;i++) {
            System.out.println("--------一棵树---------");
            FSDataOutputStream out=fileSystem.create(new Path(args[4]+"/finalOut/tree"+(i+1)));
            arrayList.get(i).put(treeNodeArrayList.get(i),out);
            out.close();
        }

        System.out.println(arrayLists.size());
        fileSystem.create(new Path(args[4]+"/finalOut/predict")).close();
        FSDataOutputStream out=fileSystem.append(new Path(args[4]+"/finalOut/predict"));
        for (int i=0;i<arrayLists.size();i++){
            ID3.testData(arrayLists.get(i),treeNodeArrayList,out);
        }
        out.close();

    }

    public static void init(String[] args) throws IOException {
        App.args=args;
        configuration=new Configuration();
        configuration.setBoolean("dfs.support.append",true);
        otherArgs=new GenericOptionsParser(configuration,args).getRemainingArgs();
        if(otherArgs.length!=6){
            System.err.println("Usage: RandomForest <in> <out> <label> <test> <testOut> <kind>");
            System.exit(2);
        }

        Scanner scanner=new Scanner(System.in);
        System.out.println("请输入num和random：");
        num=scanner.nextInt();
        randNum=scanner.nextInt();
        configuration.set("num",num+"");
        configuration.set("randNum",randNum+"");
        fileSystem= FileSystem.get(configuration);
    }

    public static String[] getAttr()  {
        try {
            FileSystem hdfs=FileSystem.get(configuration);
            FSDataInputStream his = hdfs.open(new Path(args[2]));
            byte[] buff = new byte[1024];
            int length;
            String result="";
            while ((length = his.read(buff)) != -1) {
                result+=new String(buff, 0, length);
            }
            return result.split(" ");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new String[]{};
    }

    public static String[] getKind()  {
        try {
            FileSystem hdfs=FileSystem.get(configuration);
            FSDataInputStream his = hdfs.open(new Path(args[5]));
            byte[] buff = new byte[1024];
            int length;
            String result="";
            while ((length = his.read(buff)) != -1) {
                result+=new String(buff, 0, length);
            }
            return result.split(" ");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new String[]{};
    }

    //拿到值
    public static HashMap<String,String> getIndex(String[] otherArgs) throws InterruptedException, IOException, ClassNotFoundException {
        StartJob1(otherArgs);
        String resultTemp=readFile(FileSystem.get(configuration),otherArgs[1]+"/mapreduce1/part-r-00000");
        String result[]=resultTemp.split(" ")[0].split("\t");

        HashMap<String,String> hashMap=new HashMap<>();
        hashMap.put("index",result[0]);
        hashMap.put("value",result[1]);

        configuration.set("index",result[0]);

        return hashMap;
    }


    public static Boolean StartJob1(String[] otherArgs) throws IOException, ClassNotFoundException, InterruptedException {
        //得到最大的增益index
        //设置环境参数
        Job job=Job.getInstance(new Configuration());

        //设置该任务的启动类
        job.setJarByClass(App.class);

        //添加MyMapper类
        job.setMapperClass(AttributeMapper.class);
        //添加combine类
        job.setCombinerClass(AttributeCombine.class);
        //添加MyReduce类
        job.setReducerClass(AttributeReducer.class);

        //设置输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        //设置输入文件
        FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
        //设置输出文件
        FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]+"/mapreduce1"));
        //启动任务并执行
        return job.waitForCompletion(true);
    }


    public static ArrayList<String> StartJob2(String[] otherArgs) throws IOException, ClassNotFoundException, InterruptedException {

        Job job1=Job.getInstance(configuration);
        job1.setJarByClass(App.class);
        job1.setMapperClass(SplitMapper.class);
        job1.setReducerClass(SplitReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        //设置输入文件
        FileInputFormat.addInputPath(job1,new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job1,new Path(otherArgs[1]+"/mapreduce2"));
        MultipleOutputs.addNamedOutput(job1,"abc", TextOutputFormat.class,Text.class,Text.class);
        LazyOutputFormat.setOutputFormatClass(job1, TextOutputFormat.class);
        //启动任务并执行
        job1.waitForCompletion(true);

        //拿到分裂的文件名列表
        return App.getFilenames(otherArgs[1]+"/mapreduce2");
    }


    public static String readFile(FileSystem fs, String path){
        try {
            FSDataInputStream his = fs.open(new Path(path));
            byte[] buff = new byte[1024];
            int length = 0;
            String result="";
            while ((length = his.read(buff)) != -1) {
                result+=new String(buff, 0, length);
            }
            return result;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }


    public static ArrayList<String> getFilenames(String pathDir) throws IOException {
        FileSystem hdfs=FileSystem.get(configuration);
        //这里的路径是在hdfs上的存放路径，但是事先需将hdfs-site.xml文件放在工程的source文件下，这样才能找到hdfs
        Path path=new Path(pathDir+"/");
        FileStatus[] stats=hdfs.listStatus(path);

        ArrayList<String> paths=new ArrayList<>();
        for (int i=0; i<stats.length; i++){
            //打印每个文件路径
            String pathTemp=stats[i].getPath().toString();
            if(!pathTemp.contains("_SUCCESS")){
                paths.add(stats[i].getPath().toString());
            }
        }
        return paths;
    }


    public static boolean generateRandomFile() throws IOException, ClassNotFoundException, InterruptedException {
        //得到最大的增益index
        //设置环境参数
        Job job=Job.getInstance(configuration);

        //设置该任务的启动类
        job.setJarByClass(App.class);

        //添加MyMapper类
        job.setMapperClass(ForestMapper.class);
        //添加MyReduce类
        job.setReducerClass(ForestReducer.class);

        //设置输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //设置输入文件
        FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
        //设置输出文件
        FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]+"/input"));

        MultipleOutputs.addNamedOutput(job,"abc", TextOutputFormat.class,Text.class,Text.class);
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        //启动任务并执行
        return job.waitForCompletion(true);
    }


    public static ArrayList<ArrayList<String>> readTestData( String path) throws IOException {
        FileSystem fs=FileSystem.get(configuration);
        ArrayList<ArrayList<String>> arrayLists=new ArrayList<>();
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
            String tmp;
            while ((tmp = reader.readLine()) != null) {
                //按行处理
                ArrayList<String> arrayList=new ArrayList();
                for (String temp:tmp.split(" ")){
                    arrayList.add(temp);
                }
                arrayLists.add(arrayList);
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return arrayLists;
    }


    //拿到最后一列的值
    public static String getType(String path) throws IOException {
        FileSystem fs=FileSystem.get(configuration);
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
            String tmp;
            String temp="";
            while ((tmp = reader.readLine()) != null) {
                temp=tmp.split(" ")[tmp.split(" ").length-1];
                System.out.println(tmp);
                break;
            }
            return temp;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }

}
