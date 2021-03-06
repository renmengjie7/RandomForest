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
            System.out.println("--------?????????---------");
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
        System.out.println("?????????num???random???");
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

    //?????????
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
        //?????????????????????index
        //??????????????????
        Job job=Job.getInstance(new Configuration());

        //???????????????????????????
        job.setJarByClass(App.class);

        //??????MyMapper???
        job.setMapperClass(AttributeMapper.class);
        //??????combine???
        job.setCombinerClass(AttributeCombine.class);
        //??????MyReduce???
        job.setReducerClass(AttributeReducer.class);

        //??????????????????
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        //??????????????????
        FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
        //??????????????????
        FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]+"/mapreduce1"));
        //?????????????????????
        return job.waitForCompletion(true);
    }


    public static ArrayList<String> StartJob2(String[] otherArgs) throws IOException, ClassNotFoundException, InterruptedException {

        Job job1=Job.getInstance(configuration);
        job1.setJarByClass(App.class);
        job1.setMapperClass(SplitMapper.class);
        job1.setReducerClass(SplitReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        //??????????????????
        FileInputFormat.addInputPath(job1,new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job1,new Path(otherArgs[1]+"/mapreduce2"));
        MultipleOutputs.addNamedOutput(job1,"abc", TextOutputFormat.class,Text.class,Text.class);
        LazyOutputFormat.setOutputFormatClass(job1, TextOutputFormat.class);
        //?????????????????????
        job1.waitForCompletion(true);

        //??????????????????????????????
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
        //?????????????????????hdfs???????????????????????????????????????hdfs-site.xml?????????????????????source??????????????????????????????hdfs
        Path path=new Path(pathDir+"/");
        FileStatus[] stats=hdfs.listStatus(path);

        ArrayList<String> paths=new ArrayList<>();
        for (int i=0; i<stats.length; i++){
            //????????????????????????
            String pathTemp=stats[i].getPath().toString();
            if(!pathTemp.contains("_SUCCESS")){
                paths.add(stats[i].getPath().toString());
            }
        }
        return paths;
    }


    public static boolean generateRandomFile() throws IOException, ClassNotFoundException, InterruptedException {
        //?????????????????????index
        //??????????????????
        Job job=Job.getInstance(configuration);

        //???????????????????????????
        job.setJarByClass(App.class);

        //??????MyMapper???
        job.setMapperClass(ForestMapper.class);
        //??????MyReduce???
        job.setReducerClass(ForestReducer.class);

        //??????????????????
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //??????????????????
        FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
        //??????????????????
        FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]+"/input"));

        MultipleOutputs.addNamedOutput(job,"abc", TextOutputFormat.class,Text.class,Text.class);
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        //?????????????????????
        return job.waitForCompletion(true);
    }


    public static ArrayList<ArrayList<String>> readTestData( String path) throws IOException {
        FileSystem fs=FileSystem.get(configuration);
        ArrayList<ArrayList<String>> arrayLists=new ArrayList<>();
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
            String tmp;
            while ((tmp = reader.readLine()) != null) {
                //????????????
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


    //????????????????????????
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
