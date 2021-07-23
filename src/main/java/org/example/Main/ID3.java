package org.example.Main;


import org.apache.hadoop.fs.FSDataOutputStream;
import org.example.App;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;


public class ID3 {

    static ArrayList<String> label = new ArrayList<>();                        //特征标签
    private String kind;
    static ArrayList<String> kinds=new ArrayList<>();

    public void init() throws IOException {
        String[] temp = App.getAttr();
        for (int i = 0; i < temp.length; i++) {
            label.add(temp[i]);
        }
        String[] temp1 =App.getKind();
        for (String s : temp1) {
            kinds.add(s);
        }
    }

    //计算信息增益最大属性
    public HashMap<String, String> Gain(String[] otherArgs) throws IOException, ClassNotFoundException, InterruptedException {
        return App.getIndex(otherArgs);
    }


    //构建决策树
    public treeNode createTree(String[] args) throws InterruptedException, IOException, ClassNotFoundException {

        HashMap<String, String> index = Gain(args);

//        不用拆分文件，是叶子节点
        if (Double.parseDouble(index.get("value") + "") == 0) {

//            System.out.println("-----------type:  "+args[0]+"   "+App.getType(args[0]));
            return new treeNode(App.getType(args[0]));

        }
        else {
//            需要拆分文件
            treeNode node = new treeNode(label.get(Integer.parseInt("" + index.get("index"))));
            ArrayList<String> sons = App.StartJob2(args);

            for (String son : sons) {
                String[] nameTeamp = son.split("/");
                String name = nameTeamp[nameTeamp.length - 1].replace("-r-00000", "");
                node.label.add(name);
                String[] argsSon = new String[2];
                argsSon[0] = son;
                argsSon[1] = son + "-son";
                node.node.add(createTree(argsSon));
            }
            return node;
        }
    }


    //用于递归的函数（写出到文件中）
    public void put(treeNode node,FSDataOutputStream out) throws IOException {
        out.write(("结点：" + node.getsname() + "\n").getBytes());
        System.out.println("结点：" + node.getsname() + "\n");
        for (int i = 0; i < node.label.size(); i++) {
            out.write((node.getsname() + "的标签属性:" + node.label.get(i)+"\n").getBytes());
            System.out.println(node.getsname() + "的标签属性:" + node.label.get(i));
            if (node.node.get(i).node.isEmpty()) {
                out.write(("叶子结点：" + node.node.get(i).getsname()+"\n").getBytes());
                System.out.println("叶子结点：" + node.node.get(i).getsname());
            } else {
                put(node.node.get(i),out);
            }
        }
    }


    //用于对待决策数据进行预测并将结果保存在指定路径
    public static void testData(ArrayList<String> test, ArrayList<treeNode> nodes, FSDataOutputStream out) throws IOException {

        HashMap<String,Integer> countResult=new HashMap<>();

        for (treeNode node:nodes){
            String temp=testPut(node,test);
            if(countResult.containsKey(temp)){
                countResult.put(temp,countResult.get(temp)+1);
            }
            else {
                countResult.put(temp,1);
            }
        }

        int max=0;
        String typeResult="";
        //找到最大的投票
        for (String type:countResult.keySet()){
            if (countResult.get(type)>=max){
                typeResult=type;
                max=countResult.get(type);
            }
        }

        //拿到结果，输出到文件
        String re="";
        for (int i=0;i<test.size();i++){
            re+=test.get(i)+" ";
        }
        re+=typeResult;

        out.write((re+"\n").getBytes());
//        System.out.println("该次分类结果正确率为：" + (double) count / test.size() * 100 + "%");
    }


    //用于测试的递归调用
    public static String testPut(treeNode node, ArrayList<String> t) {
        int index = 0;
        for (int i = 0; i < label.size(); i++) {
            if (label.get(i).equals(node.getsname())) {
                index = i;
                break;
            }
        }
        for (int i = 0; i < node.label.size(); i++) {
            if (!t.get(index).equals(node.label.get(i))) {
                continue;
            }
            if (node.node.get(i).node.isEmpty()) {
                return node.node.get(i).getsname();     //取出分类结果
            } else {
               return testPut(node.node.get(i), t);
            }
        }
        //如果找不到，随机返回吧
        System.out.println("-------------随机返回");
        Random random=new Random();
        System.out.println(kinds.size());
        System.out.println(kinds);
        return kinds.get(random.nextInt(kinds.size()));
    }


}