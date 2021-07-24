 **本项目为随机森林的Hadoop实现**
# 算法介绍


![`}G@~9U6`D%VJBYZ`{8}Z4M](https://user-images.githubusercontent.com/70495062/126855500-ec026f9f-aefb-4542-b7f1-2c60855a60fc.png)
![@32E6N$HB2A(M7HC M@J7I](https://user-images.githubusercontent.com/70495062/126855504-74365b4a-fe48-431b-8470-10f0df7a5bdc.png)





# mapreduce设计
## 流程图
![GI$EF$Y% P629@SF8A`CI6A](https://user-images.githubusercontent.com/70495062/126855518-5a1a93f3-5d65-4935-98fe-417e47c03491.png)
## 1.随机抽样训练集——分裂文件
![TX~WDHA51WO0Z2HU%Z~ Y)3](https://user-images.githubusercontent.com/70495062/126855560-d2aadcfb-b034-46e6-8204-d014deab4da7.png)
## 2.得到最佳分裂属性——计算
![SA (8%F19L(APAYK7JLIQAN](https://user-images.githubusercontent.com/70495062/126855561-62af00d2-17c9-4c11-b2c0-8d240d37a1b9.png)
## 3.根据最佳分裂属性分裂数据集——分裂文件
![{%1S_}{ZDVVT3)OTXIV KF0](https://user-images.githubusercontent.com/70495062/126855562-c361c4a2-440a-4b8f-940f-f3c6e66b52df.png)


# 编程实现
![T{D LBVJ@{W_K74 ADP0U)T](https://user-images.githubusercontent.com/70495062/126855568-c99e3c7d-ba9f-4be6-bfba-e7c34c377c0a.png)


# 算法效果
## 运行过程
![5(`}6B6 `LJU14USWQ5W{~C](https://user-images.githubusercontent.com/70495062/126855623-e628ea58-3fab-4bde-8825-cb07af87d975.png)

## 决策树可视化
![image](https://user-images.githubusercontent.com/70495062/126855640-384d5e8a-f61d-475c-b73d-d0ded48093d4.png)
