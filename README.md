# Flinl10_learn
主要用于学习 flink 10 版本的目的

## 包目录介绍
batch存放 批代码的包

stream存放 流代码的包



## 问题
### 从 flink 官网使用 maven 初始化的项目 问题
1.idea 本地运行 提示 缺包问题。修改 pom文件 dependency 的 scope 范围，可以直接注释掉这个 选项