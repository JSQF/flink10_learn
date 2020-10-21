# Deploy
## StandAlone
bin/flink run --class com.... -classpath xxx xxx.jar
## Flink On Yarn
bin/flink run -m yarn-cluster --class xxx --classpath xxx xxx.jar

## 当job 有算子增减的时候，加入这个参数，允许删除的算子不加载 实例的 状态
flink run --allowNonRestoredState