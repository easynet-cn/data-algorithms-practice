# chapter 03

hadoop fs -mkdir -p /kv/input
hadoop fs -rm -r /kv/output
hadoop jar data-algorithms-practice-1.0.0.jar com.easynetcn.data.algorithms.practice.chapter03.AggregateByKeyDriver /kv/input /kv/output
hadoop fs -cat /kv/output/part*