# chapter 03

hadoop fs -mkdir /top10list
hadoop fs -mkdir /top10list/input
hadoop fs -mkdir /top10list/input/cat_input.seq
hadoop jar data-algorithms-practice-1.0.0.jar com.easynetcn.data.algorithms.practice.chaptSequenceFileWriterForTopN /top10list/input/cat_input.seq 20
hadoop fs -rm -r /top10list/output
hadoop jar data-algorithms-practice-1.0.0.jar com.easynetcn.data.algorithms.practice.chapter03.TopNDriver /top10list/input /top10list/output
hadoop fs -cat /top10list/output/part*