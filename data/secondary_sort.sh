hadoop fs -mkdir /secondary_sort
hadoop fs -mkdir /secondary_sort/input
hadoop fs -rm -r /secondary_sort/output
hadoop jar data-algorithms-practice-1.0.0.jar com.easynetcn.data.algorithms.practice.chapter01.SecondarySortDriver /secondary_sort/input /secondary_sort/output