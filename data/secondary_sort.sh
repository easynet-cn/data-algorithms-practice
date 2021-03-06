# chapter 01

hadoop fs -mkdir /secondary_sort
hadoop fs -mkdir /secondary_sort/input
hadoop fs -put sample_input.txt /secondary_sort/input/sample_input.txt
hadoop fs -rm -r /secondary_sort/output
hadoop jar data-algorithms-practice-1.0.0.jar com.easynetcn.data.algorithms.practice.chapter01.SecondarySortDriver /secondary_sort/input /secondary_sort/output
hadoop fs -cat /secondary_sort/output/part*

# chapter 02

hadoop fs -put stock_input.txt /secondary_sort/input/stock_input.txt
hadoop fs -rm -r /secondary_sort/output
hadoop jar data-algorithms-practice-1.0.0.jar com.easynetcn.data.algorithms.practice.chapter02.SecondarySortDriver /secondary_sort/input /secondary_sort/output