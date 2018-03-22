package com.easynetcn.data.algorithms.practice.chapter03;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AggregateByKeyMapper extends Mapper<Object, Text, Text, IntWritable> {
	private Text k = new Text();
	private IntWritable v = new IntWritable();

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] tokens = value.toString().split(",");

		k.set(tokens[0]);
		v.set(Integer.parseInt(tokens[1]));

		context.write(k, v);
	}
}
