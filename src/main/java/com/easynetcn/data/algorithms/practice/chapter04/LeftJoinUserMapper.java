package com.easynetcn.data.algorithms.practice.chapter04;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import tl.lin.data.pair.PairOfStrings;

public class LeftJoinUserMapper extends Mapper<LongWritable, Text, PairOfStrings, PairOfStrings> {
	private PairOfStrings outputKey = new PairOfStrings();
	private PairOfStrings outputValue = new PairOfStrings();

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] tokens = value.toString().split("\t");

		outputKey.set(tokens[0], "1");
		outputValue.set("L", tokens[1]);

		context.write(outputKey, outputValue);
	}
}
