package com.easynetcn.data.algorithms.practice.chapter03;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopNMapper extends Mapper<Text, IntWritable, NullWritable, Text> {
	private int N = 10;
	private SortedMap<Integer, String> top = new TreeMap<>();

	@Override
	public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
		top.put(value.get(), key.toString() + "," + value.get());

		if (top.size() > N) {
			top.remove(top.firstKey());
		}
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		this.N = context.getConfiguration().getInt("N", 10);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		for (String str : top.values()) {
			context.write(NullWritable.get(), new Text(str));
		}
	}
}
