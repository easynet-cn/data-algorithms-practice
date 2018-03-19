package com.easynetcn.data.algorithms.practice.chapter01;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondarySortReducer extends Reducer<DateTemperaturePair, Text, Text, Text> {
	@Override
	protected void reduce(DateTemperaturePair key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		StringBuilder sb = new StringBuilder();

		for (Text value : values) {
			sb.append(value.toString());
			sb.append(",");
		}

		context.write(key.getYearMonth(), new Text(sb.toString()));
	}
}
