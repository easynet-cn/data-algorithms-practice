package com.easynetcn.data.algorithms.practice.chapter02;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.easynetcn.data.algorithms.practice.util.DateUtil;

public class SecondarySortMapper extends Mapper<LongWritable, Text, CompositeKey, NaturalValue> {
	private final CompositeKey compositeKey = new CompositeKey();
	private final NaturalValue naturalValue = new NaturalValue();

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] tokens = value.toString().split(",");
		Date date = DateUtil.getDate(tokens[1]);

		if (null != date) {
			long timestamp = date.getTime();

			compositeKey.set(tokens[0], timestamp);
			naturalValue.set(timestamp, Double.parseDouble(tokens[2]));

			context.write(compositeKey, naturalValue);
		}
	}
}
