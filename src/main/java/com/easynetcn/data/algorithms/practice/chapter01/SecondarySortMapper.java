package com.easynetcn.data.algorithms.practice.chapter01;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondarySortMapper extends Mapper<LongWritable, Text, DateTemperaturePair, Text> {
	private final Text temperature = new Text();
	private final DateTemperaturePair dateTemperaturePair = new DateTemperaturePair();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] tokens = line.split(",");

		dateTemperaturePair.setYearMonth(tokens[0] + tokens[1]);
		dateTemperaturePair.setDay(tokens[2]);
		dateTemperaturePair.setTemperature(Integer.parseInt(tokens[3]));

		this.temperature.set(tokens[3]);

		context.write(dateTemperaturePair, temperature);
	}
}
