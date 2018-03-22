package com.easynetcn.data.algorithms.practice.chapter03;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class AggregateByKeyDriver extends Configured implements Tool {
	private static final Logger logger = Logger.getLogger(AggregateByKeyDriver.class);

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		Job job = Job.getInstance(conf, "AggregateByKeyDriver");

		job.setJarByClass(AggregateByKeyDriver.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(AggregateByKeyMapper.class);
		job.setReducerClass(AggregateByKeyReducer.class);
		job.setCombinerClass(AggregateByKeyReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean status = job.waitForCompletion(true);

		logger.info("run(): status=" + status);

		return status ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int status = ToolRunner.run(new AggregateByKeyDriver(), args);

		logger.info("returnStatus=" + status);

		System.exit(status);
	}

}
