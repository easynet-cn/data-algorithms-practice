package com.easynetcn.data.algorithms.practice.chapter01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class SecondarySortDriver extends Configured implements Tool {
	private static final Logger logger = Logger.getLogger(SecondarySortDriver.class);

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		Job job = Job.getInstance(conf, "SecondarySortDriver");

		job.setJarByClass(SecondarySortDriver.class);
		job.setOutputKeyClass(DateTemperaturePair.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(SecondarySortMapper.class);
		job.setReducerClass(SecondarySortReducer.class);
		job.setPartitionerClass(DateTemperaturePartitioner.class);
		job.setGroupingComparatorClass(DateTemperatureGroupingComparator.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean status = job.waitForCompletion(true);

		logger.info("run(): status=" + status);

		return status ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int status = ToolRunner.run(new SecondarySortDriver(), args);

		logger.info("returnStatus=" + status);

		System.exit(status);
	}
}
