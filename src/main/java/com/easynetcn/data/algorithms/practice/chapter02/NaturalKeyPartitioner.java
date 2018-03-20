package com.easynetcn.data.algorithms.practice.chapter02;

import org.apache.hadoop.mapreduce.Partitioner;

public class NaturalKeyPartitioner extends Partitioner<CompositeKey, NaturalValue> {

	@Override
	public int getPartition(CompositeKey compositeKey, NaturalValue naturalValue, int numberOfPartitions) {
		return Math.abs(compositeKey.getStockSymbol().hashCode() % numberOfPartitions);
	}

}
