package com.easynetcn.data.algorithms.practice.chapter04;

import org.apache.hadoop.mapreduce.Partitioner;

import tl.lin.data.pair.PairOfStrings;

public class SecondarySortPartitioner extends Partitioner<PairOfStrings, Object> {

	@Override
	public int getPartition(PairOfStrings key, Object value, int numberOfPartitions) {
		return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numberOfPartitions;
	}

}
