package com.easynetcn.data.algorithms.practice.chapter02;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class NaturalKeyGroupingComparator extends WritableComparator {
	protected NaturalKeyGroupingComparator() {
		super(CompositeKey.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		CompositeKey compositeKey1 = (CompositeKey) a;
		CompositeKey compositeKey2 = (CompositeKey) b;

		return compositeKey1.getStockSymbol().compareTo(compositeKey2.getStockSymbol());
	}
}
