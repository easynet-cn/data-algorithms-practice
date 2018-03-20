package com.easynetcn.data.algorithms.practice.chapter02;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeyComparator extends WritableComparator {
	protected CompositeKeyComparator() {
		super(CompositeKey.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		CompositeKey compositeKey1 = (CompositeKey) a;
		CompositeKey compositeKey2 = (CompositeKey) b;

		int compareValue = compositeKey1.getStockSymbol().compareTo(compositeKey2.getStockSymbol());

		if (compareValue == 0 && compositeKey1.getTimestamp() != compositeKey2.getTimestamp()) {
			compareValue = compositeKey1.getTimestamp() < compositeKey2.getTimestamp() ? -1 : 1;
		}

		return compareValue;
	}
}
