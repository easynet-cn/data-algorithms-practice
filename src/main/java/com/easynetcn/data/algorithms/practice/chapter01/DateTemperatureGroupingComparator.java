package com.easynetcn.data.algorithms.practice.chapter01;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DateTemperatureGroupingComparator extends WritableComparator {

	public DateTemperatureGroupingComparator() {
		super(DateTemperaturePair.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		DateTemperaturePair dateTemperaturePair1 = (DateTemperaturePair) a;
		DateTemperaturePair dateTemperaturePair2 = (DateTemperaturePair) b;

		return dateTemperaturePair1.getYearMonth().compareTo(dateTemperaturePair2.getYearMonth());
	}
}
