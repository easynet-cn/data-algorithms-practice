package com.easynetcn.data.algorithms.practice.chapter04;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import tl.lin.data.pair.PairOfStrings;

public class LeftJoinReducer extends Reducer<PairOfStrings, PairOfStrings, Text, Text> {
	private Text productId = new Text();
	private Text locationId = new Text("");

	@Override
	public void reduce(PairOfStrings key, Iterable<PairOfStrings> values, Context context)
			throws IOException, InterruptedException {
		Iterator<PairOfStrings> iterator = values.iterator();

		if (iterator.hasNext()) {
			PairOfStrings firstPair = iterator.next();

			if (firstPair.getLeftElement().equals("L")) {
				locationId.set(firstPair.getRightElement());
			}
		}

		while (iterator.hasNext()) {
			PairOfStrings productPair = iterator.next();

			productId.set(productPair.getRightElement());

			context.write(productId, locationId);
		}
	}
}
