package com.easynetcn.data.algorithms.practice.chapter02;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.easynetcn.data.algorithms.practice.util.DateUtil;

public class SecondarySortReducer extends Reducer<CompositeKey, NaturalValue, Text, Text> {
	@Override
	public void reduce(CompositeKey key, Iterable<NaturalValue> values, Context context)
			throws IOException, InterruptedException {
		StringBuilder sb = new StringBuilder();

		for (NaturalValue value : values) {
			sb.append("(").append(DateUtil.getDateAsString(value.getTimestamp())).append(",").append(value.getPrice())
					.append(")");
		}

		context.write(new Text(key.getStockSymbol()), new Text(sb.toString()));
	}
}
