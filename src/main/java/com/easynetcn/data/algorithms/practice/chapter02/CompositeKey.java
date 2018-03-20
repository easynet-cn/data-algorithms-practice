package com.easynetcn.data.algorithms.practice.chapter02;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKey implements WritableComparable<CompositeKey> {

	private String stockSymbol;
	private long timestamp;

	public CompositeKey() {

	}

	public CompositeKey(String stockSymbol, long timestamp) {
		this.stockSymbol = stockSymbol;
		this.timestamp = timestamp;
	}

	public String getStockSymbol() {
		return stockSymbol;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void set(String stockSymbol, long timestamp) {
		this.stockSymbol = stockSymbol;
		this.timestamp = timestamp;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.stockSymbol);
		out.writeLong(this.timestamp);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.stockSymbol = in.readUTF();
		this.timestamp = in.readLong();
	}

	@Override
	public int compareTo(CompositeKey o) {
		int compareValue = this.stockSymbol.compareTo(o.getStockSymbol());

		if (compareValue == 0 && this.timestamp != o.getTimestamp()) {
			compareValue = this.timestamp < o.getTimestamp() ? -1 : 1;
		}

		return compareValue;
	}

	public static class CompositeKeyComparator extends WritableComparator {
		public CompositeKeyComparator() {
			super(CompositeKey.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return compareBytes(b1, s1, l1, b2, s2, l2);
		}
	}

	static {
		WritableComparator.define(CompositeKey.class, new CompositeKeyComparator());
	}
}
