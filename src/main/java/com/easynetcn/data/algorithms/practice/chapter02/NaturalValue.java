package com.easynetcn.data.algorithms.practice.chapter02;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class NaturalValue implements Writable, Comparable<NaturalValue> {
	private long timestamp;
	private double price;

	public NaturalValue() {

	}

	public NaturalValue(long timestamp, double price) {
		this.timestamp = timestamp;
		this.price = price;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public double getPrice() {
		return price;
	}

	public void set(long timestamp, double price) {
		this.timestamp = timestamp;
		this.price = price;
	}

	@Override
	public int compareTo(NaturalValue o) {
		if (this.timestamp == o.getTimestamp()) {
			return 0;
		}

		return this.timestamp < o.getTimestamp() ? -1 : 1;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(this.timestamp);
		out.writeDouble(this.price);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.timestamp = in.readLong();
		this.price = in.readDouble();
	}

}
