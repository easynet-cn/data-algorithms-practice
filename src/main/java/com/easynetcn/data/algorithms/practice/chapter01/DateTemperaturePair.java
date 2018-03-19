package com.easynetcn.data.algorithms.practice.chapter01;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class DateTemperaturePair implements Writable, WritableComparable<DateTemperaturePair> {
	private Text yearMonth = new Text();
	private Text day = new Text();
	private IntWritable temperature = new IntWritable();

	public DateTemperaturePair() {

	}

	public DateTemperaturePair(String yearMonth, String day, int temperature) {
		this.yearMonth.set(yearMonth);
		this.day.set(day);
		this.temperature.set(temperature);
	}

	public Text getYearMonth() {
		return yearMonth;
	}

	public void setYearMonth(String yearMonth) {
		this.yearMonth.set(yearMonth);
	}

	public Text getDay() {
		return day;
	}

	public void setDay(String day) {
		this.day.set(day);
	}

	public IntWritable getTemperature() {
		return temperature;
	}

	public void setTemperature(int temperature) {
		this.temperature.set(temperature);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		yearMonth.readFields(in);
		day.readFields(in);
		temperature.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		yearMonth.write(out);
		day.write(out);
		temperature.write(out);
	}

	@Override
	public int compareTo(DateTemperaturePair o) {
		int compareValue = this.yearMonth.compareTo(o.getYearMonth());

		if (compareValue == 0) {
			compareValue = this.temperature.compareTo(o.getTemperature());
		}

		return compareValue;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((day == null) ? 0 : day.hashCode());
		result = prime * result + ((temperature == null) ? 0 : temperature.hashCode());
		result = prime * result + ((yearMonth == null) ? 0 : yearMonth.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DateTemperaturePair other = (DateTemperaturePair) obj;
		if (day == null) {
			if (other.day != null)
				return false;
		} else if (!day.equals(other.day))
			return false;
		if (temperature == null) {
			if (other.temperature != null)
				return false;
		} else if (!temperature.equals(other.temperature))
			return false;
		if (yearMonth == null) {
			if (other.yearMonth != null)
				return false;
		} else if (!yearMonth.equals(other.yearMonth))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return new StringBuilder().append("DateTemperaturePair [yearMonth=").append(yearMonth).append(", day=")
				.append(day).append(", temperature=").append(temperature).append("]").toString();
	}

}
