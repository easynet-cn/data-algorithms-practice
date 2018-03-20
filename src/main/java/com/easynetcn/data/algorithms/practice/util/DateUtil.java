package com.easynetcn.data.algorithms.practice.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {
	private static final String DATE_FORMAT = "yyyy-MM-dd";

	public static Date getDate(String text) {
		Date date = null;

		try {
			date = new SimpleDateFormat(DATE_FORMAT).parse(text);
		} catch (ParseException e) {

		}

		return date;
	}

	public static String getDateAsString(long timestamp) {
		return new SimpleDateFormat(DATE_FORMAT).format(timestamp);
	}
}
