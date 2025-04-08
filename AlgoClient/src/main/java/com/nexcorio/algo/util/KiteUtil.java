package com.nexcorio.algo.util;

import java.util.Calendar;
import java.util.Date;

/**
 * 
 * @author Keshav Shetty
 *
 */
public class KiteUtil {

	public static Date getDailyCustomTime(int hour, int minute, int second) {
		Calendar cal = Calendar.getInstance();			
		cal.set(Calendar.HOUR_OF_DAY, hour);
		cal.set(Calendar.MINUTE, minute);
		cal.set(Calendar.SECOND, second);
		return cal.getTime();
	}
	
}
