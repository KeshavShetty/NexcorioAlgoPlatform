package com.nexcorio.algo.util;

import java.util.Calendar;
import java.util.Date;

/**
 * 
 * @author Keshav Shetty
 *
 */
public class KiteUtil {

	public static boolean USEATM_TRUE = true;
	public static boolean USEATM_FALSE = false;
	
	public static boolean PLACE_ACTUAL_ORDER_TRUE = true;
	public static boolean PLACE_ACTUAL_ORDER_FALSE = false;
	
	public static boolean FILTER_OPTION_WORTH_TRUE = true;
	public static boolean FILTER_OPTION_WORTH_FALSE = false;
	
	public static boolean USE_NORMAL_ORDER_TRUE = true;
	public static boolean USE_NORMAL_ORDER_FALSE = false;
	
	public static String SEGMENT_EQUITY = "equity";
	
	public static Date getDailyCustomTime(int hour, int minute, int second) {
		Calendar cal = Calendar.getInstance();			
		cal.set(Calendar.HOUR_OF_DAY, hour);
		cal.set(Calendar.MINUTE, minute);
		cal.set(Calendar.SECOND, second);
		return cal.getTime();
	}
	
	
}
