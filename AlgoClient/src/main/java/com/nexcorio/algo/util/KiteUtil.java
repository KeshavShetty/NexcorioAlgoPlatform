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
	
	public static Date getDailyCustomTime(Date fromTime, int hour, int minute, int second) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(fromTime);
		cal.set(Calendar.HOUR_OF_DAY, hour);
		cal.set(Calendar.MINUTE, minute);
		cal.set(Calendar.SECOND, second);
		//System.out.println("In getDailyCustomTime "+  cal.getTime());
		return cal.getTime();
	}
	
	public static int getStrike(String tradingSymbol) {
		int returnStrike = 0;
		String optionName = tradingSymbol;
		int rearEnd = optionName.length()-2;
		int frontEnd = rearEnd;
		for(int i=rearEnd-1;i>0;i--) {
			if (Character.isDigit(optionName.charAt(i))) {
				frontEnd = i;
			} else {
				break;
			}
		}
		String strPart = optionName.substring(frontEnd, rearEnd);
		
		if (frontEnd < rearEnd) {
			//System.out.println("For " + optionName + " str=" + strPart); // Disbale
			if (strPart.length()<=5) {
				returnStrike = Integer.parseInt(strPart);
			} else {
				returnStrike = Integer.parseInt(optionName.substring(optionName.length()-7,optionName.length()-2));
			}
		}
		return returnStrike;
	}
	
}
