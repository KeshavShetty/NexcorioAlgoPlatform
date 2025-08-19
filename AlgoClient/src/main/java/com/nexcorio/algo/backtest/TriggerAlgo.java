package com.nexcorio.algo.backtest;

import java.lang.reflect.Constructor;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.experimental.theories.PotentialAssignment;

import com.nexcorio.algo.kite.KiteHelper;

/**
 * 
 * @author Keshav Shetty
 *
 */
public class TriggerAlgo {
	private static final Logger log = LogManager.getLogger(TriggerAlgo.class);
	
	public static void triggerAlgo(Long napAlgoId, String forDate) {
		
		Map<Long, String> retMap = CloneAlgo.getExistingAlgo(napAlgoId);
		if (retMap==null) {
			retMap = CloneAlgo.cloneAlgo(napAlgoId, true, null);
		} else {
			CloneAlgo.deleteBacktestData(retMap.keySet().iterator().next(), forDate);
		}
		
		retMap.keySet().iterator();
		Long algoIdToRun = retMap.keySet().iterator().next();
		
		float indexCheckValue = KiteHelper.getIndexCheck(forDate);
		if (indexCheckValue>0) {
			String algoClassname = retMap.get(algoIdToRun);
			System.out.println("Going o run algo "+algoIdToRun);
			try {
				Class<?> myClass = Class.forName(algoClassname);
				Constructor<?> ctr = myClass.getConstructor(Long.class, String.class);
				Object object = ctr.newInstance(new Object[] { algoIdToRun, forDate });
			} catch (Exception e) {
				log.error(e.getMessage());
				e.printStackTrace();
			}
		} else {
			System.out.println("Not a atrading day");
		}
	}
	
	public static void triggerAlgo(Long napAlgoId, String fromDate, String toDate) {
		SimpleDateFormat postgresLongDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		try {
			Calendar cal = Calendar.getInstance();		
			cal.setTime(postgresLongDateFormat.parse(fromDate));
			
			do {
				System.out.println("Launching algo for day " + postgresLongDateFormat.format(cal.getTime()));
				triggerAlgo(napAlgoId, postgresLongDateFormat.format(cal.getTime()));
				cal.add(Calendar.DATE, 1);
			} while(cal.getTime().before(postgresLongDateFormat.parse(toDate)) || cal.getTime().equals(postgresLongDateFormat.parse(toDate)));
			
			
		} catch(Exception ex) {
			ex.printStackTrace();
		}
		
	}
	
	public static void main(String[] args) {
	
		String forDate = "2025-08-01";
		String toDate  = "2025-08-01";
		
		triggerAlgo(275L, forDate + " 09:20:00", toDate + " 09:21:00");
//		
//		triggerAlgo(246L, forDate + " 09:20:00", toDate + " 09:21:00");
//		triggerAlgo(247L, forDate + " 09:20:00", toDate + " 09:21:00");
//		triggerAlgo(248L, forDate + " 09:20:00", toDate + " 09:21:00");
//		triggerAlgo(249L, forDate + " 09:20:00", toDate + " 09:21:00");
//		triggerAlgo(250L, forDate + " 09:20:00", toDate + " 09:21:00");
//		triggerAlgo(251L, forDate + " 09:20:00", toDate + " 09:21:00");
//		triggerAlgo(252L, forDate + " 09:20:00", toDate + " 09:21:00");
//		triggerAlgo(253L, forDate + " 09:20:00", toDate + " 09:21:00");
//		triggerAlgo(254L, forDate + " 09:20:00", toDate + " 09:21:00");
//		triggerAlgo(255L, forDate + " 09:20:00", toDate + " 09:21:00");
//		triggerAlgo(256L, forDate + " 09:20:00", toDate + " 09:21:00");
//		triggerAlgo(257L, forDate + " 09:20:00", toDate + " 09:21:00");
		
	}
	
}
