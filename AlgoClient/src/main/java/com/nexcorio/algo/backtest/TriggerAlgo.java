package com.nexcorio.algo.backtest;

import java.lang.reflect.Constructor;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.experimental.theories.PotentialAssignment;

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
		//Long algoId = 42L;
//		triggerAlgo(55L, "2025-04-01 09:20:00", "2025-04-25 09:20:00");
//		triggerAlgo(58L, "2025-04-01 09:20:00", "2025-04-25 09:20:00");
		
		triggerAlgo(114L, "2025-04-21 09:20:00", "2025-05-05 09:24:00");
		
		
		//triggerAlgo(66L, "2025-04-01 09:20:00", "2025-04-25 09:20:00");
//		triggerAlgo(63L, "2025-04-01 09:20:00", "2025-04-25 09:20:00");
//		triggerAlgo(65L, "2025-04-01 09:20:00", "2025-04-25 09:20:00");
		
	}
	
}
