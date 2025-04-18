package com.nexcorio.algo.backtest;

import java.lang.reflect.Constructor;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
	
	public static void main(String[] args) {
		triggerAlgo(51L, "2025-04-15 09:20:00");
	}


}
