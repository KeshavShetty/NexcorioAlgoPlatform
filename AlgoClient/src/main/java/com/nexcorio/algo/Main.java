package com.nexcorio.algo;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.analytics.ATMMovementAnalyzerThreadAlgoThread;
import com.nexcorio.algo.core.G3NapAlgoTriggerThread;
import com.nexcorio.algo.dto.MainInstruments;
import com.nexcorio.algo.kite.KiteHelper;
import com.nexcorio.algo.oms.OrderExecutionThreadAlgoThread;
import com.nexcorio.algo.util.KiteUtil;
import com.zerodhatech.kiteconnect.kitehttp.exceptions.KiteException;

/**
 * 
 * @author Keshav Shetty
 *
 */
public class Main {

	private static final Logger log = LogManager.getLogger(Main.class);
	
	public static void main(String[] args) {
		
		log.info("Time starts now");
		
		KiteHelper kiteHelper = new KiteHelper();
		kiteHelper.login(); // First login to Kite 
		log.info("Login done");
		
		kiteHelper.populateInstruments();
				
		// Wait till 9:10:05
		try {
			while  ( (new Date()).before(KiteUtil.getDailyCustomTime(9, 10, 5)) )  {
				log.info("Too early going to sleep for 30 seconds");
				Thread.sleep(30*1000);
			}
			System.out.println("Ready to fire");
			
			List<Long> zerodhaTokensToSubscribe = kiteHelper.getZerodhaTokensToSubscribe();
			log.info("zerodhaToken size To Subscribe {}", zerodhaTokensToSubscribe.size());
			
			kiteHelper.tickerUsage((ArrayList<Long>) zerodhaTokensToSubscribe);
			
			List<MainInstruments> mainInstruments = kiteHelper.getMainInstrumentsDto();
			for(MainInstruments mainInstrument : mainInstruments) {
				if (!mainInstrument.getShortName().equals("VIX")) { // Exclude VIX (Vix has no options
					new ATMMovementAnalyzerThreadAlgoThread(mainInstrument.getShortName(), null);
				}
			}
			
			new OrderExecutionThreadAlgoThread(1L); // Todo: For each user separate thread should start
			new G3NapAlgoTriggerThread();
			
		} catch (Exception | KiteException e) {
			e.printStackTrace();
			log.error("Exception in main", e);
		}
		
		log.info("I am done, let the childern take care of themselves");
				
	}

}
