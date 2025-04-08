package com.nexcorio.algo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.neovisionaries.ws.client.WebSocketException;
import com.nexcorio.algo.kite.KiteHelper;
import com.nexcorio.algo.util.KiteUtil;
import com.zerodhatech.kiteconnect.KiteConnect;
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
			log.info("Ready to fire");
			
			List<Long> zerodhaTokensToSubscribe = kiteHelper.getZerodhaTokensToSubscribe();
			log.info("zerodhaToken size To Subscribe {}", zerodhaTokensToSubscribe.size());
			
			kiteHelper.tickerUsage((ArrayList<Long>) zerodhaTokensToSubscribe);
			
		} catch (InterruptedException | IOException | WebSocketException | KiteException e) {
			e.printStackTrace();
		}
		
		log.info("I am done, let the childern take care of themselves");
				
	}

}
