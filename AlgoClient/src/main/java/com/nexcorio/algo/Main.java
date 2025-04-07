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
		
		log.info("Fun Begins");
		
		KiteHelper kiteHelper = new KiteHelper();
		KiteConnect kiteConnect = kiteHelper.login();
		
		kiteHelper.populateInstruments();
		
		List<Long> zerodhaTokensToSubscribe = kiteHelper.getZerodhaTokensToSubscribe();
		log.info("zerodhaTokensToSubscribe {}", zerodhaTokensToSubscribe);
		
		// Wait till 9:10:05
		try {
			while  ( (new Date()).before(KiteUtil.getDailyCustomTime(9, 10, 5)) )  {
				log.info("Too early going to sleep sleeping");
				Thread.sleep(30*1000);
			}
			log.info("Ready to fire");
			kiteHelper.tickerUsage(kiteConnect, (ArrayList<Long>) zerodhaTokensToSubscribe);
		} catch (InterruptedException | IOException | WebSocketException | KiteException e) {
			e.printStackTrace();
		}
		
		
		
		log.info("Fun Ends");
				
	}

}
