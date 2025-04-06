package com.nexcorio.algo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.neovisionaries.ws.client.WebSocketException;
import com.nexcorio.algo.kite.KiteHelper;
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
		try {
			kiteHelper.tickerUsage(kiteConnect, (ArrayList<Long>) zerodhaTokensToSubscribe);
		} catch (Exception | KiteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		log.info("Fun Ends");
				
	}

}
