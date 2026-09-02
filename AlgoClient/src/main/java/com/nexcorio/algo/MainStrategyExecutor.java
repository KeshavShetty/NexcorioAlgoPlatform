package com.nexcorio.algo;

import java.util.Date;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.analytics.ATMMovementAnalyzerThreadAlgoThread;
import com.nexcorio.algo.analytics.V2GreeksMovementAnalyzerThread;
import com.nexcorio.algo.core.G3NapAlgoTriggerThread;
import com.nexcorio.algo.dto.MainInstruments;
import com.nexcorio.algo.kite.KiteHelper;
import com.nexcorio.algo.oms.OrderExecutionThreadAlgoThread;
import com.nexcorio.algo.util.ApplicationConfig;
import com.nexcorio.algo.util.KiteUtil;
import com.nexcorio.algo.util.TelegramChannelReader;

/**
 * 
 * @author Keshav Shetty
 *
 */
public class MainStrategyExecutor {

	private static final Logger log = LogManager.getLogger(MainStrategyExecutor.class);
	
	public static void main(String[] args) {
		
		log.info("Strategy executor -> Time starts now");
		KiteHelper kiteHelper = new KiteHelper();
		try {
			while  ( (new Date()).before(KiteUtil.getDailyCustomTime(9, 10, 5)) )  {
				System.out.println("Strategy executor -> Too early going to sleep for 30 seconds");
				Thread.sleep(30*1000);
			}
			System.out.println("Ready to fire");
			
			List<MainInstruments> mainInstruments = kiteHelper.getMainInstrumentsDto();
			for(MainInstruments mainInstrument : mainInstruments) {
				if (!mainInstrument.getShortName().equals("VIX")) { // Exclude VIX (Vix has no options
					new ATMMovementAnalyzerThreadAlgoThread(mainInstrument.getShortName(), null);
					new V2GreeksMovementAnalyzerThread(mainInstrument.getShortName(), null);
				}
			}
			
			Long userId = kiteHelper.getUserIdByZerodhaUserId(ApplicationConfig.getProperty("zerodha.user.id"));
			System.out.println("userId="+userId);
			new OrderExecutionThreadAlgoThread(userId); // Todo: For each user separate thread should start
			new G3NapAlgoTriggerThread();
			
			if (ApplicationConfig.getProperty("enable.telegram.integration").equals("true")) new TelegramChannelReader(); // Telegram integration 
			else System.out.println("Telegram Not enabled");
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Exception in main", e);
		}
		
		log.info("I am done, let the childern take care of themselves");
				
	}

}
