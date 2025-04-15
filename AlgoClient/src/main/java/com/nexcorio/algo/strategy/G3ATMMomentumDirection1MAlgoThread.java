package com.nexcorio.algo.strategy;

import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.util.KiteUtil;

public class G3ATMMomentumDirection1MAlgoThread extends G3BaseClass implements Runnable{

	private static final Logger log = LogManager.getLogger(G3ATMMomentumDirection1MAlgoThread.class);
	
	private Thread threadRef = null;
		
	public float baseDelta = 0.5f;
	
	public G3ATMMomentumDirection1MAlgoThread(Long napAlgoId, String backTestDateStr) {
		super(napAlgoId);
		initializeParameters(backTestDateStr);
		
		fileLogTelegramWriter.write(this.algoname);
		Thread t = new Thread(this, this.mainInstrument.getShortName()+this.algoname);
		threadRef = t;
		t.setPriority(Thread.MAX_PRIORITY);
		t.start();
	}
	
	@Override
	public void run() {
		log.info("IntrdayShortStraddleAlgoIndexSpotPriceBasedAdjustmentThread-" +this.mainInstrument.getShortName()+this.algoname+" reached run");
		System.out.println("placeActualOrder="+this.placeActualOrder);
		try {
			
			long ceDbId = -1;
			long peDbId = -1;
			
			float maxProfitReached = 0f;
			Date maxProfitReachedAt = getCurrentTime();
			float maxLowestpointReached = 0f;
			Date maxLowestpointReachedAt = getCurrentTime();
			float maxTrailingProfit = 0f;
			
			updateAlgoStatus("Running");
			
			String lastKnownTrend = "Unknown";
			
			float indexPointsCaptured =0f;
			float indexAtSignal = 0f;
			
			this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
			
			String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(baseDelta, 500);
			
			//printAllStradleOptions();
			
			ceStraddleOptionName =  entryStraddleOptionNames[0];
			peStraddleOptionName =  entryStraddleOptionNames[1];
			
			float cePrice = getPriceFromTicks(ceStraddleOptionName);
			float pePrice = getPriceFromTicks(peStraddleOptionName);
			
			fileLogTelegramWriter.write( " Forming ="+ceStraddleOptionName +"(@"+cePrice+") "+peStraddleOptionName +"(@"+pePrice+")");
			// Place order
			ceDbId = createAlgoSellOrder(ceStraddleOptionName, cePrice, noOfLots*lotSize);
			peDbId = createAlgoSellOrder(peStraddleOptionName, pePrice, noOfLots*lotSize);
			
			do {
				sleep(30); // Every 10sec
				fileLogTelegramWriter.write("====================================================================================================");
				this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
				
				OptionGreek ceOptionGreeks = !ceStraddleOptionName.equals("")?getOptionGreeks(ceStraddleOptionName):null;
				OptionGreek peOptionGreeks = !peStraddleOptionName.equals("")?getOptionGreeks(peStraddleOptionName):null;
				print(ceOptionGreeks, peOptionGreeks);
				
				float runningCePrice = ceOptionGreeks==null?0: ceOptionGreeks.getLtp();
				float runningPePrice = peOptionGreeks==null?0: peOptionGreeks.getLtp();
				
				if (!ceStraddleOptionName.equals("")) updateCurrentOrderBuyPrice(ceStraddleOptionName, ceDbId, runningCePrice);
				if (!peStraddleOptionName.equals("")) updateCurrentOrderBuyPrice(peStraddleOptionName, peDbId, runningPePrice);
				
				currentProfitPerUnit = getProfitFromDB();
				if (currentProfitPerUnit>maxProfitReached) {
					maxProfitReached=currentProfitPerUnit;
					maxProfitReachedAt = getCurrentTime();
				}
				if (currentProfitPerUnit<maxLowestpointReached) {
					maxLowestpointReached=currentProfitPerUnit;
					maxLowestpointReachedAt = getCurrentTime();
				}
				trailingProfit = (currentProfitPerUnit-maxProfitReached);
				if (trailingProfit<maxTrailingProfit) {
					maxTrailingProfit = trailingProfit;
				}
				fileLogTelegramWriter.write( " IndexLtp=" + this.instrumentLtp +" [[ currentProfit="+currentProfitPerUnit+" ]] maxLowestpointReachedPerUnit="+(maxLowestpointReached/lotSize)+" maxTrailingProfit="+maxTrailingProfit);
				
				boolean reAlignmentRequired = checkDeltaGammaEffectForRealignment(ceStraddleOptionName, peStraddleOptionName);

				if (reAlignmentRequired == true) {
					entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(baseDelta, 500);
					
					ceStraddleOptionName =  entryStraddleOptionNames[0];
					peStraddleOptionName =  entryStraddleOptionNames[1];
					
					cePrice = getPriceFromTicks(ceStraddleOptionName);
					pePrice = getPriceFromTicks(peStraddleOptionName);
					
					fileLogTelegramWriter.write( " Forming ="+ceStraddleOptionName +"(@"+cePrice+") "+peStraddleOptionName +"(@"+pePrice+")");
					// Place order
					ceDbId = createAlgoSellOrder(ceStraddleOptionName, cePrice, noOfLots*lotSize);
					peDbId = createAlgoSellOrder(peStraddleOptionName, pePrice, noOfLots*lotSize);
				}
				
				checkExitSignals();
				
				if (exitThread==true) {
					if (lastKnownTrend.equals("CE")) {
						indexPointsCaptured = indexPointsCaptured + indexAtSignal - this.instrumentLtp;
					} else if (lastKnownTrend.equals("PE")) {
						indexPointsCaptured = indexPointsCaptured + this.instrumentLtp - indexAtSignal;
					}					
				}	
				saveAlgoDailySummary(currentProfitPerUnit, maxProfitReached, maxProfitReachedAt, maxLowestpointReached, maxLowestpointReachedAt, maxTrailingProfit);
			} while(!exitThread);
			updateAlgoStatus("Terminated");
			String logString = "Exiting Strddle ceStraddleOptionName="+ceStraddleOptionName + " peStraddleOptionName="+peStraddleOptionName; 
			log.info(logString);
			fileLogTelegramWriter.write( " " + logString);
			// exit all positions
			if (this.placeActualOrder) exitStraddle(ceDbId, peDbId);
			fileLogTelegramWriter.write( " noOfOrders="+noOfOrders +" indexPointsCaptured=" + indexPointsCaptured+ " ROI=" + (currentProfitPerUnit*this.lotSize*100f)/requiredMargin + "% (Max profit/lot reached to "+ (maxProfitReached) +"@" + maxProfitReachedAt+ "\n and Lowest reached to " + (maxLowestpointReached) + "@" + maxLowestpointReachedAt + ")");
		
			log.info("================= Done. Exiting IndexSpotPriceBasedIntrdayShortStraddleAlgoThread " + this.mainInstrument.getShortName() + this.algoname +" =================");
			
		} catch (Exception e) {			
			updateAlgoStatus("Error");
			log.error("Error"+e.getMessage(), e);
			fileLogTelegramWriter.write("Error " + ExceptionUtils.getStackTrace(e));
		} finally {
			fileLogTelegramWriter.close();
		}
	}
	
	private void printAllStradleOptions() {
		fileLogTelegramWriter.write("---------------------------------Delta optimised---------------------------------");
		String[] optionNames = getStraddleOptionNamesByGreekOptimised("delta", 0.4f, 0);
		print(getOptionGreeks(optionNames[0]), getOptionGreeks(optionNames[1]));
		
		fileLogTelegramWriter.write("---------------------------------ltp optimised---------------------------------");
		optionNames = getStraddleOptionNamesByGreekOptimised("ltp", 0.4f, 0);
		print(getOptionGreeks(optionNames[0]), getOptionGreeks(optionNames[1]));
		
		fileLogTelegramWriter.write("---------------------------------Gamma optimised---------------------------------");
		optionNames = getStraddleOptionNamesByGreekOptimised("gamma", 0.4f, 0);
		print(getOptionGreeks(optionNames[0]), getOptionGreeks(optionNames[1]));
		
		fileLogTelegramWriter.write("---------------------------------IV optimised---------------------------------");
		optionNames = getStraddleOptionNamesByGreekOptimised("iv", 0.4f, 0);
		print(getOptionGreeks(optionNames[0]), getOptionGreeks(optionNames[1]));
	}
	
	private boolean checkDeltaGammaEffectForRealignment(String ceOptionName, String peOptionName) {
		
		boolean retval = false;
		fileLogTelegramWriter.write("In checkDeltaGammaEffect");
		
		//String[] entryStraddleOptions = getStraddleOptionNamesByGreekNeutralWithOptimalDiff(this.mainInstrument.getShortName(), "delta", this.indexLtp, baseDelta, backtestDate);
		
		OptionGreek ceOptionGreeks = getOptionGreeks(ceOptionName);
		OptionGreek peOptionGreeks = getOptionGreeks(peOptionName);
		print(ceOptionGreeks, peOptionGreeks);
		
		//if (Math.abs(Math.abs(ceOptionGreeks.getDelta()) - Math.abs(peOptionGreeks.getDelta())) < 0.04) {
			int underlyingMovement = 25;
			int noOfIntegration = 5;
			
			int intervals = underlyingMovement/noOfIntegration;
			
			float currentCeDeta = ceOptionGreeks.getDelta();
			float currentPeDeta = peOptionGreeks.getDelta();
			
			float currentCePrice = ceOptionGreeks.getLtp();
			float currentPePrice = peOptionGreeks.getLtp();
			fileLogTelegramWriter.write("Starting currentCePrice="+currentCePrice+"(Delta: " + currentCeDeta + "), " + " currentPePrice="+currentPePrice+"(Delta: " + currentPeDeta + ")");
			
			for(int i=1;i<=noOfIntegration; i++) {
				
				currentCePrice = currentCePrice + currentCeDeta*(i*intervals); // Change in Premium = Delta * change in spot  i.e  0.3 * 70 = 21
				currentPePrice = currentPePrice + currentPeDeta*(i*intervals);
				
				currentCeDeta = currentCeDeta + ceOptionGreeks.getGamma()*(i*intervals);
				currentPeDeta = currentPeDeta + peOptionGreeks.getGamma()*(i*intervals);
				
				fileLogTelegramWriter.write("i=" + i + ". currentCePrice="+currentCePrice+"(Delta: " + currentCeDeta + "), " + " currentPePrice="+currentPePrice+"(Delta: " + currentPeDeta + ")");		
			}
			
			float cePriceSellersGain = ceOptionGreeks.getLtp() - currentCePrice;
			float pePriceSellersGain = peOptionGreeks.getLtp() - currentPePrice;
			
			float raisingNet = (cePriceSellersGain+pePriceSellersGain);
			fileLogTelegramWriter.write(" After rise cePriceSellersGain="+cePriceSellersGain+" pePriceSellersGain="+pePriceSellersGain+" net=" + raisingNet);
			
			// Fall
			
			currentCeDeta = ceOptionGreeks.getDelta();
			currentPeDeta = peOptionGreeks.getDelta();
			
			currentCePrice = ceOptionGreeks.getLtp();
			currentPePrice = peOptionGreeks.getLtp();
			fileLogTelegramWriter.write("Starting currentCePrice="+currentCePrice+"(Delta: " + currentCeDeta + "), " + " currentPePrice="+currentPePrice+"(Delta: " + currentPeDeta + ")");
			
			for(int i=1;i<=noOfIntegration; i++) {
				
				currentCePrice = currentCePrice + currentCeDeta*(-i*intervals); // Change in Premium = Delta * change in spot  i.e  0.3 * 70 = 21
				currentPePrice = currentPePrice + currentPeDeta*(-i*intervals);
				
				currentCeDeta = currentCeDeta + ceOptionGreeks.getGamma()*(-i*intervals);
				currentPeDeta = currentPeDeta + peOptionGreeks.getGamma()*(-i*intervals);
				
				fileLogTelegramWriter.write("i=" + i + ". currentCePrice="+currentCePrice+"(Delta: " + currentCeDeta + "), " + " currentPePrice="+currentPePrice+"(Delta: " + currentPeDeta + ")");		
			}
			
			cePriceSellersGain = ceOptionGreeks.getLtp() - currentCePrice;
			pePriceSellersGain = peOptionGreeks.getLtp() - currentPePrice;
			
			float fallingNet =  (cePriceSellersGain+pePriceSellersGain);
			fileLogTelegramWriter.write(" After fall cePriceSellersGain="+cePriceSellersGain+" pePriceSellersGain="+pePriceSellersGain+" net=" + fallingNet);
			
//			if ((fallingNet > 20f && raisingNet < -20f)
//					|| (fallingNet < -20f && raisingNet > 20f) ) {
//				retval = true;
//			}
			if ((fallingNet < -20f || raisingNet < -20f) ) {
				retval = true;
			}
		//}
		return retval;
	}
	
	public boolean isAlive() {
		boolean retVal = false;
		try {
			retVal = threadRef.isAlive();
		} catch(Exception ex) {
			ex.printStackTrace();
		}
		return retVal;
	}
	
	public void exitThread() {
		prepareExit("Companion asked to quit");	
	}
	
	
	public void print(List<Integer> strikes) {
		for(Integer aInt: strikes) {
			fileLogTelegramWriter.write(aInt+"");	
		}
	}
	
	
	public static void main(String[] args) {
		String dateStr = "2025-03-24 09:21:00";
		new G3ATMMomentumDirection1MAlgoThread(521L, "2025-03-24 09:21:00"); // TopOIs 6
		
		//new G3TopOIIVBasedDirection1MAlgoThread(422L, dateStr); // TopOIs 6
		//new G3TopOIIVBasedDirection1MAlgoThread(423L, dateStr); // TopOIs 6
	}

	

}
