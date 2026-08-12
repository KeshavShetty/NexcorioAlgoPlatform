package com.nexcorio.algo.strategy;

import java.util.Date;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.util.KiteUtil;

public class G3AutoIndexFollowerRollingGreekAlgoThread extends G3BaseClass implements Runnable {

	private static final Logger log = LogManager.getLogger(G3PriceParityIVBasedAlgoThread.class);

	public float baseDelta = 0.5f;
	public float indexPoints = 35f; 
	
	public float deltaBias = 0.1f;
	public boolean bullNeutral = false;
	public boolean wait4IdealPremium = Boolean.FALSE;
	
	private float idealPremium = 0f;
	
	public float upIndexPoints = -1f;
	public float dnIndexPoints = -1f;
	
	public G3AutoIndexFollowerRollingGreekAlgoThread(Long napAlgoId, String backTestDateStr) {
		super(napAlgoId);
		initializeParameters(backTestDateStr);
		
		fileLogTelegramWriter.write(this.algoname);
		Thread t = new Thread(this, this.mainInstrument.getShortName()+this.algoname);
		t.setPriority(Thread.MAX_PRIORITY);
		t.start();
	}
	
	@Override
	public void run() {
		
		try {
			long ceDbId = -1;
			long peDbId = -1;
						
			this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
			
			fileLogTelegramWriter.write( " this.instrumentLtp="+this.instrumentLtp);
			
			printFields(this);
			
			boolean isitFirstAttempt = true;
			if (this.wait4IdealPremium) {
				
				boolean foundIdealpremium = false;
				do {
					float currentPremium = getStradlePremium();
					if (idealPremium==0f) {
						this.idealPremium = getIdealPremiumBasedOnPreviousStradlePremium();
					}
					if (currentPremium >= idealPremium) foundIdealpremium = true; 
					else {
						fileLogTelegramWriter.write("currentPremium="+currentPremium+" idealStraddlePremium="+idealPremium+" sleep");
						sleep(10);
						checkExitSignals();
						isitFirstAttempt = false;
					}
				} while(foundIdealpremium==false && exitThread==false);
				if (isitFirstAttempt==false) sleep(120);
				
			}
			
			if (exitThread==true) {
				return;
			}
			
			
			float maxProfitReached = 0f;
			Date maxProfitReachedAt = getCurrentTime();
			float maxLowestpointReached = 0f;
			Date maxLowestpointReachedAt = getCurrentTime();
			float maxTrailingProfit = 0f;
			
			updateAlgoStatus("Running");
			
			if (upIndexPoints < 0f ) upIndexPoints = indexPoints;
			if (dnIndexPoints < 0f ) dnIndexPoints = indexPoints;
			
			fileLogTelegramWriter.write( "Set upIndexPoints=" + upIndexPoints + " dnIndexPoints="+dnIndexPoints);
			
			String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(this.baseDelta, this.optimalHedgeDistance);
			
			ceStraddleOptionName =  entryStraddleOptionNames[0];
			peStraddleOptionName =  entryStraddleOptionNames[1];
			
			OptionGreek ceOptionGreeks = getOptionGreeks(ceStraddleOptionName);
			OptionGreek peOptionGreeks = getOptionGreeks(peStraddleOptionName);
			print(ceOptionGreeks, peOptionGreeks);
			
			String logString = "Forming straddleceStraddleOptionName="+ceStraddleOptionName + "(@" + ceOptionGreeks.getLtp() +") ceHedgeOptionName="+ceHedgeOptionName+" " + peStraddleOptionName + "(@" + peOptionGreeks.getLtp() +") peHedgeOptionName="+peHedgeOptionName; 
			fileLogTelegramWriter.write( " "+logString);
			
			ceDbId = createAlgoSellOrder(ceStraddleOptionName, ceOptionGreeks.getLtp(), noOfLots*lotSize);
			peDbId = createAlgoSellOrder(peStraddleOptionName, peOptionGreeks.getLtp(), noOfLots*lotSize);
			
			ceHedgeOptionName =  entryStraddleOptionNames[2];
			if (this.placeActualOrder) {
				placeRealOrder(ceHedgeOptionName, noOfLots*lotSize, "BUY",  false, KiteUtil.USE_NORMAL_ORDER_FALSE);	
			}
			peHedgeOptionName =  entryStraddleOptionNames[3];
			if (this.placeActualOrder) {
				placeRealOrder(peHedgeOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);	
			}
			
			if (this.placeActualOrder) { // Place the straddle order with Kite
				placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
				placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
			}
			
			float indexWhenStraddleFormed = this.instrumentLtp;
			
			do {
				sleep(5); // Quick to react
				
				this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
				
				ceOptionGreeks = !ceStraddleOptionName.equals("")?getOptionGreeks(ceStraddleOptionName):null;
				peOptionGreeks = !peStraddleOptionName.equals("")?getOptionGreeks(peStraddleOptionName):null;
				print(ceOptionGreeks, peOptionGreeks);
				
				float runningCePrice = ceOptionGreeks==null?0: ceOptionGreeks.getLtp();
				float runningPePrice = peOptionGreeks==null?0: peOptionGreeks.getLtp();
				
				if (!ceStraddleOptionName.equals("")) updateCurrentOrderBuyPrice(ceStraddleOptionName, ceDbId, runningCePrice);
				if (!peStraddleOptionName.equals("")) updateCurrentOrderBuyPrice(peStraddleOptionName, peDbId, runningPePrice);
				
				currentProfitPerUnit = getProfitFromDB();
				if (currentProfitPerUnit > maxProfitReached) {
					maxProfitReached=currentProfitPerUnit;
					maxProfitReachedAt = getCurrentTime();
				}
				if (currentProfitPerUnit < maxLowestpointReached) {
					maxLowestpointReached=currentProfitPerUnit;
					maxLowestpointReachedAt = getCurrentTime();
				}
				trailingProfit = (currentProfitPerUnit-maxProfitReached);
				if (trailingProfit<maxTrailingProfit) {
					maxTrailingProfit = trailingProfit;
				}
				fileLogTelegramWriter.write( " instrumentLtp=" + this.instrumentLtp +" ****** currentProfit="+currentProfitPerUnit+" ****** maxLowestpointReachedPerUnit="+(maxLowestpointReached)+" maxTrailingProfit="+maxTrailingProfit);
				
				if (this.instrumentLtp > indexWhenStraddleFormed + upIndexPoints || this.instrumentLtp < indexWhenStraddleFormed - dnIndexPoints) {
					
					// Exit existing function
					fileLogTelegramWriter.write( " Exiting running straddle="+ceStraddleOptionName +" and " + peStraddleOptionName);
					if (this.placeActualOrder) {
						placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
					}
					updateCurrentOrderStatus(ceStraddleOptionName, ceDbId, "LegClosed");
					updateCurrentOrderStatus(peStraddleOptionName, peDbId, "LegClosed");
					ceStraddleOptionName = "";
					peStraddleOptionName = "";
					
					if (this.noOfOrders < maxAllowedNoOfOrders) {
						
						float upDelat2Use = baseDelta+deltaBias;
						float dnDelat2Use = baseDelta-deltaBias;
						
						if (bullNeutral && this.instrumentLtp > indexWhenStraddleFormed + upIndexPoints) {
							upDelat2Use = baseDelta;
							dnDelat2Use = baseDelta;
						}
						
						String[] entryStraddleOptionNames1 = getStraddleOptionNamesByDeltaOptimised( upDelat2Use, 0);
						String[] entryStraddleOptionNames2 = getStraddleOptionNamesByDeltaOptimised( dnDelat2Use, 0);
						
						if (this.instrumentLtp > indexWhenStraddleFormed + upIndexPoints) {
							ceStraddleOptionName =  entryStraddleOptionNames2[0];
							peStraddleOptionName =  entryStraddleOptionNames1[1];
						} else {
							ceStraddleOptionName =  entryStraddleOptionNames1[0];
							peStraddleOptionName =  entryStraddleOptionNames2[1];
						}
						
						ceOptionGreeks = getOptionGreeks(ceStraddleOptionName);
						peOptionGreeks = getOptionGreeks(peStraddleOptionName);
						print(ceOptionGreeks, peOptionGreeks);
						
						logString = "Forming straddleceStraddleOptionName="+ceStraddleOptionName + "(@" + ceOptionGreeks.getLtp() +") ceHedgeOptionName="+ceHedgeOptionName+" " + peStraddleOptionName + "(@" + peOptionGreeks.getLtp() +") peHedgeOptionName="+peHedgeOptionName; 
						fileLogTelegramWriter.write( " "+logString);
						
						ceDbId = createAlgoSellOrder(ceStraddleOptionName, ceOptionGreeks.getLtp(), noOfLots*lotSize);
						peDbId = createAlgoSellOrder(peStraddleOptionName, peOptionGreeks.getLtp(), noOfLots*lotSize);
						
						if (this.placeActualOrder) { // Place the straddle order with Kite
							placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						indexWhenStraddleFormed = this.instrumentLtp;
					} else {
						prepareExit("Too many orders");
					}
				}
				
				checkExitSignals();
				
				if (exitThread==true) {
					if (!ceStraddleOptionName.equals("")) {
						updateCurrentOrderStatus(ceStraddleOptionName, ceDbId, "LegClosed");
					} 
					if (!peStraddleOptionName.equals("")) {
						updateCurrentOrderStatus(peStraddleOptionName, peDbId, "LegClosed");
					}
				}	
				saveAlgoDailySummary(currentProfitPerUnit, maxProfitReached, maxProfitReachedAt, maxLowestpointReached, maxLowestpointReachedAt, maxTrailingProfit);
			} while(!exitThread);
			updateAlgoStatus("Terminated");
			logString = "Exiting Strddle ceStraddleOptionName="+ceStraddleOptionName + " peStraddleOptionName="+peStraddleOptionName; 
			log.info(logString);
			fileLogTelegramWriter.write( " " + logString);
			// exit all positions
			if (this.placeActualOrder) exitStraddle(ceDbId, peDbId);
			fileLogTelegramWriter.write( " noOfOrders="+noOfOrders + " ROI=" + (currentProfitPerUnit*this.lotSize*100f)/requiredMargin + "% (Max profit/lot reached to "+ (maxProfitReached) +"@" + maxProfitReachedAt+ "\n and Lowest reached to " + (maxLowestpointReached) + "@" + maxLowestpointReachedAt + ")");
			
		} catch (Exception e) {			
			updateAlgoStatus("Error");
			log.error("Error"+e.getMessage(), e);
			fileLogTelegramWriter.write("Error " + ExceptionUtils.getStackTrace(e));
		} finally {
			fileLogTelegramWriter.close();
		}
	}
	
}
