package com.nexcorio.algo.strategy;

import java.util.Date;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.util.KiteUtil;

public class G3SpikeManagedBuyAndAutoIndexFollowerAlgoThread extends G3BaseClass implements Runnable{

	private static final Logger log = LogManager.getLogger(G3PriceParityIVBasedAlgoThread.class);
	
	public float baseDelta = 0.5f;
	public float premiumSpikePercent = 8f;
	public float indexPoints = 50f;
	
	public float deltaBias = 0.1f;
	
	String ceBuyOptionname = "";
	String peBuyOptionname = "";
	
	public G3SpikeManagedBuyAndAutoIndexFollowerAlgoThread(Long napAlgoId, String backTestDateStr) {
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
			
			long buyCeDbId = -1;
			long buyPeDbId = -1;
						
			this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
			
			fileLogTelegramWriter.write( " this.instrumentLtp="+this.instrumentLtp);
			
			printFields(this);
			
			float maxProfitReached = 0f;
			Date maxProfitReachedAt = getCurrentTime();
			float maxLowestpointReached = 0f;
			Date maxLowestpointReachedAt = getCurrentTime();
			float maxTrailingProfit = 0f;
			
			updateAlgoStatus("Running");
			
			float lowestATMStraddlePremium = getATMStraddlePremium();
			float highestATMStraddlePremium = lowestATMStraddlePremium;
			
			float indexWhenStraddleFormed = 0f;
			do {
				sleep(5); // Quick to react
				
				this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
				
				OptionGreek ceOptionGreeks = !ceStraddleOptionName.equals("")?getOptionGreeks(ceStraddleOptionName):null;
				OptionGreek peOptionGreeks = !peStraddleOptionName.equals("")?getOptionGreeks(peStraddleOptionName):null;
				print(ceOptionGreeks, peOptionGreeks);
				
				float runningCePrice = ceOptionGreeks==null?0: ceOptionGreeks.getLtp();
				float runningPePrice = peOptionGreeks==null?0: peOptionGreeks.getLtp();
				
				if (!ceStraddleOptionName.equals("")) updateCurrentOrderBuyPrice(ceStraddleOptionName, ceDbId, runningCePrice);
				if (!peStraddleOptionName.equals("")) updateCurrentOrderBuyPrice(peStraddleOptionName, peDbId, runningPePrice);
				
				if (!ceBuyOptionname.equals("")) updateCurrentOrderSellPrice(ceBuyOptionname, buyCeDbId, getPriceFromTicks(ceBuyOptionname));
				if (!peBuyOptionname.equals("")) updateCurrentOrderSellPrice(peBuyOptionname, buyPeDbId, getPriceFromTicks(peBuyOptionname));
				
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
				fileLogTelegramWriter.write( " instrumentLtp=" + this.instrumentLtp +" currentProfit="+currentProfitPerUnit+" maxLowestpointReachedPerUnit="+(maxLowestpointReached)+" maxTrailingProfit="+maxTrailingProfit);
				
				fileLogTelegramWriter.write("lowestATMStraddlePremium="+ lowestATMStraddlePremium+" highestATMStraddlePremium="+highestATMStraddlePremium+" Entry at "
						+ (highestATMStraddlePremium*(100f - premiumSpikePercent)/100f) + " Exit at " + ( lowestATMStraddlePremium*(100f + premiumSpikePercent)/100f) );  
				
				float currentATMStraddlePremium = getATMStraddlePremium();
				
				if (ceStraddleOptionName.equals("")) { // No Short position
					if (currentATMStraddlePremium < highestATMStraddlePremium*(100f - premiumSpikePercent)/100f) {
						
						// Exit Long position 
						if (!ceBuyOptionname.equals("")) {
							if (this.placeActualOrder) {
								placeRealOrder(ceBuyOptionname, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);	
							}
							ceBuyOptionname = "";	
						}
						if (!peBuyOptionname.equals("")) {
							if (this.placeActualOrder) {
								placeRealOrder(peBuyOptionname, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);	
							}
							peBuyOptionname = "";	
						}
						
						// Enter Short positions
						String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(baseDelta, this.hedgeDistance);
						
						ceStraddleOptionName =  entryStraddleOptionNames[0];
						peStraddleOptionName =  entryStraddleOptionNames[1];
						
						ceOptionGreeks = !ceStraddleOptionName.equals("")?getOptionGreeks(ceStraddleOptionName):null;
						peOptionGreeks = !peStraddleOptionName.equals("")?getOptionGreeks(peStraddleOptionName):null;
						print(ceOptionGreeks, peOptionGreeks);
						
						String logString = "Forming straddleceStraddleOptionName="+ceStraddleOptionName + "(@" + ceOptionGreeks.getLtp() +") ceHedgeOptionName="+ceHedgeOptionName+" " + peStraddleOptionName + "(@" + peOptionGreeks.getLtp() +") peHedgeOptionName="+peHedgeOptionName; 
						fileLogTelegramWriter.write( " "+logString);
						
						ceDbId = createAlgoSellOrder(ceStraddleOptionName, ceOptionGreeks.getLtp(), noOfLots*lotSize);
						peDbId = createAlgoSellOrder(peStraddleOptionName, peOptionGreeks.getLtp(), noOfLots*lotSize);
						
						if (ceHedgeOptionName.equals("")) {
							ceHedgeOptionName =  entryStraddleOptionNames[2];
							if (this.placeActualOrder) {
								placeRealOrder(ceHedgeOptionName, noOfLots*lotSize, "BUY",  false, KiteUtil.USE_NORMAL_ORDER_FALSE);	
							}
						}
						if (peHedgeOptionName.equals("")) {
							peHedgeOptionName =  entryStraddleOptionNames[3];
							if (this.placeActualOrder) {
								placeRealOrder(peHedgeOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);	
							}
						}
						
						if (this.placeActualOrder) { // Place the straddle order with Kite
							placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						
						highestATMStraddlePremium = currentATMStraddlePremium;
						lowestATMStraddlePremium  = currentATMStraddlePremium;
						indexWhenStraddleFormed = this.instrumentLtp;
						
						fileLogTelegramWriter.write( "Forming indexWhenStraddleFormed="+indexWhenStraddleFormed);
					} else if (ceBuyOptionname.equals("")) { // Long also empty
						if (currentATMStraddlePremium > lowestATMStraddlePremium*(100f + premiumSpikePercent)/100f ) {
							String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(baseDelta, this.hedgeDistance);
							ceBuyOptionname = entryStraddleOptionNames[0];
							peBuyOptionname = entryStraddleOptionNames[1];
							
							float cePrice = getPriceFromTicks(ceBuyOptionname);
							float pePrice = getPriceFromTicks(peBuyOptionname);
							
							buyCeDbId = createAlgoBuyOrder(ceBuyOptionname, cePrice, noOfLots*lotSize);
							buyPeDbId = createAlgoBuyOrder(peBuyOptionname, pePrice, noOfLots*lotSize);
							if (this.placeActualOrder) {
								placeRealOrder(buyCeDbId, ceBuyOptionname, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
								placeRealOrder(buyPeDbId, peBuyOptionname, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
							highestATMStraddlePremium = currentATMStraddlePremium;
							lowestATMStraddlePremium  = currentATMStraddlePremium;
						}
					}
				} else if (!ceStraddleOptionName.equals("")) { // Already short positions running, check for exit rule
					if (currentATMStraddlePremium > lowestATMStraddlePremium*(100f + premiumSpikePercent)/100f
							) { // && currentATMStraddlePremium > atmPremiumWhenStraddleFormed
						fileLogTelegramWriter.write( " Exiting running straddle="+ceStraddleOptionName +" and " + peStraddleOptionName);
						if (this.placeActualOrder) {
							placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						updateCurrentOrderStatus(ceStraddleOptionName, ceDbId, "LegClosed");
						updateCurrentOrderStatus(peStraddleOptionName, peDbId, "LegClosed");
						ceStraddleOptionName = "";
						peStraddleOptionName = "";
						
						highestATMStraddlePremium = currentATMStraddlePremium;
						lowestATMStraddlePremium  = currentATMStraddlePremium;
						
						if (this.noOfOrders >= maxAllowedNoOfOrders) {
							prepareExit("Too many orders");
						} else { // Enter long 
							String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(baseDelta, this.hedgeDistance);
							ceBuyOptionname = entryStraddleOptionNames[0];
							peBuyOptionname = entryStraddleOptionNames[1];
							
							float cePrice = getPriceFromTicks(ceBuyOptionname);
							float pePrice = getPriceFromTicks(peBuyOptionname);
							
							buyCeDbId = createAlgoBuyOrder(ceBuyOptionname, cePrice, noOfLots*lotSize);
							buyPeDbId = createAlgoBuyOrder(peBuyOptionname, pePrice, noOfLots*lotSize);
							if (this.placeActualOrder) {
								placeRealOrder(buyCeDbId, ceBuyOptionname, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
								placeRealOrder(buyPeDbId, peBuyOptionname, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
						}
						highestATMStraddlePremium = currentATMStraddlePremium;
						lowestATMStraddlePremium  = currentATMStraddlePremium;
					} else if (this.instrumentLtp > indexWhenStraddleFormed + indexPoints || this.instrumentLtp < indexWhenStraddleFormed - indexPoints) {						
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
							
							String[] entryStraddleOptionNames1 = getStraddleOptionNamesByDeltaOptimised( baseDelta+deltaBias, 0);
							String[] entryStraddleOptionNames2 = getStraddleOptionNamesByDeltaOptimised( baseDelta-deltaBias, 0);
							
							if (this.instrumentLtp > indexWhenStraddleFormed + indexPoints) {
								ceStraddleOptionName =  entryStraddleOptionNames2[0];
								peStraddleOptionName =  entryStraddleOptionNames1[1];
							} else {
								ceStraddleOptionName =  entryStraddleOptionNames1[0];
								peStraddleOptionName =  entryStraddleOptionNames2[1];
							}
							
							ceOptionGreeks = getOptionGreeks(ceStraddleOptionName);
							peOptionGreeks = getOptionGreeks(peStraddleOptionName);
							print(ceOptionGreeks, peOptionGreeks);
							
							String logString = "Forming straddleceStraddleOptionName="+ceStraddleOptionName + "(@" + ceOptionGreeks.getLtp() +") ceHedgeOptionName="+ceHedgeOptionName+" " + peStraddleOptionName + "(@" + peOptionGreeks.getLtp() +") peHedgeOptionName="+peHedgeOptionName; 
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
				}
				
				if (currentATMStraddlePremium > highestATMStraddlePremium) highestATMStraddlePremium = currentATMStraddlePremium;
				if (currentATMStraddlePremium < lowestATMStraddlePremium)  lowestATMStraddlePremium  = currentATMStraddlePremium;
				
				if ( (runningCePrice+runningPePrice)>0 && (runningCePrice+runningPePrice)<10f ) {
					prepareExit( "Nothing much left in premium");
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
			String logString = "Exiting Strddle ceStraddleOptionName="+ceStraddleOptionName + " peStraddleOptionName="+peStraddleOptionName; 
			log.info(logString);
			fileLogTelegramWriter.write( " " + logString);
			// exit all positions
			if (this.placeActualOrder) {
				exitStraddle(ceDbId, peDbId);
				if (!ceBuyOptionname.equals("")) placeRealOrder( ceBuyOptionname, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
				if (!peBuyOptionname.equals("")) placeRealOrder( peBuyOptionname, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
			}
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
