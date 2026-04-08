package com.nexcorio.algo.strategy;

import java.util.Date;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.util.KiteUtil;

public class G3NearHedgeAutoIndexFollowerStraddleAlgoThread extends G3BaseClass implements Runnable{

	private static final Logger log = LogManager.getLogger(G3NearHedgeAutoIndexFollowerStraddleAlgoThread.class);
	
	public float baseDelta = 0.6f;
	public float indexPoints = 50f;
	
	public int initialHedgeDistance = 400;
	
	public G3NearHedgeAutoIndexFollowerStraddleAlgoThread(Long napAlgoId, String backTestDateStr) {
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
			
			long ceHedgeDbId = -1;
			long peHedgeDbId = -1;
			
			float maxProfitReached = 0f;
			Date maxProfitReachedAt = getCurrentTime();
			float maxLowestpointReached = 0f;
			Date maxLowestpointReachedAt = getCurrentTime();
			float maxTrailingProfit = 0f;
			
			this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
			
			fileLogTelegramWriter.write( " this.instrumentLtp="+this.instrumentLtp);
			
			printFields(this);
			
			updateAlgoStatus("Running");
			
			String[] entryStraddleOptionNames = null;
			
			float deltaDiff = 1f;
			do { // Wait till get delta near exact baseDelta
				this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
				entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised( baseDelta, initialHedgeDistance);
				OptionGreek ceOptionGreek = getOptionGreeks(entryStraddleOptionNames[0]);
				OptionGreek peOptionGreek = getOptionGreeks(entryStraddleOptionNames[1]);
				deltaDiff = Math.abs(ceOptionGreek.getDelta() - Math.abs(peOptionGreek.getDelta()));
				if (deltaDiff>0.02f) {
					sleep(5);
				}
			} while(deltaDiff>0.02f);
			
			String optionnamePrefix = getCurrentWeekExpiryOptionnamePrefix();
			int centerStrike = getOptionCenterStrike(optionnamePrefix);
			
			ceStraddleOptionName =  entryStraddleOptionNames[0];
			peStraddleOptionName =  entryStraddleOptionNames[1];
			
			ceHedgeOptionName =  entryStraddleOptionNames[2];			
			peHedgeOptionName =  entryStraddleOptionNames[3];
			
			float cePrice = getPriceFromTicks(ceStraddleOptionName);
			float pePrice = getPriceFromTicks(peStraddleOptionName);
			
			fileLogTelegramWriter.write( " Entering ="+ceStraddleOptionName +"(@"+cePrice+")" + peStraddleOptionName +"(@"+pePrice+")");
			// Place order
			ceDbId = createAlgoSellOrder(ceStraddleOptionName, cePrice, noOfLots*lotSize);
			peDbId = createAlgoSellOrder(peStraddleOptionName, pePrice, noOfLots*lotSize);
			
			ceHedgeDbId = createAlgoBuyOrder(ceHedgeOptionName, getPriceFromTicks(ceHedgeOptionName), noOfLots*lotSize);
			peHedgeDbId = createAlgoBuyOrder(peHedgeOptionName, getPriceFromTicks(peHedgeOptionName), noOfLots*lotSize);
			
			if (this.placeActualOrder) {
				placeRealOrder(ceHedgeDbId, ceHedgeOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
				placeRealOrder(peHedgeDbId, peHedgeOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
				placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
				placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
			}
			
			float indexWhenStraddleFormed = this.instrumentLtp;
			
			do {
				sleep(5); // Every 10sec 
				
				this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
				
				OptionGreek ceOptionGreeks = getOptionGreeks(ceStraddleOptionName);
				OptionGreek peOptionGreeks = getOptionGreeks(peStraddleOptionName);
				print(ceOptionGreeks, peOptionGreeks);
				
				OptionGreek ceHedgeOptionGreeks = getOptionGreeks(ceHedgeOptionName);
				OptionGreek peHedgeOptionGreeks = getOptionGreeks(peHedgeOptionName);
				print(ceHedgeOptionGreeks, peHedgeOptionGreeks);
				
				updateCurrentOrderBuyPrice(ceStraddleOptionName, ceDbId, ceOptionGreeks.getLtp());
				updateCurrentOrderBuyPrice(peStraddleOptionName, peDbId, peOptionGreeks.getLtp());
				
				updateCurrentOrderSellPrice(ceHedgeOptionName, ceHedgeDbId, ceHedgeOptionGreeks.getLtp());
				updateCurrentOrderSellPrice(peHedgeOptionName, peHedgeDbId, peHedgeOptionGreeks.getLtp());
				
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
				if (trailingProfit < maxTrailingProfit) {
					maxTrailingProfit = trailingProfit;
				}
				fileLogTelegramWriter.write( " instrumentLtp=" + this.instrumentLtp +" currentProfit="+currentProfitPerUnit+" maxLowestpointReachedPerUnit="+(maxLowestpointReached)+" maxTrailingProfit="+maxTrailingProfit);
				
				if (this.instrumentLtp > indexWhenStraddleFormed + indexPoints
						|| this.instrumentLtp < indexWhenStraddleFormed - indexPoints) {
					
					if (this.placeActualOrder && !ceStraddleOptionName.equals("")) {
						placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
						placeRealOrder( peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
					}
					ceStraddleOptionName = "";
					peStraddleOptionName="";
					if (this.noOfOrders<maxAllowedNoOfOrders) {
						entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised( baseDelta, initialHedgeDistance);
						
						ceStraddleOptionName =  entryStraddleOptionNames[0];
						peStraddleOptionName =  entryStraddleOptionNames[1];
						
						fileLogTelegramWriter.write( " Entering ="+ceStraddleOptionName +"(@"+getPriceFromTicks(ceStraddleOptionName)+")" + peStraddleOptionName +"(@"+getPriceFromTicks(peStraddleOptionName)+")");
						// Place order
						ceDbId = createAlgoSellOrder(ceStraddleOptionName, getPriceFromTicks(ceStraddleOptionName), noOfLots*lotSize);
						peDbId = createAlgoSellOrder(peStraddleOptionName, getPriceFromTicks(peStraddleOptionName), noOfLots*lotSize);
											
						if (this.placeActualOrder) {
							placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						indexWhenStraddleFormed = this.instrumentLtp;
					} else {
						prepareExit("Too many orders");
					}
				}
				
				checkExitSignals();
				
				saveAlgoDailySummary(currentProfitPerUnit, maxProfitReached, maxProfitReachedAt, maxLowestpointReached, maxLowestpointReachedAt, maxTrailingProfit);
			} while(!exitThread);
			updateAlgoStatus("Terminated");
			String logString = "Exiting Strddle ceStraddleOptionName="+ceStraddleOptionName + " peStraddleOptionName="+peStraddleOptionName; 
			log.info(logString);
			fileLogTelegramWriter.write( " " + logString);
			// exit all positions
			if (this.placeActualOrder) exitStraddle(ceDbId, peDbId);
			fileLogTelegramWriter.write( " noOfOrders="+noOfOrders + " ROI=" + (currentProfitPerUnit*this.lotSize*100f)/requiredMargin + "% (Max profit reached to "+ (maxProfitReached) +"@" + maxProfitReachedAt+ "\n and Lowest reached to " + (maxLowestpointReached) + "@" + maxLowestpointReachedAt + ")");
			
		} catch (Exception e) {			
			updateAlgoStatus("Error");
			log.error("Error"+e.getMessage(), e);
			fileLogTelegramWriter.write("Error " + ExceptionUtils.getStackTrace(e));
		} finally {
			fileLogTelegramWriter.close();
		}
	}
}
