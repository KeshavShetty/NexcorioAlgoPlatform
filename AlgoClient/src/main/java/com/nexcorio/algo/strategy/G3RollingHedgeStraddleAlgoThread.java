package com.nexcorio.algo.strategy;

import java.util.Date;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.util.KiteUtil;

public class G3RollingHedgeStraddleAlgoThread extends G3BaseClass implements Runnable{

	private static final Logger log = LogManager.getLogger(G3RollingHedgeStraddleAlgoThread.class);
	
	public float baseDelta = 0.6f;
	public float indexPoints = 50f;
	
	public int initialHedgeDistance = 400;
	
	public G3RollingHedgeStraddleAlgoThread(Long napAlgoId, String backTestDateStr) {
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
			
			String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised( baseDelta, 200);
			
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
			
			String originalCEHedgeOptionName = ceHedgeOptionName;  
			String originalPEHedgeOptionName = peHedgeOptionName;
			
			float pullCEHedgeAt = this.instrumentLtp + indexPoints;
			float pushCEHedgeAt = this.instrumentLtp - indexPoints;
			
			float pullPEHedgeAt = this.instrumentLtp - indexPoints;
			float pushPEHedgeAt = this.instrumentLtp + indexPoints;
			
			int currentCEHedgeDistance = 200;
			int currentPEHedgeDistance = 200;
			
			do {
				sleep(5); // Every 10sec 
				
				this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
				
				OptionGreek ceOptionGreeks = getOptionGreeks(ceStraddleOptionName);
				OptionGreek peOptionGreeks = getOptionGreeks(peStraddleOptionName);
				print(ceOptionGreeks, peOptionGreeks);
				
				fileLogTelegramWriter.write( " this.indexLtp="+this.instrumentLtp + ", pullCEHedgeAt="+pullCEHedgeAt+" pushCEHedgeAt=" + pushCEHedgeAt + " pullPEHedgeAt="+pullPEHedgeAt+" pushPEHedgeAt=" + pushPEHedgeAt +" currentCEHedgeDistance="+currentCEHedgeDistance+" currentPEHedgeDistance="+currentPEHedgeDistance);
				
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
				
				if (this.instrumentLtp > pullCEHedgeAt && currentCEHedgeDistance > 50) {
					// Exit existing CE hedge and bring hedge to half distance
					// Set new ce pull@  and push
					if (this.noOfOrders<maxAllowedNoOfOrders) {
						currentCEHedgeDistance = currentCEHedgeDistance/2;
						String newHedgenanme =  optionnamePrefix + (centerStrike+currentCEHedgeDistance) + "CE";
						ceHedgeDbId = createAlgoBuyOrder(newHedgenanme, getPriceFromTicks(newHedgenanme), noOfLots*lotSize);
						
						fileLogTelegramWriter.write( "Adjusting CE Hedge, Bring near to " +newHedgenanme);  
						
						if (this.placeActualOrder) {
							placeRealOrder(ceHedgeDbId, newHedgenanme, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE); // Buy new
							placeRealOrder(ceHedgeOptionName, noOfLots*lotSize, "SELL", true, KiteUtil.USE_NORMAL_ORDER_FALSE); // Buy new
						}
						ceHedgeOptionName = newHedgenanme;
						//getHedges(originalCEHedgeOptionName, currentCEHedgeDistance);
						// Get new hedges
						// Enter new
						// Exit old -- To maintain margin requirements issues
						pullCEHedgeAt = this.instrumentLtp + indexPoints;
						pushCEHedgeAt = this.instrumentLtp - indexPoints;
					} else {
						prepareExit("Overflow future contracts");
					}
				}
				if (this.instrumentLtp < pushCEHedgeAt && currentCEHedgeDistance < initialHedgeDistance) {
					if (this.noOfOrders<maxAllowedNoOfOrders) {
						currentCEHedgeDistance = currentCEHedgeDistance*2;
						String newHedgenanme =  optionnamePrefix + (centerStrike+currentCEHedgeDistance) + "CE";					
						ceHedgeDbId = createAlgoBuyOrder(newHedgenanme, getPriceFromTicks(newHedgenanme), noOfLots*lotSize);
						
						fileLogTelegramWriter.write( "Adjusting CE Hedge, Push further to " +newHedgenanme);  
						
						if (this.placeActualOrder) {
							placeRealOrder(ceHedgeDbId, newHedgenanme, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE); // Buy new
							placeRealOrder(ceHedgeOptionName, noOfLots*lotSize, "SELL", true, KiteUtil.USE_NORMAL_ORDER_FALSE); // Buy new
						}
						ceHedgeOptionName = newHedgenanme;
						
						// Get new hedges
						// Enter new
						// Exit old -- To maintain margin requirements issues
						pushCEHedgeAt = this.instrumentLtp - indexPoints;
						pullCEHedgeAt = this.instrumentLtp + indexPoints;
					} else {
						prepareExit("Overflow future contracts");
					}
				}
				if (this.instrumentLtp < pullPEHedgeAt && currentPEHedgeDistance > 50) {
					// Exit existing CE hedge and bring hedge to half distance
					// Set new ce pull At  and push At
					if (this.noOfOrders<maxAllowedNoOfOrders) {
						currentPEHedgeDistance = currentPEHedgeDistance/2;
						String newHedgenanme =  optionnamePrefix + (centerStrike-currentPEHedgeDistance) + "PE";
						
						peHedgeDbId = createAlgoBuyOrder(newHedgenanme, getPriceFromTicks(newHedgenanme), noOfLots*lotSize);
						
						fileLogTelegramWriter.write( "Adjusting PE Hedge, Bring near to " +newHedgenanme);
						
						if (this.placeActualOrder) {
							placeRealOrder(peHedgeDbId, newHedgenanme, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE); // Buy new
							placeRealOrder(peHedgeOptionName, noOfLots*lotSize, "SELL", true, KiteUtil.USE_NORMAL_ORDER_FALSE); // Buy new
						}
						peHedgeOptionName = newHedgenanme;
						
						pullPEHedgeAt = this.instrumentLtp - indexPoints;
						pushPEHedgeAt = this.instrumentLtp + indexPoints;
					} else {
						prepareExit("Overflow future contracts");
					}
				}
				if (this.instrumentLtp > pushPEHedgeAt && currentPEHedgeDistance < initialHedgeDistance) {
					if (this.noOfOrders<maxAllowedNoOfOrders) {
						currentCEHedgeDistance = currentCEHedgeDistance*2;
						String newHedgenanme =  optionnamePrefix + (centerStrike-currentPEHedgeDistance) + "PE";
						
						peHedgeDbId = createAlgoBuyOrder(newHedgenanme, getPriceFromTicks(newHedgenanme), noOfLots*lotSize);
						
						fileLogTelegramWriter.write( "Adjusting PE Hedge, Push further to " +newHedgenanme);
						
						if (this.placeActualOrder) {
							placeRealOrder(peHedgeDbId, newHedgenanme, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE); // Buy new
							placeRealOrder(peHedgeOptionName, noOfLots*lotSize, "SELL", true, KiteUtil.USE_NORMAL_ORDER_FALSE); // Buy new
						}
						peHedgeOptionName = newHedgenanme;
						// Get new hedges
						// Enter new
						// Exit old -- To maintain margin requirements issues
						pushPEHedgeAt = this.instrumentLtp + indexPoints;
						pullPEHedgeAt = this.instrumentLtp - indexPoints;
					} else {
						prepareExit("Overflow future contracts");
					}
				}
				if (ceOptionGreeks.getLtp()<2.5f || peOptionGreeks.getLtp()<2.5f) { // || Math.abs(ceOptionGreeks.getDelta())<0.1f || Math.abs(peOptionGreeks.getDelta())<0.1f || Math.abs(ceOptionGreeks.getDelta())>0.9f || Math.abs(peOptionGreeks.getDelta())>0.9f) {
					if (!ceStraddleOptionName.equals("")) {
						placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
						placeRealOrder( peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
						placeRealOrder( ceHedgeOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						placeRealOrder( peHedgeOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
					}
					ceStraddleOptionName = "";
					peStraddleOptionName="";
					ceHedgeOptionName="";
					peHedgeOptionName="";
					
					
					entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised( baseDelta, 400);
					
					if (getPriceFromTicks(entryStraddleOptionNames[0]) > 25f) { // Resetting
						
						this.noOfOrders = 0;
						
						centerStrike = getOptionCenterStrike(optionnamePrefix);
						
						ceStraddleOptionName =  entryStraddleOptionNames[0];
						peStraddleOptionName =  entryStraddleOptionNames[1];
						
						ceHedgeOptionName =  entryStraddleOptionNames[2];			
						peHedgeOptionName =  entryStraddleOptionNames[3];
						
						cePrice = getPriceFromTicks(ceStraddleOptionName);
						pePrice = getPriceFromTicks(peStraddleOptionName);
						
						fileLogTelegramWriter.write( "Re-Entering ="+ceStraddleOptionName +"(@"+cePrice+")" + peStraddleOptionName +"(@"+pePrice+")");
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
						
						originalCEHedgeOptionName = ceHedgeOptionName;  
						originalPEHedgeOptionName = peHedgeOptionName;
						
						pullCEHedgeAt = this.instrumentLtp + indexPoints;
						pushCEHedgeAt = this.instrumentLtp - indexPoints;
						
						pullPEHedgeAt = this.instrumentLtp - indexPoints;
						pushPEHedgeAt = this.instrumentLtp + indexPoints;
						
						currentCEHedgeDistance = initialHedgeDistance;
						currentPEHedgeDistance = initialHedgeDistance;
					
						
					} else {
						prepareExit("Nothing much left");
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
	
	public static void main(String[] args) {
		
	}
}
