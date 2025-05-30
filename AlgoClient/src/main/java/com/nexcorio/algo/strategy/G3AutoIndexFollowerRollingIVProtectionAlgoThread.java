package com.nexcorio.algo.strategy;

import java.util.Date;
import java.util.Map;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.util.KiteUtil;

public class G3AutoIndexFollowerRollingIVProtectionAlgoThread extends G3BaseClass implements Runnable{

	private static final Logger log = LogManager.getLogger(G3PriceParityIVBasedAlgoThread.class);
	
	public float baseDelta = 0.5f;
	public float indexPoints = 25f;
	
	public float deltaBias = 0.1f;
	
	public G3AutoIndexFollowerRollingIVProtectionAlgoThread(Long napAlgoId, String backTestDateStr) {
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
			
			Map<String, Float> ivMap = getCurrentATMIV();
			
			float referenceCeIv = ivMap.get("CE");
			float referencePeIv = ivMap.get("PE");
			
			this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
			
			fileLogTelegramWriter.write( " this.instrumentLtp="+this.instrumentLtp +" referenceCeIv="+referenceCeIv+" referencePeIv=" + referencePeIv);
			
			printFields(this);
			
			float maxProfitReached = 0f;
			Date maxProfitReachedAt = getCurrentTime();
			float maxLowestpointReached = 0f;
			Date maxLowestpointReachedAt = getCurrentTime();
			float maxTrailingProfit = 0f;
			
			updateAlgoStatus("Running");
			
			sleep(60*10); // 5 minutes
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
				
				ivMap = getCurrentATMIV();
				
				boolean isIVSafeForTrade = false;
				
				fileLogTelegramWriter.write( " current CE IV="+ivMap.get("CE")+" current PE IV=" + ivMap.get("PE") + " ce IV diff=" +(referenceCeIv - ivMap.get("CE"))+ " pe IV diff=" +(referencePeIv - ivMap.get("PE")) );
				
				if (referenceCeIv > (ivMap.get("CE") - 1f) && referencePeIv > (ivMap.get("PE") - 1f) ) {
					isIVSafeForTrade = true;	
				}
				
				if (ceStraddleOptionName.equals("")) { // No open position
					if ( referenceCeIv > ivMap.get("CE")+0.5f && referencePeIv > ivMap.get("PE")+0.5f ) {
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
						
						indexWhenStraddleFormed = this.instrumentLtp;
						
						fileLogTelegramWriter.write( "Forming indexWhenStraddleFormed="+indexWhenStraddleFormed);
					}
				} else { // Already positions running, check for exit rule
					if (isIVSafeForTrade == false ) { 
						fileLogTelegramWriter.write( " Exiting running straddle="+ceStraddleOptionName +" and " + peStraddleOptionName);
						if (this.placeActualOrder) {
							placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						updateCurrentOrderStatus(ceStraddleOptionName, ceDbId, "LegClosed");
						updateCurrentOrderStatus(peStraddleOptionName, peDbId, "LegClosed");
						ceStraddleOptionName = "";
						peStraddleOptionName = "";
						
						if (this.noOfOrders >= maxAllowedNoOfOrders) {
							prepareExit("Too many orders");
						}
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
							
							String logString = "ReForming straddleceStraddleOptionName="+ceStraddleOptionName + "(@" + ceOptionGreeks.getLtp() +") ceHedgeOptionName="+ceHedgeOptionName+" " + peStraddleOptionName + "(@" + peOptionGreeks.getLtp() +") peHedgeOptionName="+peHedgeOptionName; 
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
