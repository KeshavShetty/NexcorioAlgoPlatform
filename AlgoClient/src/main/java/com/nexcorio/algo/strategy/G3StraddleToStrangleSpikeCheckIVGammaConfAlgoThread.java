package com.nexcorio.algo.strategy;

import java.util.Date;
import java.util.Map;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.util.KiteUtil;

public class G3StraddleToStrangleSpikeCheckIVGammaConfAlgoThread extends G3BaseClass implements Runnable {

	private static final Logger log = LogManager.getLogger(G3PriceParityIVBasedAlgoThread.class);

	public float startingDelta = 0.5f;
	public float deltaUpgradeStep = 0.05f;
	
	public float premiumSpikePercent = 5f;
	public float ivSpikePercent = 5f;
	public float gammaSpikePercent = 5f;
	
	public boolean startFromBaseDelta = false;
	
	public G3StraddleToStrangleSpikeCheckIVGammaConfAlgoThread(Long napAlgoId, String backTestDateStr) {
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
			
			float maxProfitReached = 0f;
			Date maxProfitReachedAt = getCurrentTime();
			float maxLowestpointReached = 0f;
			Date maxLowestpointReachedAt = getCurrentTime();
			float maxTrailingProfit = 0f;
			
			updateAlgoStatus("Running");
			
			Map<String, Float>  atmData = getATMStraddleData();
			
			float lowestceiv = atmData.get("ceiv");
			float highestceiv = lowestceiv;
			
			float lowestpeiv = atmData.get("peiv");
			float highestpeiv = lowestpeiv;
					
			float lowestcegamma = atmData.get("cegamma");
			float highestcegamma = lowestcegamma;
			
			float lowestpegamma = atmData.get("pegamma");
			float highestpegamma = lowestpegamma;
			
			float lowestatmPremium = atmData.get("atmPremium");
			float highestatmPremium = lowestatmPremium;
			
			float currentDelta = startingDelta + deltaUpgradeStep;
			do {
				sleep(15); // Quick to react
				
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
				fileLogTelegramWriter.write( " instrumentLtp=" + this.instrumentLtp +" ****** currentProfit="+currentProfitPerUnit+" ****** maxLowestpointReachedPerUnit="+(maxLowestpointReached)+" maxTrailingProfit="+maxTrailingProfit);
				
				Map<String, Float>  currentAtmData = getATMStraddleData();
				
				if (!ceStraddleOptionName.equals("")) { // Position exist, check for exit
//					if (currentAtmData.get("atmPremium") > lowestatmPremium*(100f + premiumSpikePercent)/100f
//							|| currentAtmData.get("ceiv") > lowestceiv*(100f + ivSpikePercent)/100f
//							|| currentAtmData.get("peiv") > lowestpeiv*(100f + ivSpikePercent)/100f
//							|| currentAtmData.get("cegamma") < highestcegamma*(100f - gammaSpikePercent)/100f
//							|| currentAtmData.get("pegamma") < highestpegamma*(100f - gammaSpikePercent)/100f) {
					if (currentAtmData.get("atmPremium") > lowestatmPremium*(100f + premiumSpikePercent)/100f
							&& (currentAtmData.get("ceiv") > lowestceiv*(100f + ivSpikePercent)/100f
							|| currentAtmData.get("peiv") > lowestpeiv*(100f + ivSpikePercent)/100f)) {
						
						fileLogTelegramWriter.write( " Exiting running straddle="+ceStraddleOptionName +" and " + peStraddleOptionName);
						if (this.placeActualOrder) {
							placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						updateCurrentOrderStatus(ceStraddleOptionName, ceDbId, "LegClosed");
						updateCurrentOrderStatus(peStraddleOptionName, peDbId, "LegClosed");
						ceStraddleOptionName = "";
						peStraddleOptionName = "";
						
						lowestceiv = currentAtmData.get("ceiv");
						highestceiv = lowestceiv;
						
						lowestpeiv = currentAtmData.get("peiv");
						highestpeiv = lowestpeiv;
								
						lowestcegamma = currentAtmData.get("cegamma");
						highestcegamma = lowestcegamma;
						
						lowestpegamma = currentAtmData.get("pegamma");
						highestpegamma = lowestpegamma;
						
						lowestatmPremium = currentAtmData.get("atmPremium");
						highestatmPremium = lowestatmPremium;
						
						if (this.noOfOrders >= maxAllowedNoOfOrders) {
							prepareExit("Too many orders");
						}
					}
				} else { // Doesn't exist, check for entry
					if (currentAtmData.get("atmPremium") < highestatmPremium*(100f - premiumSpikePercent)/100f) {
						if (currentAtmData.get("ceiv") < highestceiv*(100f - ivSpikePercent)/100f
								&& currentAtmData.get("peiv") < highestpeiv*(100f - ivSpikePercent)/100f) {
							if (currentAtmData.get("cegamma") > lowestcegamma*(100f + gammaSpikePercent)/100f
									&& currentAtmData.get("pegamma") > lowestpegamma*(100f + gammaSpikePercent)/100f) {
								currentDelta = currentDelta - deltaUpgradeStep;
								if (startFromBaseDelta) {
									currentDelta = startingDelta + deltaUpgradeStep;	
								}
								if (currentDelta >= 0.25f ) {
									String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised( currentDelta, this.hedgeDistance);
									
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
									
									lowestceiv = currentAtmData.get("ceiv");
									highestceiv = lowestceiv;
									
									lowestpeiv = currentAtmData.get("peiv");
									highestpeiv = lowestpeiv;
											
									lowestcegamma = currentAtmData.get("cegamma");
									highestcegamma = lowestcegamma;
									
									lowestpegamma = currentAtmData.get("pegamma");
									highestpegamma = lowestpegamma;
									
									lowestatmPremium = currentAtmData.get("atmPremium");
									highestatmPremium = lowestatmPremium;
								} else {
									prepareExit(" Reached lowerDelta level");
								}
							}
						}
					}
				}
				
				if (!ceStraddleOptionName.equals("")) { // Position exist, check for realignment
					ceOptionGreeks = !ceStraddleOptionName.equals("")?getOptionGreeks(ceStraddleOptionName):null;
					peOptionGreeks = !peStraddleOptionName.equals("")?getOptionGreeks(peStraddleOptionName):null;
					
					if ( Math.abs( ceOptionGreeks.getDelta()+peOptionGreeks.getDelta()) > 2*deltaUpgradeStep ) {
						fileLogTelegramWriter.write( " Delta gap widens, Exiting running straddle="+ceStraddleOptionName +" and " + peStraddleOptionName);
						if (this.placeActualOrder) {
							placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						updateCurrentOrderStatus(ceStraddleOptionName, ceDbId, "LegClosed");
						updateCurrentOrderStatus(peStraddleOptionName, peDbId, "LegClosed");
						ceStraddleOptionName = "";
						peStraddleOptionName = "";
						
						if (currentDelta >= 0.25f && this.noOfOrders < maxAllowedNoOfOrders) {
							currentDelta = currentDelta - deltaUpgradeStep;						
							String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised( currentDelta, this.hedgeDistance);
							
							ceStraddleOptionName =  entryStraddleOptionNames[0];
							peStraddleOptionName =  entryStraddleOptionNames[1];
							
							ceOptionGreeks = !ceStraddleOptionName.equals("")?getOptionGreeks(ceStraddleOptionName):null;
							peOptionGreeks = !peStraddleOptionName.equals("")?getOptionGreeks(peStraddleOptionName):null;
							print(ceOptionGreeks, peOptionGreeks);
							
							String logString = "Forming straddleceStraddleOptionName="+ceStraddleOptionName + "(@" + ceOptionGreeks.getLtp() +") ceHedgeOptionName="+ceHedgeOptionName+" " + peStraddleOptionName + "(@" + peOptionGreeks.getLtp() +") peHedgeOptionName="+peHedgeOptionName; 
							fileLogTelegramWriter.write( " "+logString);
							
							ceDbId = createAlgoSellOrder(ceStraddleOptionName, ceOptionGreeks.getLtp(), noOfLots*lotSize);
							peDbId = createAlgoSellOrder(peStraddleOptionName, peOptionGreeks.getLtp(), noOfLots*lotSize);
							
							if (this.placeActualOrder) { // Place the straddle order with Kite
								placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
								placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
						} else {
							prepareExit(" Too many positions");
						}
					}
				}
				
				if (currentAtmData.get("ceiv") < lowestceiv) lowestceiv = currentAtmData.get("ceiv");
				if (currentAtmData.get("ceiv") > highestceiv) highestceiv = currentAtmData.get("ceiv");
				
				if (currentAtmData.get("peiv") < lowestpeiv) lowestpeiv = currentAtmData.get("peiv");
				if (currentAtmData.get("peiv") > highestpeiv) highestpeiv = currentAtmData.get("peiv");
				
				if (currentAtmData.get("cegamma") < lowestcegamma) lowestcegamma = currentAtmData.get("cegamma");
				if (currentAtmData.get("cegamma") > highestcegamma) highestcegamma = currentAtmData.get("cegamma");
				
				if (currentAtmData.get("pegamma") < lowestpegamma) lowestpegamma = currentAtmData.get("pegamma");
				if (currentAtmData.get("pegamma") > highestpegamma) highestpegamma = currentAtmData.get("pegamma");
				
				if (currentAtmData.get("atmPremium") < lowestatmPremium) lowestatmPremium = currentAtmData.get("atmPremium");
				if (currentAtmData.get("atmPremium") > highestatmPremium) highestatmPremium = currentAtmData.get("atmPremium");
				
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
