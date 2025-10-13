package com.nexcorio.algo.strategy;

import java.util.Date;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.util.KiteUtil;

public class G4RollingGreekAlgoThread extends G3BaseClass implements Runnable {

	private static final Logger log = LogManager.getLogger(G3PriceParityIVBasedAlgoThread.class);

	public float baseDelta = 0.5f;
	public String greekname = "delta";
	public float greekDiffValue = 0f;
	
	public boolean wait4IdealPremium = Boolean.FALSE;
	
	private float idealPremium = 0f;
	
	public G4RollingGreekAlgoThread(Long napAlgoId, String backTestDateStr) {
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
					else if (idealPremium > 100f + currentPremium) foundIdealpremium = true; // Check for last day expiry
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
			
			
			
			String[] entryStraddleOptionNames = getStraddleOptionNamesByGreekOptimisedNew(this.greekname,  this.baseDelta, this.optimalHedgeDistance);
			
			ceStraddleOptionName =  entryStraddleOptionNames[0];
			peStraddleOptionName =  entryStraddleOptionNames[1];
			
			OptionGreek ceOptionGreeks = getOptionGreeks(ceStraddleOptionName);
			OptionGreek peOptionGreeks = getOptionGreeks(peStraddleOptionName);
			print(ceOptionGreeks, peOptionGreeks);
			
			String logString = "Forming straddleceStraddleOptionName="+ceStraddleOptionName + "(@" + ceOptionGreeks.getLtp() +") ceHedgeOptionName="+ceHedgeOptionName+" " + peStraddleOptionName + "(@" + peOptionGreeks.getLtp() +") peHedgeOptionName="+peHedgeOptionName; 
			fileLogTelegramWriter.write( " "+logString);
			
			ceDbId = createAlgoSellOrder(ceStraddleOptionName, ceOptionGreeks.getLtp(), noOfLots*lotSize);
			peDbId = createAlgoSellOrder(peStraddleOptionName, peOptionGreeks.getLtp(), noOfLots*lotSize);
			
			if (this.placeActualOrder) { // Place the straddle order with Kite
				ceHedgeOptionName =  entryStraddleOptionNames[2];
				peHedgeOptionName =  entryStraddleOptionNames[3];
				placeRealOrder(ceHedgeOptionName, noOfLots*lotSize, "BUY",  false, KiteUtil.USE_NORMAL_ORDER_FALSE);
				placeRealOrder(peHedgeOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);	
			 
				placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
				placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
			}
			
			float maxProfitReached = 0f;
			Date maxProfitReachedAt = getCurrentTime();
			float maxLowestpointReached = 0f;
			Date maxLowestpointReachedAt = getCurrentTime();
			float maxTrailingProfit = 0f;
			
			updateAlgoStatus("Running");
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
				
				float currentGreekDiff = getPercentDiff(getGreekValue(greekname, ceOptionGreeks), getGreekValue(greekname, peOptionGreeks));
				fileLogTelegramWriter.write( " instrumentLtp=" + this.instrumentLtp +" ****** currentProfit="+currentProfitPerUnit+" currentGreekDiff="+(currentGreekDiff));
				
				if (currentGreekDiff > greekDiffValue) {
					entryStraddleOptionNames = getStraddleOptionNamesByGreekOptimisedNew(this.greekname, this.baseDelta, this.optimalHedgeDistance);
					
					String ceOptionname = entryStraddleOptionNames[0];
					
					if (!ceStraddleOptionName.equals(ceOptionname)) {
						if (!ceStraddleOptionName.equals("")) { // Exit and re enter
							fileLogTelegramWriter.write( " Exiting ="+ceStraddleOptionName );
							// Exit CE
							if (this.placeActualOrder) {
								placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
							ceStraddleOptionName = "";
						}
						if (this.noOfOrders-ignoredOrders<maxAllowedNoOfOrders) {
							ceStraddleOptionName =  ceOptionname;
							float cePrice = getPriceFromTicks(ceStraddleOptionName);
							fileLogTelegramWriter.write( " Entering ="+ceStraddleOptionName +"(@"+cePrice+")");
							// Place order
							ceDbId = createAlgoSellOrder(ceStraddleOptionName, cePrice, noOfLots*lotSize);
							if (this.placeActualOrder) {
								if (ceHedgeOptionName.equals("")) {								
									ceHedgeOptionName =  entryStraddleOptionNames[2];
									placeRealOrder(ceHedgeOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
								}
								placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
						} else {
							prepareExit("Too many orders");
						}
					} else {
						fileLogTelegramWriter.write( " Retaining ="+ceStraddleOptionName);
					}
					
					String peOptionname = entryStraddleOptionNames[1];
					if (!peStraddleOptionName.equals(peOptionname)) {
						if (!peStraddleOptionName.equals("")) { // Exit and re enter
							fileLogTelegramWriter.write( " Exiting ="+peStraddleOptionName );
							if (this.placeActualOrder) {
								placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
							peStraddleOptionName = "";
						}
						if (this.noOfOrders-ignoredOrders<maxAllowedNoOfOrders) {
							peStraddleOptionName =  peOptionname;
							float pePrice = getPriceFromTicks(peStraddleOptionName);
							fileLogTelegramWriter.write( "Entering ="+peStraddleOptionName +"(@"+pePrice+")");
							// Place order
							peDbId = createAlgoSellOrder(peStraddleOptionName, pePrice, noOfLots*lotSize);
							if (this.placeActualOrder) {
								if (peHedgeOptionName.equals("")) {
									peHedgeOptionName =  entryStraddleOptionNames[3];
									placeRealOrder(peHedgeOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
								}
								placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
						} else {
							prepareExit("Too many orders");
						}
					} else {
						fileLogTelegramWriter.write( " Retaining ="+peStraddleOptionName);
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
