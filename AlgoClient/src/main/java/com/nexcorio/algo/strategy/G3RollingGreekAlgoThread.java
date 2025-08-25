package com.nexcorio.algo.strategy;

import java.util.Date;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.util.KiteUtil;

public class G3RollingGreekAlgoThread extends G3BaseClass implements Runnable {

	private static final Logger log = LogManager.getLogger(G3PriceParityIVBasedAlgoThread.class);

	public float baseDelta = 0.5f;
	public String greekname = "delta";
	public float greekDiffPercent = 8f; // 8/100=0.08f
	
	public G3RollingGreekAlgoThread(Long napAlgoId, String backTestDateStr) {
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
			
			String[] entryStraddleOptionNames = getStraddleOptionNamesByGreekOptimised(this.greekname,  this.baseDelta, this.optimalHedgeDistance);
			
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
				
				float greekSum = 0f;
				float greekDiff = 0f;
				
				if (greekname.equals("delta")) {					
					greekSum = Math.abs(ceOptionGreeks.getDelta()) + Math.abs(peOptionGreeks.getDelta());
					greekDiff = Math.abs( Math.abs(ceOptionGreeks.getDelta()) - Math.abs(peOptionGreeks.getDelta()) );
				} else if (greekname.equals("ltp")) {
					float ceLtp = Math.abs(ceOptionGreeks.getLtp());
					float peLtp = Math.abs(peOptionGreeks.getLtp());
										
					greekSum = ceLtp + peLtp;
					greekDiff = Math.abs( ceLtp - peLtp );
				} else if (greekname.equals("timevalue")) {
					greekSum = Math.abs(getTimeValue(ceOptionGreeks.getTradingSymbol(), this.instrumentLtp, ceOptionGreeks.getLtp())) + Math.abs(getTimeValue(peOptionGreeks.getTradingSymbol(), this.instrumentLtp, peOptionGreeks.getLtp()));
					greekDiff = Math.abs( Math.abs(getTimeValue(ceOptionGreeks.getTradingSymbol(), this.instrumentLtp, ceOptionGreeks.getLtp())) - Math.abs(getTimeValue(peOptionGreeks.getTradingSymbol(), this.instrumentLtp, peOptionGreeks.getLtp())) );
				} else if (greekname.equals("impliedvolatility")) {
					greekSum = Math.abs(ceOptionGreeks.getIv()) + Math.abs(peOptionGreeks.getIv());
					greekDiff = Math.abs( Math.abs(ceOptionGreeks.getIv()) - Math.abs(peOptionGreeks.getIv()) );
				} else if (greekname.equals("vega")) {					
					greekSum = Math.abs(ceOptionGreeks.getVega()) + Math.abs(peOptionGreeks.getVega());
					greekDiff = Math.abs( Math.abs(ceOptionGreeks.getVega()) - Math.abs(peOptionGreeks.getVega()) );
				} else if (greekname.equals("theta")) {					
					greekSum = Math.abs(ceOptionGreeks.getTheta()) + Math.abs(peOptionGreeks.getTheta());
					greekDiff = Math.abs( Math.abs(ceOptionGreeks.getTheta()) - Math.abs(peOptionGreeks.getTheta()) );
				}
				
				float thetaDiffRatio = greekDiff*100f/greekSum;
				fileLogTelegramWriter.write( "GreekDiffPercent="+thetaDiffRatio);
				
				boolean needAlignment = false;
				
				if (thetaDiffRatio > this.greekDiffPercent) {
					needAlignment = true;
				}
				
				if (needAlignment) {
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
										
						entryStraddleOptionNames = getStraddleOptionNamesByGreekOptimised(this.greekname, baseDelta, this.optimalHedgeDistance);
						
						ceStraddleOptionName =  entryStraddleOptionNames[0];
						peStraddleOptionName =  entryStraddleOptionNames[1];
						
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
