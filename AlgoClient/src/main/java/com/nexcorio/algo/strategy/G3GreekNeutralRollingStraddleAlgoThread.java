package com.nexcorio.algo.strategy;

import java.util.Date;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.util.KiteUtil;

public class G3GreekNeutralRollingStraddleAlgoThread extends G3BaseClass implements Runnable{

	private static final Logger log = LogManager.getLogger(G3GreekNeutralRollingStraddleAlgoThread.class);
			
	public float acceptableDiff = 0.1f;	
	public float baseDelta = 0.6f;
	public String greekname = "delta";
	
	public G3GreekNeutralRollingStraddleAlgoThread(Long napAlgoId, String backTestDateStr) {
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
			
			String[] entryStraddleOptionNames = getStraddleOptionNamesByGreekOptimised(greekname, 0.5f, 500);
			
			ceStraddleOptionName =  entryStraddleOptionNames[0];
			ceHedgeOptionName =  entryStraddleOptionNames[2];
			
			peStraddleOptionName =  entryStraddleOptionNames[1];
			peHedgeOptionName =  entryStraddleOptionNames[3];
				
				
			float cePrice = getPriceFromTicks(ceStraddleOptionName);
			float pePrice = getPriceFromTicks(peStraddleOptionName);
			
			String logString = "Forming straddleceStraddleOptionName="+ceStraddleOptionName + "(@" + cePrice +") ceHedgeOptionName="+ceHedgeOptionName+" " + peStraddleOptionName + "(@" + pePrice +") peHedgeOptionName="+peHedgeOptionName; 
			fileLogTelegramWriter.write( " "+logString);
			
			ceDbId = createAlgoSellOrder(ceStraddleOptionName, cePrice, noOfLots*lotSize);
			peDbId = createAlgoSellOrder(peStraddleOptionName, pePrice, noOfLots*lotSize);
			
			if (this.placeActualOrder) { // Place the straddle order with Kite
				placeRealOrder( ceHedgeOptionName, noOfLots*lotSize, "BUY",  true, KiteUtil.USE_NORMAL_ORDER_FALSE);
				placeRealOrder( ceDbId, ceStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
				placeRealOrder( peHedgeOptionName, noOfLots*lotSize, "BUY",  true, KiteUtil.USE_NORMAL_ORDER_FALSE);
				placeRealOrder( peDbId , peStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
			}
						
			float maxProfitReached = 0f;
			Date maxProfitReachedAt = getCurrentTime();
			float maxLowestpointReached = 0f;
			Date maxLowestpointReachedAt = getCurrentTime();
			float maxTrailingProfit = 0f;
			
			updateAlgoStatus("Running");
			
			do {
				sleep(10); // Quick to react
				
				this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
				
				OptionGreek ceOptionGreeks = !ceStraddleOptionName.equals("")?getOptionGreeks(ceStraddleOptionName):null;
				OptionGreek peOptionGreeks = !peStraddleOptionName.equals("")?getOptionGreeks(peStraddleOptionName):null;
				print(ceOptionGreeks, peOptionGreeks);
				
				float runningCePrice = ceOptionGreeks==null?0: ceOptionGreeks.getLtp();
				float runningPePrice = peOptionGreeks==null?0: peOptionGreeks.getLtp();
				
				if (!ceStraddleOptionName.equals("")) updateCurrentOrderBuyPrice(ceStraddleOptionName, ceDbId, runningCePrice);
				if (!peStraddleOptionName.equals("")) updateCurrentOrderBuyPrice(peStraddleOptionName, peDbId, runningPePrice);
				
				float ceDelta = ceOptionGreeks!=null?ceOptionGreeks.getDelta():0f;
				float peDelta = peOptionGreeks!=null?peOptionGreeks.getDelta():0f;
				
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
				fileLogTelegramWriter.write( " IndexLtp=" + this.instrumentLtp +" currentProfit="+currentProfitPerUnit+" maxLowestpointReachedPerUnit="+(maxLowestpointReached/lotSize)+" maxTrailingProfit="+maxTrailingProfit);
				
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
				} else if (greekname.equals("iv")) {
					greekSum = Math.abs(ceOptionGreeks.getIv()) + Math.abs(peOptionGreeks.getIv());
					greekDiff = Math.abs( Math.abs(ceOptionGreeks.getIv()) - Math.abs(peOptionGreeks.getIv()) );
				} else if (greekname.equals("vega")) {					
					greekSum = Math.abs(ceOptionGreeks.getVega()) + Math.abs(peOptionGreeks.getVega());
					greekDiff = Math.abs( Math.abs(ceOptionGreeks.getVega()) - Math.abs(peOptionGreeks.getVega()) );
				} else if (greekname.equals("delta/gamma")) {					
					greekSum = Math.abs(ceOptionGreeks.getDelta()/ceOptionGreeks.getGamma()) + Math.abs(peOptionGreeks.getDelta()/peOptionGreeks.getGamma());
					greekDiff = Math.abs( Math.abs(ceOptionGreeks.getDelta()/ceOptionGreeks.getGamma()) - Math.abs(peOptionGreeks.getDelta()/peOptionGreeks.getGamma()) );
				}
				
				float thetaDiffRatio = greekDiff/greekSum;
				fileLogTelegramWriter.write( "GreekDiffRatio="+thetaDiffRatio);
				
				boolean needAlignment = false;
				
				if (thetaDiffRatio > this.acceptableDiff) {
					needAlignment = true;
				}
				
				if (needAlignment) {
					fileLogTelegramWriter.write( "Realignment required (Orders so far " + this.noOfOrders + ")");
					if (this.noOfOrders<maxAllowedNoOfOrders) {
						fileLogTelegramWriter.write( " Exiting running straddle="+ceStraddleOptionName +" and " + peStraddleOptionName);
						// Exit PE
						if (this.placeActualOrder) {
							placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						updateCurrentOrderStatus(ceStraddleOptionName, ceDbId, "LegClosed");
						updateCurrentOrderStatus(peStraddleOptionName, peDbId, "LegClosed");
						
						ceStraddleOptionName = "";
						peStraddleOptionName = "";
						
						entryStraddleOptionNames = getStraddleOptionNamesByGreekOptimised(greekname, 0.5f, 500);
						
						if (entryStraddleOptionNames!=null) {
							ceStraddleOptionName =  entryStraddleOptionNames[0];					
							peStraddleOptionName =  entryStraddleOptionNames[1];
								
							cePrice = getPriceFromTicks(ceStraddleOptionName );
							pePrice = getPriceFromTicks(peStraddleOptionName );
							
							logString = "Forming straddleceStraddleOptionName="+ceStraddleOptionName + "(@" + cePrice +") "+peStraddleOptionName + "(@" + pePrice +")"; 
							fileLogTelegramWriter.write( " "+logString);
							
							ceDbId = createAlgoSellOrder(ceStraddleOptionName, cePrice, noOfLots*lotSize);
							peDbId = createAlgoSellOrder(peStraddleOptionName, pePrice, noOfLots*lotSize);
							
							if (this.placeActualOrder) { // Place the straddle order with Kite
								placeRealOrder( ceStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
								placeRealOrder( peStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
						} else {
							prepareExit("Matching greek value not found");
						}
					} else {
						prepareExit("Need alignment, But too many orders");
					}
				}
				checkExitSignals();
				
				if ( (runningCePrice+runningPePrice)>0 && (runningCePrice+runningPePrice)<10f ) {
					prepareExit( "Nothing much left in premium");
				}
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
	
	
	public static void main(String[] args) {
		//new G3OIWorthSellerDirectionAlgoThread(108L, null);
		new G3GreekNeutralRollingStraddleAlgoThread(275L, null);
	}

	

}
