package com.nexcorio.algo.strategy;

import java.util.Date;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.util.KiteUtil;

public class G3PriceSensitiveStrangleAlgoThread extends G3BaseClass implements Runnable{

	private static final Logger log = LogManager.getLogger(G3PriceParityIVBasedAlgoThread.class);
	
	public float baseDelta = 0.5f;
	public String greekname = "ltp";
	
	public float diffValue = 0f;
	
	public float combinedDiff = 0f;
	
	public boolean useMinGreek = false;
	
	public G3PriceSensitiveStrangleAlgoThread(Long napAlgoId, String backTestDateStr) {
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
			
			float ceGreekWhenStraddleFormed = 0f;
			float peGreekWhenStraddleFormed = 0f;
			
			float minCeGreekReached = 0f;
			float minPeGreekReached = 0f;
			
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
				
				boolean needRepositioning = false;
				
				if (ceStraddleOptionName.equals("")) {
					needRepositioning = true; // Just starting, no open positions
				} 
				if (needRepositioning == false && useMinGreek == true ) {
					float ceGreekValue = ceOptionGreeks.getLtp();
					float peGreekValue = peOptionGreeks.getLtp();
					if (greekname.equals("iv")) {
						ceGreekValue = ceOptionGreeks.getIv();
						peGreekValue = peOptionGreeks.getIv();
					}
					
					if (ceGreekValue - minCeGreekReached > diffValue || peGreekValue - minPeGreekReached > diffValue ) {
						fileLogTelegramWriter.write("Realigning diffValue"); 
						needRepositioning = true;
					}	
				}
				if (needRepositioning == false && diffValue > 0f ) {
					if (ceOptionGreeks.getLtp() - ceGreekWhenStraddleFormed > diffValue || peOptionGreeks.getLtp() - peGreekWhenStraddleFormed > diffValue ) {
						fileLogTelegramWriter.write("Realigning diffValue"); 
						needRepositioning = true;
					}
				}
				if (needRepositioning == false && combinedDiff > 0f ) {
					float totalDiff = Math.abs( ceOptionGreeks.getLtp() - peOptionGreeks.getLtp() );
					if (totalDiff > combinedDiff ) {
						fileLogTelegramWriter.write("Realigning combinedDiff"); 
						needRepositioning = true;
					}
				}
				if (needRepositioning) {
					String[] entryStraddleOptionNames = getStraddleOptionNamesByGreekOptimised("ltp", baseDelta, this.optimalHedgeDistance); // getStraddleOptionNamesByGreekOptimised("ltp", this.baseDelta, this.optimalHedgeDistance);
					
					String ceOptionname = entryStraddleOptionNames[0];
					
					float ceGreekValue = 0f;
					if (!ceStraddleOptionName.equals(ceOptionname)) {
						if (!ceStraddleOptionName.equals("")) { // Exit and re enter
							fileLogTelegramWriter.write( " Exiting ="+ceStraddleOptionName );
							// Exit CE
							if (this.placeActualOrder) {
								placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
							ceStraddleOptionName = "";
						}
						if (this.noOfOrders<maxAllowedNoOfOrders) {
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
						if (this.noOfOrders<maxAllowedNoOfOrders) {
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
					
					ceOptionGreeks = !ceStraddleOptionName.equals("")?getOptionGreeks(ceStraddleOptionName):null;
					peOptionGreeks = !peStraddleOptionName.equals("")?getOptionGreeks(peStraddleOptionName):null;
					
					if (ceOptionGreeks != null) {
						if (greekname.equals("iv")) ceGreekWhenStraddleFormed = ceOptionGreeks.getIv();
						else if (greekname.equals("ltp")) ceGreekWhenStraddleFormed = ceOptionGreeks.getLtp();
					}
					if (peOptionGreeks != null) {
						if (greekname.equals("iv")) peGreekWhenStraddleFormed = peOptionGreeks.getIv();
						else if (greekname.equals("ltp")) peGreekWhenStraddleFormed = peOptionGreeks.getLtp();
					}
					
					minCeGreekReached = ceGreekWhenStraddleFormed;
					minPeGreekReached = peGreekWhenStraddleFormed;
					
				} else {
					if (greekname.equals("iv")) {
						if (ceOptionGreeks.getIv() < minCeGreekReached) {
							minCeGreekReached = ceOptionGreeks.getIv();
						}
						if (peOptionGreeks.getIv() < minPeGreekReached) {
							minPeGreekReached = peOptionGreeks.getIv();
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
