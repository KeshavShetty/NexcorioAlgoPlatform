package com.nexcorio.algo.strategy;

import java.util.Date;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.util.KiteUtil;

public class G3PriceParityIVBasedAlgoThread extends G3BaseClass implements Runnable{

	private static final Logger log = LogManager.getLogger(G3PriceParityIVBasedAlgoThread.class);
	
	public float baseDelta = 0.5f;
	
	public G3PriceParityIVBasedAlgoThread(Long napAlgoId, String backTestDateStr) {
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
			if (this.placeActualOrder) setLotBasedonAvailableMarginHalfStraddle();
			
			long ceDbId = -1;
			long peDbId = -1;
			
			this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
			fileLogTelegramWriter.write( " this.instrumentLtp="+this.instrumentLtp);
			
			String[] entryStraddleOptionNames = getStraddleOptionNamesByGreekOptimised("delta", 0.5f, 500);
			
			String lastKnownTrend = "Unknown";
			
			String currentTrend = getSellerDirectionByATMIVParity(lastKnownTrend);
			
			if (currentTrend.equals("CE")) {
				ceStraddleOptionName =  entryStraddleOptionNames[0];
				ceHedgeOptionName =  entryStraddleOptionNames[2];
				
				float cePrice = getPriceFromTicks(ceStraddleOptionName);
				
				String logString = "Taking CE directional ceStraddleOptionName="+ceStraddleOptionName + "(@" + cePrice +") ceHedgeOptionName="+ceHedgeOptionName; 
				log.info(logString);
				fileLogTelegramWriter.write( " "+logString);
				ceDbId = createAlgoSellOrder(ceStraddleOptionName, cePrice, noOfLots*lotSize);
				if (this.placeActualOrder) { // Place the order with Kite
					placeRealOrder(ceHedgeOptionName, noOfLots*lotSize, "BUY",  true, KiteUtil.USE_NORMAL_ORDER_FALSE);
					placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
				}
			} else { // PE
				peStraddleOptionName =  entryStraddleOptionNames[1];
				peHedgeOptionName =  entryStraddleOptionNames[3];
				
				float pePrice = getPriceFromTicks(peStraddleOptionName);
				String logString = "Taking PE directional peStraddleOptionName="+peStraddleOptionName + "(@" + pePrice +") peHedgeOptionName="+peHedgeOptionName; 
				log.info(logString);
				fileLogTelegramWriter.write( " "+logString);
				peDbId = createAlgoSellOrder(peStraddleOptionName, pePrice, noOfLots*lotSize);
				if (this.placeActualOrder) { // Place the straddle order with Kite
					placeRealOrder(peHedgeOptionName, noOfLots*lotSize, "BUY",  true, KiteUtil.USE_NORMAL_ORDER_FALSE);
					placeRealOrder(peDbId , peStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
				}
			}
			
			lastKnownTrend = currentTrend;
			
			float maxProfitReached = 0f;
			Date maxProfitReachedAt = getCurrentTime();
			float maxLowestpointReached = 0f;
			Date maxLowestpointReachedAt = getCurrentTime();
			float currentProfitPerLot =0f;
			float maxTrailingProfit = 0f;
			
			updateAlgoStatus("Running");
			
			do {
				sleep(15); // Every 15sec
				
				this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
				
				OptionGreek ceOptionGreeks = !ceStraddleOptionName.equals("")?getOptionGreeks(ceStraddleOptionName):null;
				OptionGreek peOptionGreeks = !peStraddleOptionName.equals("")?getOptionGreeks(peStraddleOptionName):null;
				print(ceOptionGreeks, peOptionGreeks);
				
				float runningCePrice = ceOptionGreeks==null?0: ceOptionGreeks.getLtp();
				float runningPePrice = peOptionGreeks==null?0: peOptionGreeks.getLtp();
				
				if (!ceStraddleOptionName.equals("")) updateCurrentOrderBuyPrice(ceStraddleOptionName, ceDbId, runningCePrice);
				if (!peStraddleOptionName.equals("")) updateCurrentOrderBuyPrice(peStraddleOptionName, peDbId, runningPePrice);
								
				currentProfitPerUnit = getProfitFromDB();
				currentProfitPerLot = currentProfitPerUnit*lotSize;
				if (currentProfitPerLot>maxProfitReached) {
					maxProfitReached=currentProfitPerLot;
					maxProfitReachedAt = getCurrentTime();
				}
				if (currentProfitPerLot<maxLowestpointReached) {
					maxLowestpointReached=currentProfitPerLot;
					maxLowestpointReachedAt = getCurrentTime();
				}
				trailingProfit = (currentProfitPerLot-maxProfitReached)/lotSize;
				if (trailingProfit<maxTrailingProfit) {
					maxTrailingProfit = trailingProfit;
				}
				fileLogTelegramWriter.write( " instrumentLtp=" + this.instrumentLtp +" currentProfit="+currentProfitPerUnit+" maxLowestpointReachedPerUnit="+(maxLowestpointReached/lotSize)+" maxTrailingProfit="+maxTrailingProfit);
				
				currentTrend = getSellerDirectionByATMIVParity(lastKnownTrend); // StatusQuo, CE, PE
				
				fileLogTelegramWriter.write( " currentTrend="+currentTrend);
				
				if (!currentTrend.equals(lastKnownTrend)) {
				
					entryStraddleOptionNames = getStraddleOptionNamesByGreekOptimised("delta", 0.5f, 500);
					
					if (currentTrend.equals("CE")) { // Exit PE, Enter CE
						if (!peStraddleOptionName.equals("")) { // Exit PE, taking Directional
							fileLogTelegramWriter.write( " Exiting ="+peStraddleOptionName );
							// Exit PE
							if (this.placeActualOrder) {
								placeRealOrder( peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
							peStraddleOptionName = "";
						}
						if (!ceStraddleOptionName.equals(entryStraddleOptionNames[0])) {
							if (!ceStraddleOptionName.equals("")) { // Exit and re enter
								fileLogTelegramWriter.write( " Exiting ="+ceStraddleOptionName );
								// Exit CE
								if (this.placeActualOrder) {
									placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
								}
								ceStraddleOptionName = "";
							}
							if (this.noOfOrders<maxAllowedNoOfOrders) {
								ceStraddleOptionName =  entryStraddleOptionNames[0];
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
					} else if (currentTrend.equals("PE")) { // Exit CE, Enter PE
						if (!ceStraddleOptionName.equals("")) { // Exit CE, taking Directional
							fileLogTelegramWriter.write( " Exiting ="+ceStraddleOptionName );
							// Exit CE
							if (this.placeActualOrder) {
								placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
							ceStraddleOptionName = "";
						}
						if (!peStraddleOptionName.equals(entryStraddleOptionNames[1])) {
							if (!peStraddleOptionName.equals("")) { // Exit and re enter
								fileLogTelegramWriter.write( " Exiting ="+peStraddleOptionName );
								if (this.placeActualOrder) {
									placeRealOrder( peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
								}
								peStraddleOptionName = "";
							}
							if (this.noOfOrders<maxAllowedNoOfOrders) {
								peStraddleOptionName =  entryStraddleOptionNames[1];
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
					lastKnownTrend = currentTrend;
				}
				
				checkExitSignals();
				
				if ( (runningCePrice+runningPePrice)>0 && (runningCePrice+runningPePrice)<10f ) {
					prepareExit( "Nothing much left in premium");
				}
				saveAlgoDailySummary(currentProfitPerLot, maxProfitReached, maxProfitReachedAt, maxLowestpointReached, maxLowestpointReachedAt, maxTrailingProfit);
			} while(!exitThread);
			updateAlgoStatus("Terminated");
			String logString = "Exiting Strddle ceStraddleOptionName="+ceStraddleOptionName + " peStraddleOptionName="+peStraddleOptionName; 
			log.info(logString);
			fileLogTelegramWriter.write( " " + logString);
			// exit all positions
			if (this.placeActualOrder) exitStraddle(ceDbId, peDbId);
			fileLogTelegramWriter.write( " noOfOrders="+noOfOrders + " ROI=" + (currentProfitPerLot*100f)/requiredMargin + "% (Max profit/lot reached to "+ (maxProfitReached) +"@" + maxProfitReachedAt+ "\n and Lowest reached to " + (maxLowestpointReached) + "@" + maxLowestpointReachedAt + ")");
		} catch (Exception e) {			
			updateAlgoStatus("Error");
			log.error("Error"+e.getMessage(), e);
			fileLogTelegramWriter.write("Error " + ExceptionUtils.getStackTrace(e));
		} finally {
			fileLogTelegramWriter.close();
		}
	}
	
	private String getSellerDirectionByATMIVParity(String lastKnownTrend) {
		String retVal = lastKnownTrend;
		
		try {
			fileLogTelegramWriter.write("-----------------------------------getSellerDirectionByATMIVParity Begin-----------------------------------");
			
			String[] optionNames = getStraddleOptionNamesByGreekOptimised("delta/gamma", 0.5f, 0);
			OptionGreek ceOptionGreeks = getOptionGreeks(optionNames[0]);
			OptionGreek peOptionGreeks = getOptionGreeks(optionNames[1]);
			print(ceOptionGreeks, peOptionGreeks);
			
			if (ceOptionGreeks.getLtp() > peOptionGreeks.getLtp()) {
				retVal = "CE";
			} else {
				retVal = "PE";
			}
			fileLogTelegramWriter.write("-----------------------------------getSellerDirectionByATMIVParity End----------------------------------- retVal="+retVal);
		} catch(Exception ex) {
			ex.printStackTrace();
		}	
		return retVal;
	}
	
	public static void main(String[] args) {
		new G3PriceParityIVBasedAlgoThread(353L, null);
	}

	

}
