package com.nexcorio.algo.strategy;

import java.util.Date;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.util.KiteUtil;

public class G3GreekSensitiveThetaMonitoredStrangleAlgoThread extends G3BaseClass implements Runnable{

	private static final Logger log = LogManager.getLogger(G3PriceParityIVBasedAlgoThread.class);
	
	public float baseDelta = 0.5f;
	public String greekname = "delta";
	
	public float bothLegGreekDiffPct = 7.5f;
	public float eachLegGreekDiffPct = 0f;
	public float thetaDiffPct = 2f;
	
	public G3GreekSensitiveThetaMonitoredStrangleAlgoThread(Long napAlgoId, String backTestDateStr) {
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
			
			float totalGreekWhenFormed = 0f;
			float minGreekReached = 0f;
			
			float ceGreekWhenStraddleFormed = 0f;
			float peGreekWhenStraddleFormed = 0f;
			
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
				
				float totalGreekCurrent = 0f;
				if (this.greekname.equalsIgnoreCase("delta")) {
					totalGreekCurrent = (ceOptionGreeks!=null?Math.abs(ceOptionGreeks.getDelta()):0f) + (peOptionGreeks!=null?Math.abs(peOptionGreeks.getDelta()):0f);
				} else if (this.greekname.equalsIgnoreCase("iv")) {
					totalGreekCurrent = (ceOptionGreeks!=null?Math.abs(ceOptionGreeks.getIv()):0f) + (peOptionGreeks!=null?Math.abs(peOptionGreeks.getIv()):0f);
				}
				
				String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(baseDelta, this.optimalHedgeDistance); // getStraddleOptionNamesByGreekOptimised("ltp", this.baseDelta, this.optimalHedgeDistance);
				
				String ceOptionname = entryStraddleOptionNames[0];
				String peOptionname = entryStraddleOptionNames[1];
				
				OptionGreek atmCEOptionGreeks = getOptionGreeks(ceOptionname);
				OptionGreek atmPEOptionGreeks = getOptionGreeks(peOptionname);
				fileLogTelegramWriter.write("--------------------- ATM Begin");
				print(atmCEOptionGreeks, atmPEOptionGreeks);
				fileLogTelegramWriter.write("--------------------- ATM end");
				
				float atmThetaDiffPct = Math.abs(atmCEOptionGreeks.getIv() - atmPEOptionGreeks.getIv());
				
				fileLogTelegramWriter.write("Theta out of range, atmThetaDiffPct="+atmThetaDiffPct);
				
				if ( atmThetaDiffPct > thetaDiffPct) {
					
					if (!ceStraddleOptionName.equals("")) { // Exit 
						fileLogTelegramWriter.write( " Exiting ="+ceStraddleOptionName );						
						if (this.placeActualOrder) {
							placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						ceStraddleOptionName = "";
						if (!peStraddleOptionName.equals("")) { // Exit 
							fileLogTelegramWriter.write( " Exiting ="+peStraddleOptionName );
							if (this.placeActualOrder) {
								placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
							peStraddleOptionName = "";
						}
					}
				} else if (ceStraddleOptionName.equals("") && atmThetaDiffPct > thetaDiffPct-0.5f) {
					fileLogTelegramWriter.write(" Need to wait for some more time");
				} else {
					boolean needRepositioning = false;
					
					if (ceStraddleOptionName.equals("")) {
						needRepositioning = true; // Just starting, no open positions
					} 
					if (needRepositioning == false) {
						float changeinGreeksPercent =  totalGreekWhenFormed>0f? Math.abs( (totalGreekCurrent-totalGreekWhenFormed) )*100f/totalGreekWhenFormed:0f;
						if (bothLegGreekDiffPct > 0f && changeinGreeksPercent > bothLegGreekDiffPct) {
							fileLogTelegramWriter.write("Realigning bothLegGreekDiffPct="+bothLegGreekDiffPct+" changeinGreeksPercent="+changeinGreeksPercent); 
							needRepositioning = true;
						}
					} 
					if (needRepositioning == false && eachLegGreekDiffPct > 0f) {
						float changeinCEGreeksPercent =  0f; 
						float changeinPEGreeksPercent =  0f;
						if (this.greekname.equalsIgnoreCase("delta")) {
							changeinCEGreeksPercent = ceGreekWhenStraddleFormed>0f? Math.abs( (ceOptionGreeks.getDelta()-ceGreekWhenStraddleFormed) )*100f/ceGreekWhenStraddleFormed:0f;
							changeinPEGreeksPercent = peGreekWhenStraddleFormed>0f? Math.abs( (-peOptionGreeks.getDelta()-peGreekWhenStraddleFormed) )*100f/peGreekWhenStraddleFormed:0f;
						} else if (this.greekname.equalsIgnoreCase("iv")) {
							changeinCEGreeksPercent = ceGreekWhenStraddleFormed>0f? Math.abs( (ceOptionGreeks.getIv()-ceGreekWhenStraddleFormed) )*100f/ceGreekWhenStraddleFormed:0f;
							changeinPEGreeksPercent = peGreekWhenStraddleFormed>0f? Math.abs( (peOptionGreeks.getIv()-peGreekWhenStraddleFormed) )*100f/peGreekWhenStraddleFormed:0f;
						}
						if (changeinCEGreeksPercent > eachLegGreekDiffPct || changeinPEGreeksPercent > eachLegGreekDiffPct) {
							fileLogTelegramWriter.write("Realigning changeinCEGreeksPercent="+changeinCEGreeksPercent+" changeinPEGreeksPercent="+changeinPEGreeksPercent);
							needRepositioning = true;
						}
					}
					
					if (needRepositioning) {
						ceOptionname = entryStraddleOptionNames[0];
						
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
						
						peOptionname = entryStraddleOptionNames[1];
						float peGreekValue = 0f;
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
						
						if (!ceStraddleOptionName.equals("")) {
							ceOptionGreeks = getOptionGreeks(ceStraddleOptionName);
							if (this.greekname.equalsIgnoreCase("delta")) {
								ceGreekValue = Math.abs(ceOptionGreeks.getDelta());	
							} else if (this.greekname.equalsIgnoreCase("iv")) {
								ceGreekValue = ceOptionGreeks.getIv();	
							}
						}
						if (!peStraddleOptionName.equals("")) {
							peOptionGreeks = getOptionGreeks(peStraddleOptionName);
							if (this.greekname.equalsIgnoreCase("delta")) {
								peGreekValue = Math.abs(peOptionGreeks.getDelta());	
							} else if (this.greekname.equalsIgnoreCase("iv")) {
								peGreekValue = peOptionGreeks.getIv();	
							}
						}
						totalGreekWhenFormed = ceGreekValue + peGreekValue;
						minGreekReached = totalGreekWhenFormed;
						ceGreekWhenStraddleFormed = ceGreekValue;
						peGreekWhenStraddleFormed = peGreekValue;
					} else {
						if (totalGreekCurrent < minGreekReached) {
							minGreekReached = totalGreekCurrent;
						}
					}
					
					if ( (runningCePrice+runningPePrice)>0 && (runningCePrice+runningPePrice)<10f ) {
						prepareExit( "Nothing much left in premium");
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
