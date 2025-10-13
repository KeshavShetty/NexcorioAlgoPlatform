package com.nexcorio.algo.strategy;

import java.util.Date;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.util.KiteUtil;

public class G3MaxTimeValueStraddleAlgoThread extends G3BaseClass implements Runnable{

	private static final Logger log = LogManager.getLogger(G3PriceParityIVBasedAlgoThread.class);
	
	public float baseDelta = 0.5f;
	
	public float timevalueDiff = 10f;
	
	public boolean wait4IdealPremium = Boolean.FALSE;
	
	private float idealPremium = 0f;
	
	public G3MaxTimeValueStraddleAlgoThread(Long napAlgoId, String backTestDateStr) {
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
			
			String[] entryStraddleOptionNames = null;
			float ceIvWhenStraddleFormed  =0f;
			float peIvWhenStraddleFormed  =0f;
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
				
				float currentATMStraddlePremium = getStradlePremium();
				float currentStraddlePositionPremium = (ceOptionGreeks==null?0: ceOptionGreeks.getLtp()) + (peOptionGreeks==null?0: peOptionGreeks.getLtp());
				
				fileLogTelegramWriter.write( "currentATMStraddlePremium="+currentATMStraddlePremium+" currentStraddlePositionPremium="+currentStraddlePositionPremium);
				
				boolean needRepositioning = false;
				
				if (ceStraddleOptionName.equals("")) {
					needRepositioning = true; // Just starting, no open positions
					entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(this.baseDelta, this.optimalHedgeDistance);
				}  else {
					entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(this.baseDelta, this.optimalHedgeDistance);
					float totalTimevalueInRunningStraddle = getTotalTimeValue(ceOptionGreeks, peOptionGreeks);
					
					OptionGreek newCeGreek = getOptionGreeks(entryStraddleOptionNames[0]);
					OptionGreek newPeGreek = getOptionGreeks(entryStraddleOptionNames[1]);
					
					float totalTimevalueInNewStraddle = getTotalTimeValue(newCeGreek, newPeGreek);
					fileLogTelegramWriter.write("Diff="+ (totalTimevalueInNewStraddle - totalTimevalueInRunningStraddle));
					if (totalTimevalueInNewStraddle - totalTimevalueInRunningStraddle > timevalueDiff) {
						needRepositioning = true; 
					}
				
					if ( (ceOptionGreeks.getIv() - ceIvWhenStraddleFormed) + (peOptionGreeks.getIv() - peIvWhenStraddleFormed) > 0.75f) {
						fileLogTelegramWriter.write("IV raise");
						needRepositioning = true; 
					}
				}
				if (needRepositioning) {
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
					ceOptionGreeks = !ceStraddleOptionName.equals("")?getOptionGreeks(ceStraddleOptionName):null;
					peOptionGreeks = !peStraddleOptionName.equals("")?getOptionGreeks(peStraddleOptionName):null;
					ceIvWhenStraddleFormed = ceOptionGreeks.getIv();
					peIvWhenStraddleFormed = peOptionGreeks.getIv();
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
	
	private float getTotalTimeValue(OptionGreek ceGreek, OptionGreek peGreek) {
		float retVal = 0;
		
		int ceStrike = getStrikePriceFromOptionName(ceGreek.getTradingSymbol());
		int peStrike = getStrikePriceFromOptionName(peGreek.getTradingSymbol());
		
		float ceIntrinsicValue =  ceStrike<this.instrumentLtp?(this.instrumentLtp-ceStrike):0f;
		float peIntrinsicValue =  this.instrumentLtp<peStrike?(peStrike-this.instrumentLtp):0f;
		
		float ceTimevalue = ceGreek.getLtp() - ceIntrinsicValue;
		float peTimevalue = peGreek.getLtp() - peIntrinsicValue;
		
		retVal = ceTimevalue + peTimevalue;
		
		fileLogTelegramWriter.write("this.instrumentLtp="+this.instrumentLtp+" " + ceGreek.getTradingSymbol() + " " + peGreek.getTradingSymbol()
			+ " ceIntrinsicValue="+ceIntrinsicValue+" peIntrinsicValue="+ peIntrinsicValue+" ceTimevalue=" + ceTimevalue+ " peTimevalue="+peTimevalue);
				
		return retVal;
	}
	
}
