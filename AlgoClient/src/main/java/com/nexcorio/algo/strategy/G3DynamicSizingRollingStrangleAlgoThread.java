package com.nexcorio.algo.strategy;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.util.KiteUtil;

public class G3DynamicSizingRollingStrangleAlgoThread extends G3BaseClass implements Runnable {

	private static final Logger log = LogManager.getLogger(G3PriceParityIVBasedAlgoThread.class);
	
	public float baseDelta = 0.5f;
	public int maxBatches = 3;	
	public float  indexRollingPts = 50f;
		
	public boolean wait4IdealPremium = Boolean.FALSE;
	
	private float idealPremium = 0f;
	
	public G3DynamicSizingRollingStrangleAlgoThread(Long napAlgoId, String backTestDateStr) {
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
			
			List<Long> ceOrderIds = new ArrayList<Long>();
			List<Long> peOrderIds = new ArrayList<Long>();
			
			String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(this.baseDelta, this.optimalHedgeDistance);
			
			for(int i=0;i<maxBatches;i++) {
				ceStraddleOptionName =  entryStraddleOptionNames[0];
				float cePrice = getPriceFromTicks(ceStraddleOptionName);
				fileLogTelegramWriter.write( " Entering ="+ceStraddleOptionName +"(@"+cePrice+") qty"+(noOfLots*lotSize));
				// Place order
				Long ceDbId = createAlgoSellOrder(ceStraddleOptionName, cePrice, noOfLots*lotSize);
				if (this.placeActualOrder) {
					if (ceHedgeOptionName.equals("")) {								
						ceHedgeOptionName =  entryStraddleOptionNames[2];
						placeRealOrder(ceHedgeOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
					}
					placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
				}
				ceOrderIds.add(ceDbId);
				
				peStraddleOptionName =  entryStraddleOptionNames[1];
				float pePrice = getPriceFromTicks(peStraddleOptionName);
				fileLogTelegramWriter.write( "Entering ="+peStraddleOptionName +"(@"+pePrice+") qty"+(noOfLots*lotSize));
				// Place order
				Long peDbId = createAlgoSellOrder(peStraddleOptionName, pePrice, noOfLots*lotSize);
				if (this.placeActualOrder) {
					if (peHedgeOptionName.equals("")) {
						peHedgeOptionName =  entryStraddleOptionNames[3];
						placeRealOrder(peHedgeOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
					}
					placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
				}
				peOrderIds.add(peDbId);
			}
			
			float indexUpperPoint = this.instrumentLtp + indexRollingPts;
			float indexLowerPoint = this.instrumentLtp - indexRollingPts;
			fileLogTelegramWriter.write( "indexUpperPoint="+indexUpperPoint+" indexLowerPoint="+indexLowerPoint);
			
			float maxProfitReached = 0f;
			Date maxProfitReachedAt = getCurrentTime();
			float maxLowestpointReached = 0f;
			Date maxLowestpointReachedAt = getCurrentTime();
			float maxTrailingProfit = 0f;
			
			updateAlgoStatus("Running");
			
			do {
				sleep(5); // Quick to react
				
				this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
				
				OptionGreek ceOptionGreeks = !ceStraddleOptionName.equals("")?getOptionGreeks(ceStraddleOptionName):null;
				OptionGreek peOptionGreeks = !peStraddleOptionName.equals("")?getOptionGreeks(peStraddleOptionName):null;
				
				for(Long ceDbId:ceOrderIds) {
					updateCurrentOrderBuyPrice(ceStraddleOptionName, ceDbId, ceOptionGreeks.getLtp());
				}
				
				for(Long peDbId:peOrderIds) {
					updateCurrentOrderBuyPrice(peStraddleOptionName, peDbId, peOptionGreeks.getLtp());
				}
				
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
				fileLogTelegramWriter.write( " instrumentLtp=" + this.instrumentLtp +" currentProfit="+currentProfitPerUnit+" ceOrderIds.size="+ceOrderIds.size()+" peOrderIds.size="+peOrderIds.size());
				
				if (this.instrumentLtp > indexUpperPoint) { // reduce CE by 1, Increase PE by 1
					if (ceOrderIds.size()>0) {
						fileLogTelegramWriter.write( "Index Upper breach, Exiting ="+ceStraddleOptionName + "qty="+noOfLots*lotSize);
						if (this.placeActualOrder) {
							placeRealOrder(ceOrderIds.get(0), ceStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						ceOrderIds.remove(0);
						indexUpperPoint = this.instrumentLtp + indexRollingPts;
						indexLowerPoint = this.instrumentLtp - indexRollingPts;
					}
					if (peOrderIds.size() < maxBatches) {
						Long peDbId = createAlgoSellOrder(peStraddleOptionName, peOptionGreeks.getLtp(), noOfLots*lotSize);
						if (this.placeActualOrder) {
							placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						peOrderIds.add(peDbId);
						indexUpperPoint = this.instrumentLtp + indexRollingPts;
						indexLowerPoint = this.instrumentLtp - indexRollingPts;
					}
					fileLogTelegramWriter.write( "indexUpperPoint="+indexUpperPoint+" indexLowerPoint="+indexLowerPoint);
				} else if (this.instrumentLtp < indexLowerPoint) {
					if (peOrderIds.size()>0) {
						fileLogTelegramWriter.write( "Index Lower breach, Exiting ="+peStraddleOptionName + "qty="+noOfLots*lotSize);
						if (this.placeActualOrder) {
							placeRealOrder(peOrderIds.get(0), peStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						peOrderIds.remove(0);
						indexUpperPoint = this.instrumentLtp + indexRollingPts;
						indexLowerPoint = this.instrumentLtp - indexRollingPts;
					}
					if (ceOrderIds.size() < maxBatches) {
						Long ceDbId = createAlgoSellOrder(ceStraddleOptionName, ceOptionGreeks.getLtp(), noOfLots*lotSize);
						if (this.placeActualOrder) {
							placeRealOrder(ceDbId, peStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						ceOrderIds.add(ceDbId);
						indexUpperPoint = this.instrumentLtp + indexRollingPts;
						indexLowerPoint = this.instrumentLtp - indexRollingPts;
					}
					fileLogTelegramWriter.write( "indexUpperPoint="+indexUpperPoint+" indexLowerPoint="+indexLowerPoint);
				}
				
//				if ( (ceOptionGreeks.getDelta()+peOptionGreeks.getLtp())>0 && (ceOptionGreeks.getDelta()+peOptionGreeks.getLtp())<10f ) {
//					prepareExit( "Nothing much left in premium");
//				}
				
				checkExitSignals();
					
				saveAlgoDailySummary(currentProfitPerUnit, maxProfitReached, maxProfitReachedAt, maxLowestpointReached, maxLowestpointReachedAt, maxTrailingProfit);
			} while(!exitThread);
			updateAlgoStatus("Terminated");
			String logString = "Exiting Strddle ceStraddleOptionName="+ceStraddleOptionName + " peStraddleOptionName="+peStraddleOptionName; 
			log.info(logString);
			fileLogTelegramWriter.write( " " + logString);
			// exit all positions
			if (this.placeActualOrder) {
				for(int i=0;i<ceOrderIds.size();i++) {
					placeRealOrder(ceOrderIds.get(i), ceStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
				}
				for(int i=0;i<peOrderIds.size();i++) {
					placeRealOrder(peOrderIds.get(i), peStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
				}
			}
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
