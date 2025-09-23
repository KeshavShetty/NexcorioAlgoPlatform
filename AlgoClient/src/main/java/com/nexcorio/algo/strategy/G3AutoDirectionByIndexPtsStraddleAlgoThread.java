package com.nexcorio.algo.strategy;

import java.util.Date;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.util.KiteUtil;

public class G3AutoDirectionByIndexPtsStraddleAlgoThread extends G3BaseClass implements Runnable{

	private static final Logger log = LogManager.getLogger(G3AutoDirectionByIndexPtsStraddleAlgoThread.class);
	
	public float baseDelta = 0.6f;
	public float indexPoints = 50f;
	public float reEntryPoints = 0f;
	
	public boolean adjustEntryExit = false;
	public boolean matchDelta = false;
	
	public G3AutoDirectionByIndexPtsStraddleAlgoThread(Long napAlgoId, String backTestDateStr) {
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
			
			float maxProfitReached = 0f;
			Date maxProfitReachedAt = getCurrentTime();
			float maxLowestpointReached = 0f;
			Date maxLowestpointReachedAt = getCurrentTime();
			float maxTrailingProfit = 0f;
			
			this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
			
			fileLogTelegramWriter.write( " this.instrumentLtp="+this.instrumentLtp);
			
			printFields(this);
			
			updateAlgoStatus("Running");
			
			String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised( baseDelta, this.hedgeDistance);
			
			ceStraddleOptionName =  entryStraddleOptionNames[0];
			peStraddleOptionName =  entryStraddleOptionNames[1];
			
			ceHedgeOptionName =  entryStraddleOptionNames[2];			
			peHedgeOptionName =  entryStraddleOptionNames[3];
			
			String ceOptionname = ceStraddleOptionName; // Original picks
			String peOptionname = peStraddleOptionName;
			
			float cePrice = getPriceFromTicks(ceStraddleOptionName);
			float pePrice = getPriceFromTicks(peStraddleOptionName);
			
			fileLogTelegramWriter.write( " Entering ="+ceStraddleOptionName +"(@"+cePrice+")" + peStraddleOptionName +"(@"+pePrice+")");
			// Place order
			ceDbId = createAlgoSellOrder(ceStraddleOptionName, cePrice, noOfLots*lotSize);
			peDbId = createAlgoSellOrder(peStraddleOptionName, pePrice, noOfLots*lotSize);
			
			if (this.placeActualOrder) {
				placeRealOrder(ceHedgeOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
				placeRealOrder(peHedgeOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
				placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
				placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
			}
			
			boolean ceLegOpen = true;
			boolean peLegOpen = true;
			
			float ceExitAtIndex    = this.instrumentLtp + indexPoints;
			float ceReEntryAtIndex = this.instrumentLtp + indexPoints/2f;
			if (reEntryPoints != 0) ceReEntryAtIndex = this.instrumentLtp + reEntryPoints;
			
			float peExitAtIndex    = this.instrumentLtp - indexPoints;
			float peReEntryAtIndex = this.instrumentLtp - indexPoints/2f;
			if (reEntryPoints != 0) peReEntryAtIndex = this.instrumentLtp - reEntryPoints;
			
			float minIndexReached = this.instrumentLtp;
			float maxIndexReached = this.instrumentLtp;
			
			
			do {
				sleep(5); // Every 10sec
				
				fileLogTelegramWriter.write( " this.indexLtp="+this.instrumentLtp + ", Set ceExitAtIndex="+ceExitAtIndex+" ceReEntryAtIndex=" + ceReEntryAtIndex + " peExitAtIndex="+peExitAtIndex+" peReEntryAtIndex=" + peReEntryAtIndex +" minIndexReached="+minIndexReached+" maxIndexReached="+maxIndexReached); 
				
				this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
				
				OptionGreek ceOptionGreeks = getOptionGreeks(ceStraddleOptionName);
				OptionGreek peOptionGreeks = getOptionGreeks(peStraddleOptionName);
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
				if (trailingProfit < maxTrailingProfit) {
					maxTrailingProfit = trailingProfit;
				}
				fileLogTelegramWriter.write( " instrumentLtp=" + this.instrumentLtp +" currentProfit="+currentProfitPerUnit+" maxLowestpointReachedPerUnit="+(maxLowestpointReached)+" maxTrailingProfit="+maxTrailingProfit);
				
				if (ceLegOpen == true && this.instrumentLtp > ceExitAtIndex) {
					// Exit CE leg
					fileLogTelegramWriter.write( " Exiting ="+ceStraddleOptionName );
					if (this.placeActualOrder) {
						placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
					}
					ceStraddleOptionName = "";
					ceLegOpen = false;
				}
				if (peLegOpen == true && this.instrumentLtp < peExitAtIndex) {
					// Exit PE leg
					fileLogTelegramWriter.write( " Exiting ="+peStraddleOptionName );
					if (this.placeActualOrder) {
						placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
					}
					peStraddleOptionName = "";
					peLegOpen = false;
				}
				
				if (ceLegOpen == false || peLegOpen == false) {
					if (matchDelta) {
						float deltaToUse = 0.5f;
						if (ceLegOpen == false) deltaToUse = Math.abs(peOptionGreeks.getDelta());
						else deltaToUse = Math.abs(ceOptionGreeks.getDelta());
						entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(deltaToUse, this.hedgeDistance);
						if (ceLegOpen == false) ceOptionname = entryStraddleOptionNames[0];
						else if (peLegOpen == false) ceOptionname = entryStraddleOptionNames[1];
					}
					float origCeOptionPriceNow = getPriceFromTicks(ceOptionname);
					float origPeOptionPriceNow = getPriceFromTicks(peOptionname);
				
					if (ceLegOpen == false && this.instrumentLtp < ceReEntryAtIndex) {
						ceStraddleOptionName = ceOptionname;
						ceDbId = createAlgoSellOrder(ceStraddleOptionName, origCeOptionPriceNow, noOfLots*lotSize);
						
						fileLogTelegramWriter.write( " Reentering ="+ceStraddleOptionName+"@" + origCeOptionPriceNow ); 
						
						if (this.placeActualOrder) {
							placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						ceLegOpen = true;
						if (adjustEntryExit) ceExitAtIndex    = this.instrumentLtp + indexPoints;
					}
					
					if (peLegOpen == false &&  this.instrumentLtp > peReEntryAtIndex) {
						peStraddleOptionName = peOptionname;
						peDbId = createAlgoSellOrder(peStraddleOptionName, origPeOptionPriceNow, noOfLots*lotSize);
						
						fileLogTelegramWriter.write( " Reentering ="+peStraddleOptionName+"@" + origPeOptionPriceNow ); 
						
						if (this.placeActualOrder) {
							placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						peLegOpen = true;
						if (adjustEntryExit) peExitAtIndex    = this.instrumentLtp - indexPoints;
					}
				}
				
				if ( this.instrumentLtp < minIndexReached) {
					minIndexReached = this.instrumentLtp;
					if (adjustEntryExit) peReEntryAtIndex = this.instrumentLtp + indexPoints/2f;
				}
				if ( this.instrumentLtp > maxIndexReached) {
					maxIndexReached = this.instrumentLtp;
					if (adjustEntryExit) ceReEntryAtIndex = this.instrumentLtp - indexPoints/2f;
				}
				
				checkExitSignals();
				
				if ( (runningCePrice+runningPePrice)>0 && (runningCePrice+runningPePrice)<10f ) {
					prepareExit( "Nothing much left in premium");
				}
				saveAlgoDailySummary(currentProfitPerUnit, maxProfitReached, maxProfitReachedAt, maxLowestpointReached, maxLowestpointReachedAt, maxTrailingProfit);
			} while(!exitThread);
			updateAlgoStatus("Terminated");
			String logString = "Exiting Strddle ceStraddleOptionName="+ceStraddleOptionName + " peStraddleOptionName="+peStraddleOptionName; 
			log.info(logString);
			fileLogTelegramWriter.write( " " + logString);
			// exit all positions
			if (this.placeActualOrder) exitStraddle(ceDbId, peDbId);
			fileLogTelegramWriter.write( " noOfOrders="+noOfOrders + " ROI=" + (currentProfitPerUnit*this.lotSize*100f)/requiredMargin + "% (Max profit reached to "+ (maxProfitReached) +"@" + maxProfitReachedAt+ "\n and Lowest reached to " + (maxLowestpointReached) + "@" + maxLowestpointReachedAt + ")");
			
		} catch (Exception e) {			
			updateAlgoStatus("Error");
			log.error("Error"+e.getMessage(), e);
			fileLogTelegramWriter.write("Error " + ExceptionUtils.getStackTrace(e));
		} finally {
			fileLogTelegramWriter.close();
		}
	}
	
	public static void main(String[] args) {
		
	}
}
