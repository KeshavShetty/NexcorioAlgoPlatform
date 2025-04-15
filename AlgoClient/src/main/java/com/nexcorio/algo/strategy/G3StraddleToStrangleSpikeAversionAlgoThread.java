package com.nexcorio.algo.strategy;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.util.KiteUtil;
import com.nexcorio.algo.util.db.HDataSource;

public class G3StraddleToStrangleSpikeAversionAlgoThread extends G3BaseClass implements Runnable{

	private static final Logger log = LogManager.getLogger(G3PriceParityIVBasedAlgoThread.class);

	public float startingDelta = 0.25f;
	public float deltaUpgradeStep = 0.05f;
	
	public float premiumSpikePercent = 8f;
	
	public G3StraddleToStrangleSpikeAversionAlgoThread(Long napAlgoId, String backTestDateStr) {
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
			
			float lowestATMStraddlePremium = getATMStraddlePremium();
			float highestATMStraddlePremium = lowestATMStraddlePremium;
			
			float currentDelta = startingDelta - deltaUpgradeStep;
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
				fileLogTelegramWriter.write( " instrumentLtp=" + this.instrumentLtp +" ****** currentProfit="+currentProfitPerUnit+" ****** maxLowestpointReachedPerUnit="+(maxLowestpointReached/lotSize)+" maxTrailingProfit="+maxTrailingProfit);
				
				fileLogTelegramWriter.write("lowestATMStraddlePremium="+ lowestATMStraddlePremium+" highestATMStraddlePremium="+highestATMStraddlePremium+" Entry at "
						+ (highestATMStraddlePremium*(100f - premiumSpikePercent)/100f) + " Exit at " + ( lowestATMStraddlePremium*(100f + premiumSpikePercent)/100f) );  
				
				float currentATMStraddlePremium = getATMStraddlePremium();
				
				if (!ceStraddleOptionName.equals("")) { // Position exist, check for realignment
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
						
						if (currentDelta <= 0.5f ) {
							currentDelta = currentDelta + deltaUpgradeStep;						
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
						}
					}
				}
				
				if (ceStraddleOptionName.equals("")) { // No open position
					if (currentATMStraddlePremium < highestATMStraddlePremium*(100f - premiumSpikePercent)/100f) {
						
						currentDelta = currentDelta + deltaUpgradeStep;						
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
						
						highestATMStraddlePremium = currentATMStraddlePremium;
						lowestATMStraddlePremium  = currentATMStraddlePremium;
					}
				} else { // Already positions running, check for exit rule
					if (currentATMStraddlePremium > lowestATMStraddlePremium*(100f + premiumSpikePercent)/100f
							) { // && currentATMStraddlePremium > atmPremiumWhenStraddleFormed
						fileLogTelegramWriter.write( " Exiting running straddle="+ceStraddleOptionName +" and " + peStraddleOptionName);
						if (this.placeActualOrder) {
							placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						updateCurrentOrderStatus(ceStraddleOptionName, ceDbId, "LegClosed");
						updateCurrentOrderStatus(peStraddleOptionName, peDbId, "LegClosed");
						ceStraddleOptionName = "";
						peStraddleOptionName = "";
						
						highestATMStraddlePremium = currentATMStraddlePremium;
						lowestATMStraddlePremium  = currentATMStraddlePremium;
						
						if (this.noOfOrders >= maxAllowedNoOfOrders) {
							prepareExit("Too many orders");
						}
					}
				}
				
				if (currentATMStraddlePremium > highestATMStraddlePremium) highestATMStraddlePremium = currentATMStraddlePremium;
				if (currentATMStraddlePremium < lowestATMStraddlePremium)  lowestATMStraddlePremium  = currentATMStraddlePremium;
				
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
	
	private float getATMStraddlePremium() {
		float avgeAtmPremium = 0f;
		
		Connection conn = null;
		try {
			conn = HDataSource.getConnection();
			Statement stmt = conn.createStatement();
			
			String fetchSql = "select celtp+peltp as atmPremium from nexcorio_option_atm_movement_data where f_main_instrument = " + this.mainInstrument.getId() +""
					+ " and base_delta > 0.49 and base_delta < 0.51 ";
			
			if (this.backtestDate!=null) {
				fetchSql = fetchSql + " and record_time <= '" + postgresLongDateFormat.format(getCurrentTime()) + "'";
			}
			fetchSql = fetchSql + " order by record_time desc limit 3";
			
			fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
			ResultSet rs = stmt.executeQuery(fetchSql);
			
			// We only need avg(2 closest among 3), this is because we observed some sharp big spike/fall in atm premium lasting 1 sec or burst found, to eliminate such outlier 
			
			List<Float> allNumbers = new ArrayList<>();
			while (rs.next()) {
				float currentAtmPremium = rs.getFloat("atmPremium");
				allNumbers.add(currentAtmPremium);
				avgeAtmPremium = avgeAtmPremium + currentAtmPremium;
			}
			rs.close();			
			stmt.close();
			
			avgeAtmPremium = avgeAtmPremium/3f;
			
			List<Float> leftSideNumbers = new ArrayList<>();
			List<Float> rightSideNumbers = new ArrayList<>();
			
			for(int i=0;i<allNumbers.size();i++) {
				if (allNumbers.get(i) > avgeAtmPremium) leftSideNumbers.add(allNumbers.get(i));
				else rightSideNumbers.add(allNumbers.get(i));
			}
			
			if (leftSideNumbers.size()>1) {
				avgeAtmPremium = (leftSideNumbers.get(0) + leftSideNumbers.get(1))/2f;
			} else {
				avgeAtmPremium = (rightSideNumbers.get(0) + rightSideNumbers.get(1))/2f;
			}
			fileLogTelegramWriter.write("In getATMStraddlePremium returning="+avgeAtmPremium);
		} catch(Exception ex) {
			ex.printStackTrace();
		}finally {
			try {
				if (conn!=null) conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
			
		return avgeAtmPremium;
	}
	
}
