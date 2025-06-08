package com.nexcorio.algo.strategy;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.util.KiteUtil;
import com.nexcorio.algo.util.db.HDataSource;

public class G3StraddleToStrangleIVCrushAlgoThread extends G3BaseClass implements Runnable {

	private static final Logger log = LogManager.getLogger(G3PriceParityIVBasedAlgoThread.class);

	public float startingDelta = 0.5f;
	public float deltaUpgradeStep = 0.05f;
	
	public float premiumSpikePercent = 8f;
	public boolean startFromBaseDelta = false;
	
	public G3StraddleToStrangleIVCrushAlgoThread(Long napAlgoId, String backTestDateStr) {
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
			
			boolean lastKnowStatus = false;;
			
			float currentDelta = startingDelta + deltaUpgradeStep;
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
				fileLogTelegramWriter.write( " instrumentLtp=" + this.instrumentLtp +" ****** currentProfit="+currentProfitPerUnit+" ****** maxLowestpointReachedPerUnit="+(maxLowestpointReached)+" maxTrailingProfit="+maxTrailingProfit);
				
				boolean ivRangeSafeForSeller = isIvRangeSafeForSeller(lastKnowStatus);
				
				fileLogTelegramWriter.write("ivRangeSafeForSeller="+ivRangeSafeForSeller );
				
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
						
						if (currentDelta >= 0.25f ) {
							currentDelta = currentDelta - deltaUpgradeStep;						
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
						} else {
							prepareExit(" Too many positions");
						}
					}
				}
				
				if (ceStraddleOptionName.equals("")) { // No open position
					if (ivRangeSafeForSeller) {
						
						currentDelta = currentDelta - deltaUpgradeStep;
						if (startFromBaseDelta) {
							currentDelta = startingDelta + deltaUpgradeStep;	
						}
						if (currentDelta >= 0.25f ) {
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
						} else {
							prepareExit(" Reached lowerDelta level");
						}
					}
				} else { // Already positions running, check for exit rule
					if (!ivRangeSafeForSeller) { // && currentATMStraddlePremium > atmPremiumWhenStraddleFormed
						fileLogTelegramWriter.write( " Exiting running straddle="+ceStraddleOptionName +" and " + peStraddleOptionName);
						if (this.placeActualOrder) {
							placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						updateCurrentOrderStatus(ceStraddleOptionName, ceDbId, "LegClosed");
						updateCurrentOrderStatus(peStraddleOptionName, peDbId, "LegClosed");
						ceStraddleOptionName = "";
						peStraddleOptionName = "";
						
						if (this.noOfOrders >= maxAllowedNoOfOrders) {
							prepareExit("Too many orders");
						}
					}
				}
				
				lastKnowStatus = ivRangeSafeForSeller;
				
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
	
	private boolean isIvRangeSafeForSeller(boolean lastKnowStatus) {
		boolean retVal = lastKnowStatus;
		
		Connection conn = null;
		try {
			conn = HDataSource.getConnection();
			Statement stmt = conn.createStatement();
			
			String fetchSql = "select ceiv, peiv from nexcorio_option_atm_movement_data where f_main_instrument = " + this.mainInstrument.getId() +""
					+ " and base_delta > 0.49 and base_delta < 0.51 ";
			
			if (this.backtestDate!=null) {
				fetchSql = fetchSql + " and record_time <= '" + postgresLongDateFormat.format(getCurrentTime()) + "'";
			}
			fetchSql = fetchSql + " order by record_time desc limit 1";
			
			fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
			ResultSet rs = stmt.executeQuery(fetchSql);
			
			float currentCeIv = 0f;
			float currentPeIv = 0f;
			while (rs.next()) {
				currentCeIv = rs.getFloat("ceiv");
				currentPeIv = rs.getFloat("peiv");
			}
			rs.close();
			
			fetchSql = "select min(ceiv) as minCeIv, max(ceiv) as maxCeIv, min(peiv) as minPeIv, max(peiv) as maxPeIv from nexcorio_option_atm_movement_data where f_main_instrument = " + this.mainInstrument.getId() +""
					+ " and base_delta > 0.49 and base_delta < 0.51 ";
			
			if (this.backtestDate!=null) {
				fetchSql = fetchSql + " and record_time <= '" + postgresLongDateFormat.format(getCurrentTime()) + "'";
			}
			fetchSql = fetchSql + " and record_time >= '" + postgresLongDateFormat.format(getCurrentTime(-5)) + "'";
			
			fileLogTelegramWriter.write("2. fetchSql="+fetchSql);
			
			rs = stmt.executeQuery(fetchSql);
			
			float minCeIv = 0f;
			float maxCeIv = 0f;
			
			float minPeIv = 0f;
			float maxPeIv = 0f;
			
			while (rs.next()) {
				minCeIv = rs.getFloat("minCeIv");
				maxCeIv = rs.getFloat("maxCeIv");
				
				minPeIv = rs.getFloat("minPeIv");
				maxPeIv = rs.getFloat("maxPeIv");
			}
			rs.close();
			stmt.close();
			
			float midCe = (minCeIv + maxCeIv)/2f;
			float midPe = (minPeIv + maxPeIv)/2f;
			
			float oneThirdCe = (maxCeIv - minCeIv)/3;
			float oneThirdPe = (maxPeIv - minPeIv)/3;
			
			fileLogTelegramWriter.write("currentCeIv="+currentCeIv+" currentPeIv="+currentPeIv+" minCeIv="+minCeIv+" maxCeIv="+maxCeIv+" minPeIv="+minPeIv+" maxPeIv="+maxPeIv+" midCe="+midCe+" midPe="+midPe);
			
			if (maxCeIv-minCeIv < 0.5f && maxPeIv-minPeIv < 0.5f) {	
				return retVal;
			}
			
			
			if (maxCeIv-minCeIv > 1f || maxPeIv-minPeIv > 1f) {				
				if (currentCeIv <= midCe && currentPeIv <= midPe) {
					retVal = true;
				} else if (lastKnowStatus==true) {
					if(currentCeIv < (minCeIv+2f*oneThirdCe) && currentPeIv < (minPeIv+2f*oneThirdPe) ) {
						retVal = true;
					} else {
						retVal = false;
					}
				}
			} else {
				retVal = false;
			}
			
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
		return retVal;
	}
	
}
