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

public class G3AllSeasonRollingJodiAlgoThread extends G3BaseClass implements Runnable {

	private static final Logger log = LogManager.getLogger(G3PriceParityIVBasedAlgoThread.class);
	
	public float baseDelta = 0.5f;	
	public boolean  avoidOutlier = false;
	public boolean waitForIdealEntry = false;
	
	
	private float indexAt920 = 0f;
	
	
		
	public G3AllSeasonRollingJodiAlgoThread(Long napAlgoId, String backTestDateStr) {
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
			
			this.indexAt920 = this.instrumentLtp;
			
			fileLogTelegramWriter.write( " this.instrumentLtp="+this.instrumentLtp);
			
			printFields(this);
			
			if(this.waitForIdealEntry ==  true) {
				while(!isIdealEntry()) {
					sleep(15);
				}
			}
			
			this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
			fileLogTelegramWriter.write( " this.instrumentLtp="+this.instrumentLtp);
			
			String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(this.baseDelta, this.optimalHedgeDistance);

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
			
			peStraddleOptionName =  entryStraddleOptionNames[1];
			float pePrice = getPriceFromTicks(peStraddleOptionName);
			fileLogTelegramWriter.write( "Entering ="+peStraddleOptionName +"(@"+pePrice+")");
			// Place order
			peDbId = createAlgoSellOrder(peStraddleOptionName, pePrice, noOfLots*lotSize);
			if (this.placeActualOrder) {
				if (peHedgeOptionName.equals("")) {
					//peHedgeOptionName =  entryStraddleOptionNames[3];
					//placeRealOrder(peHedgeOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
				}
				placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
			}
			
			OptionGreek ceGreekWhenJodiFormed = getOptionGreeks(ceStraddleOptionName);
			OptionGreek peGreekWhenJodiFormed = getOptionGreeks(peStraddleOptionName);
			
			float maxProfitReached = 0f;
			Date maxProfitReachedAt = getCurrentTime();
			float maxLowestpointReached = 0f;
			Date maxLowestpointReachedAt = getCurrentTime();
			float maxTrailingProfit = 0f;
			
			updateAlgoStatus("Running");
			
			float indexAtWhenJodiFormed = this.instrumentLtp;
			
			String lastKnownTrend = "Neutral";
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
				
				String volatilityDirection = getVolatalityDirection(lastKnownTrend);
				
				if (volatilityDirection.equals("Avoid")) {
					if (!ceStraddleOptionName.equals("")) {
						fileLogTelegramWriter.write( "Outlier avoid, Exiting ="+ceStraddleOptionName );
						if (this.placeActualOrder) {
							placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						updateCurrentOrderStatus(ceStraddleOptionName, ceDbId, "LegClosed");
						ceStraddleOptionName = "";
					}
					if (!peStraddleOptionName.equals("")) {
						fileLogTelegramWriter.write( "Outlier avoid, Exiting ="+peStraddleOptionName );
						if (this.placeActualOrder) {
							placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						updateCurrentOrderStatus(peStraddleOptionName, peDbId, "LegClosed");
						peStraddleOptionName = "";
					}
				} else {
					if (!ceStraddleOptionName.equals("") && !peStraddleOptionName.equals("")) { // Both exist
						if (!volatilityDirection.equals("Neutral")) {
							if (volatilityDirection.equals("CE")) {
								fileLogTelegramWriter.write( "Volatality, Exiting ="+peStraddleOptionName );
								if (this.placeActualOrder) {
									placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
								}
								updateCurrentOrderStatus(peStraddleOptionName, peDbId, "LegClosed");
								peStraddleOptionName = "";
							} else {
								fileLogTelegramWriter.write( "Volatality, Exiting ="+ceStraddleOptionName );
								if (this.placeActualOrder) {
									placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
								}
								updateCurrentOrderStatus(ceStraddleOptionName, ceDbId, "LegClosed");
								ceStraddleOptionName = "";
							}
						} else {
							if (Math.abs(ceOptionGreeks.getDelta()) - Math.abs(ceGreekWhenJodiFormed.getDelta()) > 0.12f || Math.abs(peOptionGreeks.getDelta()) - Math.abs(peGreekWhenJodiFormed.getDelta()) > 0.12f							
								
									|| Math.abs(ceOptionGreeks.getStrike() - this.instrumentLtp) > 75f || Math.abs(peOptionGreeks.getStrike() - this.instrumentLtp) > 75f ) {
								entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(this.baseDelta, this.optimalHedgeDistance);
								ceGreekWhenJodiFormed = getOptionGreeks(entryStraddleOptionNames[0]);
								if (!ceStraddleOptionName.equals(entryStraddleOptionNames[0])) { // Exit CE leg and enter
									fileLogTelegramWriter.write( " Exiting ="+ceStraddleOptionName );
									if (this.placeActualOrder) {
										placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
									}
									updateCurrentOrderStatus(ceStraddleOptionName, ceDbId, "LegClosed");
									ceStraddleOptionName = "";
									if (this.noOfOrders<maxAllowedNoOfOrders) {
										ceStraddleOptionName =  entryStraddleOptionNames[0];
										
										fileLogTelegramWriter.write( " Entering ="+ceStraddleOptionName +"(@"+ceGreekWhenJodiFormed.getLtp()+")");
										ceDbId = createAlgoSellOrder(ceStraddleOptionName, ceGreekWhenJodiFormed.getLtp(), noOfLots*lotSize);
										if (this.placeActualOrder) {
											placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
										}
									} else {
										prepareExit("Too many orders");
									}
								} else {
									fileLogTelegramWriter.write( "Reatining " + ceStraddleOptionName);
								}
								peGreekWhenJodiFormed = getOptionGreeks(entryStraddleOptionNames[1]);
								if (!peStraddleOptionName.equals(entryStraddleOptionNames[1])) { // Exit CE leg and enter
									fileLogTelegramWriter.write( " Exiting ="+peStraddleOptionName );
									if (this.placeActualOrder) {
										placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
									}
									updateCurrentOrderStatus(peStraddleOptionName, peDbId, "LegClosed");
									peStraddleOptionName = "";
									if (this.noOfOrders<maxAllowedNoOfOrders) {
										peStraddleOptionName =  entryStraddleOptionNames[1];
										fileLogTelegramWriter.write( " Entering ="+peStraddleOptionName +"(@"+peGreekWhenJodiFormed.getLtp()+")");
										peDbId = createAlgoSellOrder(peStraddleOptionName, peGreekWhenJodiFormed.getLtp(), noOfLots*lotSize);
										if (this.placeActualOrder) {
											placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
										}
									} else {
										prepareExit("Too many orders");
									}
								} else {
									fileLogTelegramWriter.write( "Reatining " + ceStraddleOptionName);
								}
							}
						}
					} else { // Mostly high volatile time, only one leg exist, check for renetry 
						if (volatilityDirection.equals("Neutral")) {
							if (ceStraddleOptionName.equals("")) {
								if (this.noOfOrders<maxAllowedNoOfOrders) {
									ceStraddleOptionName =  entryStraddleOptionNames[0];
									//ceGreekWhenJodiFormed = getOptionGreeks(ceStraddleOptionName);
									cePrice = getPriceFromTicks(ceStraddleOptionName);
									fileLogTelegramWriter.write( "ReEntering ="+ceStraddleOptionName +"(@"+cePrice+")");
									ceDbId = createAlgoSellOrder(ceStraddleOptionName, cePrice, noOfLots*lotSize);
									if (this.placeActualOrder) {
										placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
									}
								} else {
									prepareExit("Too many orders");
								}
							}
							if (peStraddleOptionName.equals("")) {
								if (this.noOfOrders<maxAllowedNoOfOrders) {
									peStraddleOptionName =  entryStraddleOptionNames[1];
									//peGreekWhenJodiFormed = getOptionGreeks(peStraddleOptionName);
									pePrice = getPriceFromTicks(peStraddleOptionName);
									fileLogTelegramWriter.write( "ReEntering ="+peStraddleOptionName +"(@"+pePrice+")");
									peDbId = createAlgoSellOrder(peStraddleOptionName, pePrice, noOfLots*lotSize);
									if (this.placeActualOrder) {
										placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
									}
								} else {
									prepareExit("Too many orders");
								}
							}
						} else {
							if (volatilityDirection.equals("CE")) {
								if (!peStraddleOptionName.equals("")) {
									fileLogTelegramWriter.write( "Volatality, Exiting ="+peStraddleOptionName );
									if (this.placeActualOrder) {
										placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
									}
									updateCurrentOrderStatus(peStraddleOptionName, peDbId, "LegClosed");
									peStraddleOptionName = "";
								}
								if (ceStraddleOptionName.equals("")) {
									if (this.noOfOrders<maxAllowedNoOfOrders) {
										ceStraddleOptionName =  entryStraddleOptionNames[0];
										ceGreekWhenJodiFormed = getOptionGreeks(ceStraddleOptionName);
										fileLogTelegramWriter.write( "ReEntering ="+ceStraddleOptionName +"(@"+ceGreekWhenJodiFormed.getLtp()+")");
										ceDbId = createAlgoSellOrder(ceStraddleOptionName, ceGreekWhenJodiFormed.getLtp(), noOfLots*lotSize);
										if (this.placeActualOrder) {
											placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
										}
									} else {
										prepareExit("Too many orders");
									}
								}
							} else {
								if (!ceStraddleOptionName.equals("")) {
									fileLogTelegramWriter.write( "Volatality, Exiting ="+ceStraddleOptionName );
									if (this.placeActualOrder) {
										placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
									}
									updateCurrentOrderStatus(ceStraddleOptionName, ceDbId, "LegClosed");
									ceStraddleOptionName = "";
								}
								if (peStraddleOptionName.equals("")) {
									if (this.noOfOrders<maxAllowedNoOfOrders) {
										peStraddleOptionName =  entryStraddleOptionNames[1];
										peGreekWhenJodiFormed = getOptionGreeks(peStraddleOptionName);
										fileLogTelegramWriter.write( "ReEntering ="+peStraddleOptionName +"(@"+peGreekWhenJodiFormed.getLtp()+")");
										peDbId = createAlgoSellOrder(peStraddleOptionName, peGreekWhenJodiFormed.getLtp(), noOfLots*lotSize);
										if (this.placeActualOrder) {
											placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
										}
									} else {
										prepareExit("Too many orders");
									}
								}
							}
						}
					}
				}
				lastKnownTrend = volatilityDirection;
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
	
	private String getVolatalityDirection(String lastKnownTrend) {
		String retVal = lastKnownTrend;
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			if (this.avoidOutlier) {
				String fetchSql = "select countceoutlier, countpeoutlier from nexcorio_option_atm_movement_data where f_main_instrument = " + this.mainInstrument.getId() + ""
						+ " and record_time <= '" + postgresLongDateFormat.format(getCurrentTime()) + "'"
						+ " order by record_time desc limit 5";
				fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
				ResultSet rs = stmt.executeQuery(fetchSql);
				
				int ceCount = 0;
				int peCount = 0;
				while (rs.next()) {
					float ceGreek = rs.getFloat("countceoutlier");
					float peGreek = rs.getFloat("countpeoutlier");
					
					fileLogTelegramWriter.write("ceGreek="+ceGreek+" peGreek="+peGreek);
					
					if (ceGreek > 10) ceCount++;
					if (peGreek > 8) peCount++;
				}
				rs.close();
				
				if ( ceCount >= 5 || peCount >= 5) {
					retVal = "Avoid";
					return retVal;
				}
			}
			
			String fetchSql = "select min(celtp+peltp) as minValue, max(celtp+peltp) as maxValue  from nexcorio_option_atm_movement_data where f_main_instrument = " + mainInstrument.getId() + ""
					+ " and record_time >= '" + postgresShortDateFormat.format(getCurrentTime()) + " 09:20:00'"
					//+ " and record_time >= '" + postgresLongDateFormat.format(getCurrentTime(-120)) + "'"
					+ " and record_time <= '" + postgresLongDateFormat.format(getCurrentTime()) + "'";
			fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
			ResultSet rs = stmt.executeQuery(fetchSql);
			
			float minValue = 0f;
			float maxValue = 0f;
			while (rs.next()) {
				minValue = rs.getFloat("minValue");
				maxValue = rs.getFloat("maxValue");
			}
			rs.close();
			
			fetchSql = "select celtp+peltp as curPremium, future_outstanding_volume from nexcorio_option_atm_movement_data where f_main_instrument = " + mainInstrument.getId() + ""
					+ " and record_time <= '" + postgresLongDateFormat.format(getCurrentTime()) + "' order by record_time desc limit 1";
			fileLogTelegramWriter.write("2. fetchSql="+fetchSql);
			
			rs = stmt.executeQuery(fetchSql);
			float curValue = 0f;
			float futureOutstandingVolume = 0f;
			while (rs.next()) {
				curValue = rs.getFloat("curPremium");
				futureOutstandingVolume = rs.getFloat("future_outstanding_volume");
			}
			rs.close();
			
			
			float exitAt = maxValue - (maxValue-minValue)/3f;
			float entryAt = maxValue - (maxValue-minValue)*1f/2f;
			
			fileLogTelegramWriter.write("indexAt920="+indexAt920+" instrumentLtp="+instrumentLtp+" maxValue="+maxValue+" minValue="+minValue+" range="+(maxValue-minValue)+" curValue="+curValue+" exitAt="+exitAt+" entryAt="+entryAt);
			
			if (maxValue-minValue > 5f && curValue > exitAt) {
				//if (futureOutstandingVolume < 0) retVal="CE"; // 
				if (this.indexAt920 >= this.instrumentLtp) retVal="CE";
				else retVal="PE";
			} else if (curValue < entryAt) {
				retVal = "Neutral";
			}
			stmt.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		return retVal;
	}
	
	private boolean isIdealEntry() {
		boolean retVal = false;
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			String fetchSql = "select min(celtp+peltp) as minValue, max(celtp+peltp) as maxValue from nexcorio_option_atm_movement_data where f_main_instrument = " + mainInstrument.getId() + ""
					+ " and record_time >= '" + postgresShortDateFormat.format(getCurrentTime()) + " 09:20:00'"
					//+ " and record_time >= '" + postgresLongDateFormat.format(getCurrentTime(-120)) + "'"
					+ " and record_time <= '" + postgresLongDateFormat.format(getCurrentTime()) + "'";
			fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
			ResultSet rs = stmt.executeQuery(fetchSql);
			
			float minValue = 0f;
			float maxValue = 0f;
			while (rs.next()) {
				minValue = rs.getFloat("minValue");
				maxValue = rs.getFloat("maxValue");
			}
			rs.close();
			
			fetchSql = "select celtp+peltp as curPremium from nexcorio_option_atm_movement_data where f_main_instrument = " + mainInstrument.getId() + ""
					+ " and record_time <= '" + postgresLongDateFormat.format(getCurrentTime()) + "' order by record_time desc limit 1";
			fileLogTelegramWriter.write("2. fetchSql="+fetchSql);
			
			rs = stmt.executeQuery(fetchSql);
			float curValue = 0f;
			while (rs.next()) {
				curValue = rs.getFloat("curPremium");
			}
			rs.close();
			
			if (maxValue-minValue > 10f && curValue < (maxValue+minValue)/2f) {
				retVal = true;
			}
			stmt.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		return retVal;
	}
	
}
