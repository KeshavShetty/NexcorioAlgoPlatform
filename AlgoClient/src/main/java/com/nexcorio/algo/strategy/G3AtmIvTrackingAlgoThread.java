package com.nexcorio.algo.strategy;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.backtest.TriggerAlgo;
import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.kite.CaffeineCache;
import com.nexcorio.algo.util.KiteUtil;
import com.nexcorio.algo.util.db.HDataSource;

public class G3AtmIvTrackingAlgoThread extends G3BaseClass implements Runnable{

	private static final Logger log = LogManager.getLogger(G3AtmIvTrackingAlgoThread.class);
		
	public float baseDelta = 0.5f;
	
	public float greekDiff = 0.5f;
	
	private String ceBuyOptionname = "";
	private String peBuyOptionname = "";
	
	public G3AtmIvTrackingAlgoThread(Long napAlgoId, String backTestDateStr) {
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
			
			float buyPrice = 0f;
			Date lastOrderAt = getCurrentTime();
			
			float targetPts = 25f;
			float stoplossPts = 25f;
			int maxTimePerOrder = 3; //minute
			
			do {
				sleep(5); // Quick to react
				
				this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
				fileLogTelegramWriter.write("instrumentLtp="+instrumentLtp);
				
				OptionGreek ceOptionGreeks = getOptionGreeks(ceBuyOptionname);
				OptionGreek peOptionGreeks = getOptionGreeks(peBuyOptionname);
				print(ceOptionGreeks, peOptionGreeks);
				
				float runningCePrice = ceOptionGreeks==null?0: ceOptionGreeks.getLtp();
				float runningPePrice = peOptionGreeks==null?0: peOptionGreeks.getLtp();
				
				if (!ceBuyOptionname.equals("")) updateCurrentOrderSellPrice(ceBuyOptionname, ceDbId, runningCePrice);
				if (!peBuyOptionname.equals("")) updateCurrentOrderSellPrice(peBuyOptionname, peDbId, runningPePrice);
				
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
				
				
				// Check exit plans
				if (!ceBuyOptionname.equals("")) { // CE buy running
					if (runningCePrice > buyPrice+targetPts || runningCePrice < buyPrice-stoplossPts || getTimeDiffMinute(getCurrentTime(), lastOrderAt) > maxTimePerOrder) {
						
						fileLogTelegramWriter.write( " Exiting="+ceBuyOptionname + " because " + (buyPrice+targetPts) + " " + (runningCePrice < buyPrice-stoplossPts) + " " + getTimeDiffMinute(getCurrentTime(), lastOrderAt));
						if (this.placeActualOrder) {
							placeRealOrder(ceDbId, ceBuyOptionname, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						ceBuyOptionname = "";
					}
				}
				if (!peBuyOptionname.equals("")) { // PE buy running
					if (runningPePrice > buyPrice+targetPts || runningPePrice < buyPrice-stoplossPts || getTimeDiffMinute(getCurrentTime(), lastOrderAt) > maxTimePerOrder) {
						fileLogTelegramWriter.write( " Exiting="+peBuyOptionname + " because " + (buyPrice+targetPts) + " " + (runningPePrice < buyPrice-stoplossPts) + " " + getTimeDiffMinute(getCurrentTime(), lastOrderAt));
						
						if (this.placeActualOrder) {
							placeRealOrder(peDbId, peBuyOptionname, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						peBuyOptionname = "";
					}
				}
				
				String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(this.baseDelta, this.optimalHedgeDistance);
				
				ceOptionGreeks = getOptionGreeks(entryStraddleOptionNames[0]);
				peOptionGreeks = getOptionGreeks(entryStraddleOptionNames[1]);
				print(ceOptionGreeks, peOptionGreeks);
				
				fileLogTelegramWriter.write("Past Greeks");
				
				OptionGreek cePrevOptionGreeks = getPastOptionGreeks(entryStraddleOptionNames[0], -1);
				OptionGreek pePrevOptionGreeks = getPastOptionGreeks(entryStraddleOptionNames[1], -1);
				print(cePrevOptionGreeks, pePrevOptionGreeks);
				
				float ceDiff = ceOptionGreeks.getIv() - cePrevOptionGreeks.getIv();
				float peDiff = peOptionGreeks.getIv() - pePrevOptionGreeks.getIv();
				
				fileLogTelegramWriter.write("ceDiff="+ceDiff+" peDiff="+peDiff);
				
				String buyOptionType = "PE";
				if ( Math.abs(peOptionGreeks.getDelta()) > Math.abs(pePrevOptionGreeks.getDelta())) buyOptionType = "PE";
				if ( Math.abs(ceOptionGreeks.getDelta()) > Math.abs(cePrevOptionGreeks.getDelta())) buyOptionType = "CE";
				
				boolean signalDetected = false;
				
				if ( (ceDiff > greekDiff && peDiff > -0.05f) || (peDiff > greekDiff && ceDiff > -0.05f) ) {
					signalDetected = true;
					fileLogTelegramWriter.write("1.0 Signal found, buyOptionType "+buyOptionType);
				}
				if ( (ceDiff > 2f*greekDiff) || (peDiff > 2f*greekDiff) ) {
					signalDetected = true;
					fileLogTelegramWriter.write("2.0 Signal found, buyOptionType "+buyOptionType);
				}				
				if ( Math.abs(ceDiff) + Math.abs(peDiff) > 2.0f ) {
					signalDetected = true;
					fileLogTelegramWriter.write("3.0 Signal found, buyOptionType "+buyOptionType);
				}
								
				if (signalDetected) {
					if (buyOptionType.equals("CE") && ceBuyOptionname.equals("")) {
						if (!peBuyOptionname.equals("")) {
							fileLogTelegramWriter.write( " Exiting="+peBuyOptionname);
							if (this.placeActualOrder) {
								placeRealOrder(peDbId, peBuyOptionname, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
							peBuyOptionname = "";
						}
						if (this.noOfOrders<maxAllowedNoOfOrders) {
							ceBuyOptionname = entryStraddleOptionNames[0];
							float cePrice = getPriceFromTicks(ceBuyOptionname);
							fileLogTelegramWriter.write( " Entering Long ="+ceBuyOptionname +"@" +cePrice );
							ceDbId = createAlgoBuyOrder(ceBuyOptionname, cePrice, noOfLots*lotSize);
							if (this.placeActualOrder) {
								placeRealOrder(ceDbId, ceBuyOptionname, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
							buyPrice = cePrice;
							lastOrderAt = getCurrentTime();
						} else {
							prepareExit("Too many orders");
						}
					} else if (buyOptionType.equals("PE") && peBuyOptionname.equals("")) {
						if (!ceBuyOptionname.equals("")) {
							fileLogTelegramWriter.write( " Exiting="+ceBuyOptionname);
							if (this.placeActualOrder) {
								placeRealOrder(ceDbId, ceBuyOptionname, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
							ceBuyOptionname = "";
						}
						if (this.noOfOrders<maxAllowedNoOfOrders) {
							peBuyOptionname = entryStraddleOptionNames[1];
							float pePrice = getPriceFromTicks(peBuyOptionname);
							fileLogTelegramWriter.write( " Entering Long ="+peBuyOptionname +"@" +pePrice );
							peDbId = createAlgoBuyOrder(peBuyOptionname, pePrice, noOfLots*lotSize);
							if (this.placeActualOrder) {
								placeRealOrder(peDbId, peBuyOptionname, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							} 
							buyPrice = pePrice;
							lastOrderAt = getCurrentTime();
						} else {
							prepareExit("Too many orders");
						}
					}
				}
				
				checkExitSignals();
				saveAlgoDailySummary(currentProfitPerUnit, maxProfitReached, maxProfitReachedAt, maxLowestpointReached, maxLowestpointReachedAt, maxTrailingProfit);
			} while(!exitThread);
			// exit all positions
			if (this.placeActualOrder) {
				
				if (!ceBuyOptionname.equals("")) {
					fileLogTelegramWriter.write( " Exiting Long ="+ceBuyOptionname );
					placeRealOrder( ceDbId, ceBuyOptionname, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
					updateCurrentOrderStatus(ceBuyOptionname, ceDbId, "LegClosed");
					ceBuyOptionname = "";
				}
				if (!peBuyOptionname.equals("")) {
					fileLogTelegramWriter.write( " Exiting Long ="+peBuyOptionname );
					placeRealOrder(peDbId, peBuyOptionname, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
					updateCurrentOrderStatus(peBuyOptionname, peDbId, "LegClosed");
					peBuyOptionname = "";
				}
			}
			fileLogTelegramWriter.write( " noOfOrders="+noOfOrders + " ROI=" + (currentProfitPerUnit*this.lotSize*100f)/requiredMargin + "% (Max profit reached to "+ (maxProfitReached) +"@" + maxProfitReachedAt+ "\n and Lowest reached to " + (maxLowestpointReached) + "@" + maxLowestpointReachedAt + ")");
		} catch (Exception e) {			
			updateAlgoStatus("Error");
			log.error("Error"+e.getMessage(), e);
			fileLogTelegramWriter.write("Error " + ExceptionUtils.getStackTrace(e));
		} finally {
			fileLogTelegramWriter.close();
		}
	}

	private OptionGreek getPastOptionGreeks(String optionName, int minutes) {
		
		if (optionName==null || optionName.equals("")) return null;
		
		OptionGreek retVal = null;
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			String fetchSql = "select iv, delta, vega, theta, gamma, ltp, oi from nexcorio_option_greeks  where trading_symbol = '" + optionName + "'"
					+ ( backtestDate!=null ? ( " and quote_time <='" + postgresLongDateFormat.format(getCurrentTime(-1))+ "'") : "" )
					+ " and f_main_instrument= " + mainInstrument.getId()
					+ " order by quote_time desc limit 1";
			//fileLogTelegramWriter.write("In getOptionGreeks fetchSql="+fetchSql);
			ResultSet rs = stmt.executeQuery(fetchSql);
			while (rs.next()) {
				retVal = new OptionGreek(optionName, rs.getFloat("iv"), rs.getFloat("delta"), rs.getFloat("vega"), rs.getFloat("theta"), rs.getFloat("gamma"), rs.getFloat("ltp"), rs.getFloat("oi"));
			}
			rs.close();
			stmt.close();
			//System.out.println("retVal="+retVal);
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error"+e.getMessage(),e);
		} finally {
			try {
				if (conn!=null) conn.close();
			} catch (SQLException e) {
				log.error(e);
			}
		}
		return retVal;
	}
	
	private String getBuyerTrendByIVChange() {
		String retVal = "";
		
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			String fetchSql = "select totalceiv, totalpeiv from nexcorio_option_atm_movement_data where f_main_instrument = " + this.mainInstrument.getId() + ""
					+ " and record_time <= '" + postgresLongDateFormat.format(getCurrentTime()) + "'"
					+ " order by record_time desc limit 1";
			fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
			
			ResultSet rs = stmt.executeQuery(fetchSql);
			
			float totalCEIVNow = 0f;
			float totalPEIVNow = 0f;
			while (rs.next()) {
				totalCEIVNow = rs.getFloat("totalceiv");
				totalPEIVNow = rs.getFloat("totalpeiv");
			}
			rs.close();
			
			fetchSql = "select totalceiv, totalpeiv from nexcorio_option_atm_movement_data where f_main_instrument = " + this.mainInstrument.getId() + ""
					+ " and record_time <= '" + postgresLongDateFormat.format(getCurrentTime(-5)) + "'"
					+ " order by record_time desc limit 1";
			fileLogTelegramWriter.write("2. fetchSql="+fetchSql);
			
			rs = stmt.executeQuery(fetchSql);
			
			float totalCEIVThen = 0f;
			float totalPEIVThen = 0f;
			while (rs.next()) {
				totalCEIVThen = rs.getFloat("totalceiv");
				totalPEIVThen = rs.getFloat("totalpeiv");
			}
			rs.close();
			stmt.close();
			
			float changeinCEIV = (totalCEIVNow - totalCEIVThen)*100f/totalCEIVThen;
			float changeinPEIV = (totalPEIVNow - totalPEIVThen)*100f/totalPEIVThen;
			
			fileLogTelegramWriter.write("totalCEIVNow="+totalCEIVNow + " totalCEIVThen="+totalCEIVThen + " changeinCEIV="+changeinCEIV);
			fileLogTelegramWriter.write("totalPEIVNow="+totalPEIVNow + " totalPEIVThen="+totalPEIVThen + " changeinPEIV="+changeinPEIV);
			
			if (Math.abs(changeinCEIV) > Math.abs(changeinPEIV)) {
				if (changeinCEIV>0) retVal = "CE";
				else retVal = "PE";
			} else {
				if (changeinPEIV>0) retVal = "PE";
				else retVal = "CE";
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
	
	
	public static void main(String[] args) {
		
		
	
	}
}
