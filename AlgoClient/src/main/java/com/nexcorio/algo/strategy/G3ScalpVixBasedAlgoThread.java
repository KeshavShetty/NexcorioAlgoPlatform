package com.nexcorio.algo.strategy;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.kite.CaffeineCache;
import com.nexcorio.algo.util.KiteUtil;
import com.nexcorio.algo.util.db.HDataSource;

public class G3ScalpVixBasedAlgoThread extends G3BaseClass implements Runnable{

	private static final Logger log = LogManager.getLogger(G3ScalpVixBasedAlgoThread.class);
		
	public float baseDelta = 0.5f;
	public float targetPct = 1.25f;
	
	private String ceBuyOptionname = "";
	private String peBuyOptionname = "";
	
	public G3ScalpVixBasedAlgoThread(Long napAlgoId, String backTestDateStr) {
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
			
			float openingVix = getOpeningVix();
			fileLogTelegramWriter.write( "openingVix="+openingVix);
			
			float ceBoughtAt = 0f;
			float peBoughtAt = 0f;
			boolean justExited = false;
			Date lastOrderAt = getCurrentTime();
			do {
				sleep(5); // Quick to react
				
				this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
				float currentVix = getPriceFromTicks("VIX");
				
				OptionGreek ceOptionGreeks = getOptionGreeks(ceBuyOptionname);
				OptionGreek peOptionGreeks = getOptionGreeks(peBuyOptionname );
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
				fileLogTelegramWriter.write( " instrumentLtp=" + this.instrumentLtp +" currentVix="+currentVix+" currentProfit="+currentProfitPerUnit+" maxLowestpointReachedPerUnit="+(maxLowestpointReached)+" maxTrailingProfit="+maxTrailingProfit);
				
				String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised( baseDelta, this.hedgeDistance);
				String buyerChance = getBuyingOpportunity(entryStraddleOptionNames[0], entryStraddleOptionNames[1]);
				
				if (buyerChance.equals("CE") && ceBuyOptionname.equals("")) {
					if (!peBuyOptionname.equals("")) {
						fileLogTelegramWriter.write( " Exiting="+peBuyOptionname);
						if (this.placeActualOrder) {
							placeRealOrder(peDbId, peBuyOptionname, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						peBuyOptionname = "";
					}
					ceBuyOptionname = entryStraddleOptionNames[0];
					float cePrice = getPriceFromTicks(ceBuyOptionname);
					fileLogTelegramWriter.write( " Entering Long ="+ceBuyOptionname +"@" +cePrice );
					ceDbId = createAlgoBuyOrder(ceBuyOptionname, cePrice, noOfLots*lotSize);
					if (this.placeActualOrder) {
						placeRealOrder(ceDbId, ceBuyOptionname, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
					}
					ceBoughtAt = cePrice;
					lastOrderAt = getCurrentTime();
				} else if (buyerChance.equals("PE") && peBuyOptionname.equals("")) {
					if (!ceBuyOptionname.equals("")) {
						fileLogTelegramWriter.write( " Exiting="+ceBuyOptionname);
						if (this.placeActualOrder) {
							placeRealOrder(ceDbId, ceBuyOptionname, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						ceBuyOptionname = "";
					}
					peBuyOptionname = entryStraddleOptionNames[1];
					float pePrice = getPriceFromTicks(peBuyOptionname);
					fileLogTelegramWriter.write( " Entering Long ="+peBuyOptionname +"@" +pePrice );
					peDbId = createAlgoBuyOrder(peBuyOptionname, pePrice, noOfLots*lotSize);
					if (this.placeActualOrder) {
						placeRealOrder(peDbId, peBuyOptionname, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
					}
					peBoughtAt = pePrice;
					lastOrderAt = getCurrentTime();
				} else {					
					if (!ceBuyOptionname.equals("") || !peBuyOptionname.equals("")) { // Position exist, check for exit
						boolean exitPosition = false;
						if (!ceBuyOptionname.equals("") && runningCePrice > ceBoughtAt*targetPct) exitPosition = true;
						if (!peBuyOptionname.equals("") && runningPePrice > peBoughtAt*targetPct) exitPosition = true;
						if (!ceBuyOptionname.equals("") && runningCePrice < ceBoughtAt-15) exitPosition = true;
						if (!peBuyOptionname.equals("") && runningPePrice < peBoughtAt-15) exitPosition = true;
						if (getTimeDiffMinute(getCurrentTime(), lastOrderAt) > 30) exitPosition = true;
						if(currentVix+0.05f < openingVix) exitPosition = true;
						if (exitPosition) {
							if (!ceBuyOptionname.equals("")) {
								fileLogTelegramWriter.write( " Exiting="+ceBuyOptionname);
								if (this.placeActualOrder) {
									placeRealOrder(ceDbId, ceBuyOptionname, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
								}
								ceBuyOptionname = "";
							}
							if (!peBuyOptionname.equals("")) {
								fileLogTelegramWriter.write( " Exiting="+peBuyOptionname);
								if (this.placeActualOrder) {
									placeRealOrder(peDbId, peBuyOptionname, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
								}
								peBuyOptionname = "";
							}
						}
						sleep(60);
						justExited = true;
					} 
				}
				
				checkExitSignals();
				
				if (exitThread==true) {
					if (!ceBuyOptionname.equals("")) {
						updateCurrentOrderStatus(ceBuyOptionname, ceDbId, "LegClosed");
					} 
					if (!peBuyOptionname.equals("")) {
						updateCurrentOrderStatus(peBuyOptionname, peDbId, "LegClosed");
					}
				}
				saveAlgoDailySummary(currentProfitPerUnit, maxProfitReached, maxProfitReachedAt, maxLowestpointReached, maxLowestpointReachedAt, maxTrailingProfit);
			} while(!exitThread);
			updateAlgoStatus("Terminated");
			String logString = "Exiting Strddle ceBuyOptionname="+ceBuyOptionname + " peBuyOptionname="+peBuyOptionname; 
			log.info(logString);
			fileLogTelegramWriter.write( " " + logString);
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
	
	private String getBuyingOpportunity(String ceOptionname, String peOptionname) {
		String retStr = "UNSTABLE";
		
		try {
			
			OptionGreek ceCurrentGreek = getOptionGreeks(ceOptionname);
			OptionGreek peCurrentGreek = getOptionGreeks(peOptionname);
			
			OptionGreek cePastCurrentGreek = getPastOptionGreeks(ceOptionname);
			OptionGreek pePastCurrentGreek = getPastOptionGreeks(peOptionname);
			
			float ceIvChange = ceCurrentGreek.getIv() - cePastCurrentGreek.getIv();
			float peIvChange = peCurrentGreek.getIv() - pePastCurrentGreek.getIv();
			
			//if (Math.abs(ceIvChange)>0.1f || Math.abs(peIvChange)>0.1f) {
				if(Math.abs(ceIvChange/peIvChange) > 5f || Math.abs(peIvChange/ceIvChange) > 5f ) {
					if(ceIvChange>peIvChange) retStr="PE";
					else retStr="CE";
				}
			//}
			
			fileLogTelegramWriter.write("ceIvChange="+ceIvChange+" peIvChange=" + peIvChange + " retStr="+retStr);
		} catch(Exception ex) {
			ex.printStackTrace();
		}
		return retStr;
	}
	
	private OptionGreek getPastOptionGreeks(String optionName) {
		OptionGreek retVal = null;
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			String fetchSql = "select iv, delta, vega, theta, gamma, ltp, oi from nexcorio_option_greeks  where trading_symbol = '" + optionName + "'"
					+ ( backtestDate!=null ? ( " and quote_time <='" + postgresLongDateFormat.format(getCurrentTimeDifferSeconds(-5))+ "'") : "" )
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
	
	private float getOpeningVix() {
		float retVix = 0f;
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
		
			// Vix today at 9:20
			
			String fetchSql = "SELECT max(last_traded_price) as upRange from nexcorio_tick_data where trading_symbol = 'VIX' "
					+ " AND quote_time > '" + postgresShortDateFormat.format(getCurrentTime()) + " 09:16:55' "
					+ " AND quote_time < '" + postgresShortDateFormat.format(getCurrentTime()) + " 09:20:00' ";
					
			System.out.println(" vix-"+fetchSql);
			ResultSet rs = stmt.executeQuery(fetchSql);
			while (rs.next()) {
				retVix = rs.getFloat("upRange");
			}
			rs.close();
			stmt.close();
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
		return retVix;
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
		
		new G3ScalpVixBasedAlgoThread(174L, "2025-05-28 09:25:00" );
	
	}
}
