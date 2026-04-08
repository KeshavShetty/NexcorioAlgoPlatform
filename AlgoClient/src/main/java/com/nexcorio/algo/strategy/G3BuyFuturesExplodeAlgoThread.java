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

import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.util.KiteUtil;
import com.nexcorio.algo.util.db.HDataSource;

public class G3BuyFuturesExplodeAlgoThread extends G3BaseClass implements Runnable{

	private static final Logger log = LogManager.getLogger(G3BuyFuturesExplodeAlgoThread.class);
		
	public float baseDelta = 0.5f;	
	public float volumeCutoff = 50000f;
	public int holdingMinute = 15;
	public float tgtPerOrder = 50f;
	public float slPerOrder = -50f;
	
	
	private String ceBuyOptionname = "";
	private String peBuyOptionname = "";
	
	public G3BuyFuturesExplodeAlgoThread(Long napAlgoId, String backTestDateStr) {
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
			
			String lastKnownTrend = "Unknown";
			Date lastOrderAt = getCurrentTime();
			float ceBoughtAt = 0f;
			float peBoughtAt = 0f;
			do {
				sleep(5); // Quick to react
				
				this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
				
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
				fileLogTelegramWriter.write( " instrumentLtp=" + this.instrumentLtp +" currentProfit="+currentProfitPerUnit+" maxLowestpointReachedPerUnit="+(maxLowestpointReached)+" maxTrailingProfit="+maxTrailingProfit);
				
				String currentTrend = getBuyerTrendByFuturesVolumeExplode();
				
				if (currentTrend.equals("CE")) {
					if (!peBuyOptionname.equals("")) { // Exit PE if any
						fileLogTelegramWriter.write( " Exiting Long ="+peBuyOptionname );
						if (this.placeActualOrder) {
							placeRealOrder(peDbId, peBuyOptionname, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						updateCurrentOrderStatus(peBuyOptionname, peDbId, "LegClosed");
						peBuyOptionname = "";
					}
					if (ceBuyOptionname.equals("")) {
						String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised( baseDelta, this.hedgeDistance);
						ceBuyOptionname = entryStraddleOptionNames[0];
						ceOptionGreeks = getOptionGreeks(ceBuyOptionname);
						fileLogTelegramWriter.write( " Entering Long ="+ceBuyOptionname +"@" +ceOptionGreeks.getLtp() );
						ceDbId = createAlgoBuyOrder(ceBuyOptionname, ceOptionGreeks.getLtp(), noOfLots*lotSize);
						if (this.placeActualOrder) {
							placeRealOrder(ceDbId, ceBuyOptionname, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						ceBoughtAt = ceOptionGreeks.getLtp() ;
						lastOrderAt = getCurrentTime();
					}
				} else if (currentTrend.equals("PE")) {
					if (!ceBuyOptionname.equals("")) {
						fileLogTelegramWriter.write( " Exiting Long ="+ceBuyOptionname );
						if (this.placeActualOrder) {
							placeRealOrder( ceDbId, ceBuyOptionname, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						updateCurrentOrderStatus(ceBuyOptionname, ceDbId, "LegClosed");
						ceBuyOptionname = "";
					}
					if (peBuyOptionname.equals("")) {
						String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised( baseDelta, this.hedgeDistance);
						peBuyOptionname = entryStraddleOptionNames[1];
						peOptionGreeks = getOptionGreeks(peBuyOptionname);
						fileLogTelegramWriter.write( " Entering Long ="+peBuyOptionname +"@" +peOptionGreeks.getLtp() );
						peDbId = createAlgoBuyOrder(peBuyOptionname, peOptionGreeks.getLtp(), noOfLots*lotSize);
						if (this.placeActualOrder) {
							placeRealOrder(peDbId, peBuyOptionname, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						peBoughtAt = peOptionGreeks.getLtp() ;
						lastOrderAt = getCurrentTime();
					}
				}
				// Check exit for existing positions
				if (!ceBuyOptionname.equals("") || !peBuyOptionname.equals("")) {
					
					float gain = !ceBuyOptionname.equals("")?ceOptionGreeks.getLtp()-ceBoughtAt : peOptionGreeks.getLtp()-peBoughtAt;
					if (getTimeDiffMinute(getCurrentTime(), lastOrderAt) > this.holdingMinute
							|| gain > this.tgtPerOrder
							|| gain < this.slPerOrder) {
						if (!ceBuyOptionname.equals("")) {
							fileLogTelegramWriter.write( " Exiting Long ="+ceBuyOptionname );
							if (this.placeActualOrder) {
								placeRealOrder( ceDbId, ceBuyOptionname, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
							updateCurrentOrderStatus(ceBuyOptionname, ceDbId, "LegClosed");
							ceBuyOptionname = "";
						}
						if (!peBuyOptionname.equals("")) { // Exit PE if any
							fileLogTelegramWriter.write( " Exiting Long ="+peBuyOptionname );
							if (this.placeActualOrder) {
								placeRealOrder(peDbId, peBuyOptionname, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
							updateCurrentOrderStatus(peBuyOptionname, peDbId, "LegClosed");
							peBuyOptionname = "";
						}
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
	
	private String getBuyerTrendByFuturesVolumeExplode() {
		String retVal = "Normal";
		
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			String fetchSql = "select future_outstanding_volume from nexcorio_option_atm_movement_data where f_main_instrument = " + mainInstrument.getId() + ""
					+ " and record_time <= '" + postgresLongDateFormat.format(getCurrentTime()) + "' order by record_time desc limit 1";
			fileLogTelegramWriter.write("2. fetchSql="+fetchSql);
			
			ResultSet rs = stmt.executeQuery(fetchSql);
			
			float currentFuture_outstanding_volume = 0f;
			while (rs.next()) {
				currentFuture_outstanding_volume = rs.getFloat("future_outstanding_volume");
			}
			rs.close();
			
			fetchSql = "select future_outstanding_volume from nexcorio_option_atm_movement_data where f_main_instrument = " + mainInstrument.getId() + ""
					+ " and record_time <= '" + postgresLongDateFormat.format(getCurrentTime(-1)) + "' order by record_time desc limit 1";
			fileLogTelegramWriter.write("2. fetchSql="+fetchSql);
			
			 rs = stmt.executeQuery(fetchSql);
			
			float prevtFuture_outstanding_volume = 0f;
			while (rs.next()) {
				prevtFuture_outstanding_volume = rs.getFloat("future_outstanding_volume");
			}
			rs.close();
			stmt.close();
			
			float changeInVolume = currentFuture_outstanding_volume-prevtFuture_outstanding_volume;
			fileLogTelegramWriter.write("currentFuture_outstanding_volume="+currentFuture_outstanding_volume + " prevtFuture_outstanding_volume="+prevtFuture_outstanding_volume + " change="+ changeInVolume );
			
			int indexReminder = (int) this.instrumentLtp;
			indexReminder = indexReminder%100;
			
			if (indexReminder > 80 || indexReminder < 35) {
				if (changeInVolume > this.volumeCutoff) {
					retVal = "CE";
				} else if (changeInVolume < -this.volumeCutoff) {
					retVal = "PE";
				}
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
		
		new G3BuyFuturesExplodeAlgoThread(174L, "2025-05-28 09:25:00" );
	
	}
}
