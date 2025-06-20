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

public class G3BuySpikeFollowGammaDirectionAlgoThread extends G3BaseClass implements Runnable{

	private static final Logger log = LogManager.getLogger(G3BuySpikeFollowGammaDirectionAlgoThread.class);
		
	public float baseDelta = 0.5f;
	public float premiumSpikePercent = 5f;
	
	public float greekDiffCutoffPercent = 5f;
	
	private String ceBuyOptionname = "";
	private String peBuyOptionname = "";
	
	public G3BuySpikeFollowGammaDirectionAlgoThread(Long napAlgoId, String backTestDateStr) {
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
				
				fileLogTelegramWriter.write("lowestATMStraddlePremium="+ lowestATMStraddlePremium+" highestATMStraddlePremium="+highestATMStraddlePremium+" Entry at "
						+ (highestATMStraddlePremium*(100f - premiumSpikePercent)/100f) + " Exit at " + ( lowestATMStraddlePremium*(100f + premiumSpikePercent)/100f) );  
				
				float currentATMStraddlePremium = getATMStraddlePremium();
				
				if (!ceBuyOptionname.equals("") || !peBuyOptionname.equals("")) { // Buy position exist
					if (currentATMStraddlePremium < highestATMStraddlePremium*(100f - premiumSpikePercent)/100f) {
						
						if (!ceBuyOptionname.equals("")) {
							fileLogTelegramWriter.write( " Exiting Long ="+ceBuyOptionname );
							if (this.placeActualOrder) {
								placeRealOrder( ceDbId, ceBuyOptionname, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
							updateCurrentOrderStatus(ceBuyOptionname, ceDbId, "LegClosed");
							ceBuyOptionname = "";
						}
						if (!peBuyOptionname.equals("")) {
							fileLogTelegramWriter.write( " Exiting Long ="+peBuyOptionname );
							if (this.placeActualOrder) {
								placeRealOrder(peDbId, peBuyOptionname, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
							updateCurrentOrderStatus(peBuyOptionname, peDbId, "LegClosed");
							peBuyOptionname = "";
						}
						highestATMStraddlePremium = currentATMStraddlePremium;
						lowestATMStraddlePremium  = currentATMStraddlePremium;
						
						if (this.noOfOrders >= maxAllowedNoOfOrders) {
							prepareExit("Too many orders");
						}
					} else { // position exist check for change in direction
						String currentTrend = getBuyerTrendByGamma();
						if( (currentTrend.equals("CE") && !peBuyOptionname.equals(""))
								|| (currentTrend.equals("PE") && !ceBuyOptionname.equals("")) ) { // Changed buyer direction, exit existig position and enter current direction
							if (!ceBuyOptionname.equals("")) {
								fileLogTelegramWriter.write( " Exiting Long ="+ceBuyOptionname );
								if (this.placeActualOrder) {
									placeRealOrder( ceDbId, ceBuyOptionname, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
								}
								updateCurrentOrderStatus(ceBuyOptionname, ceDbId, "LegClosed");
								ceBuyOptionname = "";
							}
							if (!peBuyOptionname.equals("")) {
								fileLogTelegramWriter.write( " Exiting Long ="+peBuyOptionname );
								if (this.placeActualOrder) {
									placeRealOrder(peDbId, peBuyOptionname, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
								}
								updateCurrentOrderStatus(peBuyOptionname, peDbId, "LegClosed");
								peBuyOptionname = "";
							}
							String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised( baseDelta, this.hedgeDistance);
							if (currentTrend.equals("CE")) {
								ceBuyOptionname = entryStraddleOptionNames[0];
								float cePrice = getPriceFromTicks(ceBuyOptionname);
								fileLogTelegramWriter.write( " Entering Long ="+ceBuyOptionname +"@" +cePrice );
								ceDbId = createAlgoBuyOrder(ceBuyOptionname, cePrice, noOfLots*lotSize);
								if (this.placeActualOrder) {
									placeRealOrder(ceDbId, ceBuyOptionname, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
								}
							} else { // PE
								peBuyOptionname = entryStraddleOptionNames[1];
								float pePrice = getPriceFromTicks(peBuyOptionname);
								fileLogTelegramWriter.write( " Entering Long ="+peBuyOptionname +"@" +pePrice );
								peDbId = createAlgoBuyOrder(peBuyOptionname, pePrice, noOfLots*lotSize);
								if (this.placeActualOrder) {
									placeRealOrder(peDbId, peBuyOptionname, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
								}
							}
						}
					}
				} else { // No position exist
					if (currentATMStraddlePremium > lowestATMStraddlePremium*(100f + premiumSpikePercent)/100f) { // Spike, need re-eneter long position
						
						String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised( baseDelta, this.hedgeDistance);
						
						String currentTrend = getBuyerTrendByGamma();	
						if (currentTrend.equals("CE")) {
							ceBuyOptionname = entryStraddleOptionNames[0];
							float cePrice = getPriceFromTicks(ceBuyOptionname);
							fileLogTelegramWriter.write( " Entering Long ="+ceBuyOptionname +"@" +cePrice );
							ceDbId = createAlgoBuyOrder(ceBuyOptionname, cePrice, noOfLots*lotSize);
							if (this.placeActualOrder) {
								placeRealOrder(ceDbId, ceBuyOptionname, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
						} else { // PE
							peBuyOptionname = entryStraddleOptionNames[1];
							float pePrice = getPriceFromTicks(peBuyOptionname);
							fileLogTelegramWriter.write( " Entering Long ="+peBuyOptionname +"@" +pePrice );
							peDbId = createAlgoBuyOrder(peBuyOptionname, pePrice, noOfLots*lotSize);
							if (this.placeActualOrder) {
								placeRealOrder(peDbId, peBuyOptionname, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
						}
						highestATMStraddlePremium = currentATMStraddlePremium;
						lowestATMStraddlePremium  = currentATMStraddlePremium;
					}		
				}
						
				if (currentATMStraddlePremium > highestATMStraddlePremium) highestATMStraddlePremium = currentATMStraddlePremium;
				if (currentATMStraddlePremium < lowestATMStraddlePremium)  lowestATMStraddlePremium  = currentATMStraddlePremium;
				
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
	
	private String getBuyerTrendByGamma() {
		String retVal = "";
		
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			String fieldname = "cegamma as ceGreek, pegamma as peGreek";
			
			String fetchSql = "select " + fieldname + " from nexcorio_option_atm_movement_data where f_main_instrument = " + this.mainInstrument.getId() + ""
					+ " and record_time <= '" + postgresLongDateFormat.format(getCurrentTime()) + "'"
					+ " order by record_time desc limit 1";
			fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
			ResultSet rs = stmt.executeQuery(fetchSql);
			
			
			float ceGreek = 0f;
			float peGreek = 0f;
			while (rs.next()) {
				ceGreek = rs.getFloat("ceGreek");
				peGreek = rs.getFloat("peGreek");
			}
			rs.close();			
			stmt.close();
			
			float greekDiffPercent = getPercentDiff(ceGreek, peGreek);
			//if (ceGreek < peGreek) greekDiffPercent = -greekDiffPercent;
			
			fileLogTelegramWriter.write("ceGreek="+ceGreek + " peGreek="+peGreek + " greekDiffPercent="+greekDiffPercent);
			
			if (greekDiffPercent >= greekDiffCutoffPercent || greekDiffPercent <= -greekDiffCutoffPercent ) {
				fileLogTelegramWriter.write("Cutoff breached");
				if (greekDiffPercent > 0f) {
					retVal = "CE";
				} else {
					retVal = "PE";
				}
			}
			
			if (retVal.equals("Unknown") ) {
				if (greekDiffPercent > 0f) {
					retVal = "CE";
				} else {
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
		
		new G3BuySpikeFollowGammaDirectionAlgoThread(525L, "2025-03-06 09:50:00" );
	
	}
}
