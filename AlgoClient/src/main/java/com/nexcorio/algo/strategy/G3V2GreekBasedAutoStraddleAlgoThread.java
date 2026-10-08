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

public class G3V2GreekBasedAutoStraddleAlgoThread extends G3BaseClass implements Runnable {

	private static final Logger log = LogManager.getLogger(G3V2GreekBasedAutoStraddleAlgoThread.class);
	
	public float baseDelta = 0.5f;
	public float indexPoints = 35f;
	public float v2ExitCutOff = 2000f;
	public float v2ReEntryCutOff = 1500f;
	public String greekname = "V2OTMAccmlChangeInTheta";
	public boolean matchRunningDelta = false;
	
	public G3V2GreekBasedAutoStraddleAlgoThread(Long napAlgoId, String backTestDateStr) {
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
			
			String[] entryStraddleOptionNames = null;
			
			float indexWhenStraddleFormed = this.instrumentLtp;
			
			do {
				sleep(5); 
				
				this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
				fileLogTelegramWriter.write( " this.indexLtp="+this.instrumentLtp );
				
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
				
				float v2GreekValueDiff = getGreekValueDiff(this.greekname); // This method will return -2000 or +2000 ( -1500 or +1500 is reEntry) 
				
				if (!ceStraddleOptionName.equals("") && !peStraddleOptionName.equals("")) { // Both leg present
					if (Math.abs(v2GreekValueDiff) > v2ExitCutOff) { // One leg has to close
						if (v2GreekValueDiff > 0) { // Index raising, exit CE 
							fileLogTelegramWriter.write( " Exiting ="+ceStraddleOptionName );
							if (this.placeActualOrder) {
								placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
							ceStraddleOptionName = "";
						} else {
							fileLogTelegramWriter.write( " Exiting ="+peStraddleOptionName );
							if (this.placeActualOrder) {
								placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
							peStraddleOptionName = "";
						}
					} else if (this.instrumentLtp > indexWhenStraddleFormed + indexPoints || this.instrumentLtp < indexWhenStraddleFormed - indexPoints) { // Index moved by readjustStraddle Points
						entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(baseDelta, this.optimalHedgeDistance);
						indexWhenStraddleFormed = this.instrumentLtp;
						
						if (!ceStraddleOptionName.equals(entryStraddleOptionNames[0])) { // Exit and re enter
							fileLogTelegramWriter.write( " Exiting ="+ceStraddleOptionName );
							if (this.placeActualOrder) {
								placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
							ceStraddleOptionName = "";
						}
						if (!peStraddleOptionName.equals(entryStraddleOptionNames[1])) { // Exit and re enter
							fileLogTelegramWriter.write( " Exiting ="+peStraddleOptionName );
							if (this.placeActualOrder) {
								placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
							peStraddleOptionName = "";
						}
						
						if (ceStraddleOptionName.equals("")) {
							if (this.noOfOrders<maxAllowedNoOfOrders) {
								ceStraddleOptionName =  entryStraddleOptionNames[0];
								ceOptionGreeks = getOptionGreeks(ceStraddleOptionName);
								fileLogTelegramWriter.write("Entering" + ceStraddleOptionName + "(@" + ceOptionGreeks.getLtp() );
								ceDbId = createAlgoSellOrder(ceStraddleOptionName, ceOptionGreeks.getLtp(), noOfLots*lotSize);
								if (this.placeActualOrder) { // Place the straddle order with Kite
									if (ceHedgeOptionName.equals("")) {
										ceHedgeOptionName =  entryStraddleOptionNames[2];
										placeRealOrder(ceHedgeOptionName, noOfLots*lotSize, "BUY",  false, KiteUtil.USE_NORMAL_ORDER_FALSE);
									}
									placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
								}
							} else {
								prepareExit("Too many orders");
							}
						}
						if (peStraddleOptionName.equals("")) {
							if (this.noOfOrders<maxAllowedNoOfOrders) {
								peStraddleOptionName =  entryStraddleOptionNames[1];
								peOptionGreeks = getOptionGreeks(peStraddleOptionName);
								fileLogTelegramWriter.write("Entering" + peStraddleOptionName + "(@" + peOptionGreeks.getLtp() );
								peDbId = createAlgoSellOrder(peStraddleOptionName, peOptionGreeks.getLtp(), noOfLots*lotSize);
								if (this.placeActualOrder) { // Place the straddle order with Kite
									if (peHedgeOptionName.equals("")) {
										peHedgeOptionName =  entryStraddleOptionNames[3];
										placeRealOrder(peHedgeOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);	
									}
									placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
								}
							} else {
								prepareExit("Too many orders");
							}
						}
					}
				} else if (!ceStraddleOptionName.equals("") || !peStraddleOptionName.equals("")) { // One leg open
					if (Math.abs(v2GreekValueDiff) < v2ReEntryCutOff) { // Re-enter earlier closed leg
						if (ceStraddleOptionName.equals("")) {
							if (this.noOfOrders<maxAllowedNoOfOrders) {
								if (matchRunningDelta) {
									entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(Math.abs(peOptionGreeks.getDelta()), this.optimalHedgeDistance);
								}
								ceStraddleOptionName =  entryStraddleOptionNames[0];
								ceOptionGreeks = getOptionGreeks(ceStraddleOptionName);
								fileLogTelegramWriter.write("Reentry" + ceStraddleOptionName + "(@" + ceOptionGreeks.getLtp() );
								ceDbId = createAlgoSellOrder(ceStraddleOptionName, ceOptionGreeks.getLtp(), noOfLots*lotSize);
								if (this.placeActualOrder) { // Place the straddle order with Kite
									if (ceHedgeOptionName.equals("")) {
										ceHedgeOptionName =  entryStraddleOptionNames[2];
										placeRealOrder(ceHedgeOptionName, noOfLots*lotSize, "BUY",  false, KiteUtil.USE_NORMAL_ORDER_FALSE);
									}
									placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
								}
							} else {
								prepareExit("Too many orders");
							} 
						} else if (peStraddleOptionName.equals("")) {
							if (this.noOfOrders<maxAllowedNoOfOrders) {
								if (matchRunningDelta) {
									entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(Math.abs(ceOptionGreeks.getDelta()), this.optimalHedgeDistance);
								}
								peStraddleOptionName =  entryStraddleOptionNames[1];
								peOptionGreeks = getOptionGreeks(peStraddleOptionName);
								fileLogTelegramWriter.write("Reentry straddle peStraddleOptionName=" + peStraddleOptionName + "(@" + peOptionGreeks.getLtp() );
								peDbId = createAlgoSellOrder(peStraddleOptionName, peOptionGreeks.getLtp(), noOfLots*lotSize);
								if (this.placeActualOrder) { // Place the straddle order with Kite
									if (peHedgeOptionName.equals("")) {
										peHedgeOptionName =  entryStraddleOptionNames[3];
										placeRealOrder(peHedgeOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);	
									}
									placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
								}
							} else {
								prepareExit("Too many orders");
							}
						}
					}
				} else { // Zero legs
					entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(baseDelta, this.optimalHedgeDistance);
					indexWhenStraddleFormed = this.instrumentLtp;
					if (Math.abs(v2GreekValueDiff) > v2ExitCutOff) { // Directional from beginning
						if (v2GreekValueDiff > 0 ) { // Index going up or CE raising, Take position in PE
							peStraddleOptionName =  entryStraddleOptionNames[1];
							peOptionGreeks = getOptionGreeks(peStraddleOptionName);
							fileLogTelegramWriter.write("Partial straddle peStraddleOptionName=" + peStraddleOptionName + "(@" + peOptionGreeks.getLtp() );
							peDbId = createAlgoSellOrder(peStraddleOptionName, peOptionGreeks.getLtp(), noOfLots*lotSize);
							if (this.placeActualOrder) { // Place the straddle order with Kite
								if (peHedgeOptionName.equals("")) {
									peHedgeOptionName =  entryStraddleOptionNames[3];
									placeRealOrder(peHedgeOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);	
								}
								placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
						} else { // Index going down or PE raising, Take position in CE
							ceStraddleOptionName =  entryStraddleOptionNames[0];
							ceOptionGreeks = getOptionGreeks(ceStraddleOptionName);
							fileLogTelegramWriter.write("Partial straddle ceStraddleOptionName=" + ceStraddleOptionName + "(@" + ceOptionGreeks.getLtp() );
							ceDbId = createAlgoSellOrder(ceStraddleOptionName, ceOptionGreeks.getLtp(), noOfLots*lotSize);
							if (this.placeActualOrder) { // Place the straddle order with Kite
								if (ceHedgeOptionName.equals("")) {
									ceHedgeOptionName =  entryStraddleOptionNames[2];
									placeRealOrder(ceHedgeOptionName, noOfLots*lotSize, "BUY",  false, KiteUtil.USE_NORMAL_ORDER_FALSE);
								}
								placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
						}
					} else { // Take new straddle
						entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(baseDelta, this.optimalHedgeDistance);
						indexWhenStraddleFormed = this.instrumentLtp;
						
						ceStraddleOptionName =  entryStraddleOptionNames[0];
						peStraddleOptionName =  entryStraddleOptionNames[1];
						
						ceOptionGreeks = getOptionGreeks(ceStraddleOptionName);
						peOptionGreeks = getOptionGreeks(peStraddleOptionName);
						
						String logString = "Forming straddleceStraddleOptionName="+ceStraddleOptionName + "(@" + ceOptionGreeks.getLtp() + " " + peStraddleOptionName + "(@" + peOptionGreeks.getLtp() ; 
						fileLogTelegramWriter.write( " "+logString);
						
						ceDbId = createAlgoSellOrder(ceStraddleOptionName, ceOptionGreeks.getLtp(), noOfLots*lotSize);
						peDbId = createAlgoSellOrder(peStraddleOptionName, peOptionGreeks.getLtp(), noOfLots*lotSize);
						
						if (this.placeActualOrder) { // Place the straddle order with Kite
							if (ceHedgeOptionName.equals("")) {
								ceHedgeOptionName =  entryStraddleOptionNames[2];
								placeRealOrder(ceHedgeOptionName, noOfLots*lotSize, "BUY",  false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
							if (peHedgeOptionName.equals("")) {
								peHedgeOptionName =  entryStraddleOptionNames[3];
								placeRealOrder(peHedgeOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);	
							}
							placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
					}
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
	
	private float getGreekValueDiff(String greekname) {
		float retval = 0f;
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			String fieldname = "";
			if (greekname.equalsIgnoreCase("V2OTMAccmlChangeInTheta")) {
				fieldname = "drOTMAccumulatedChangein5secCeTheta as peGreek, drOTMAccumulatedChangein5secPeTheta as ceGreek";
			} else if (greekname.equalsIgnoreCase("V2OTMAccmlChangeInVega")) {
				fieldname = "drOTMAccumulatedChangein5secCeVega as peGreek, drOTMAccumulatedChangein5secPeVega as ceGreek";
			}
			Integer instrumentIdToUse = this.mainInstrument.getId().intValue();
			
			String fetchSql = "select " + fieldname + " from nexcorio_option_greek_movement_data where f_main_instrument = " + instrumentIdToUse + ""
					+ " and record_time <= '" + postgresLongDateFormat.format(getCurrentTime()) + "'"
					+ " order by record_time desc limit 5";
			fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
			ResultSet rs = stmt.executeQuery(fetchSql);
			
			
			float ceTotal=0f;
			float peTotal=0f;
			while (rs.next()) {
				ceTotal = ceTotal + rs.getFloat("ceGreek"); 
				peTotal = peTotal + rs.getFloat("peGreek");
			}	
			// Take avg
			ceTotal = ceTotal/5f;
			peTotal = peTotal/5f;
			
			retval =  ceTotal > peTotal ? ceTotal - peTotal : peTotal - ceTotal;
			
			if (ceTotal > peTotal) {
				retval = -retval;
			}
			fileLogTelegramWriter.write("ceTotal="+ceTotal+" peTotal="+peTotal+" retval="+retval);
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
		return retval;
	}
	
	public static void main(String[] args) {
		
	}
}
