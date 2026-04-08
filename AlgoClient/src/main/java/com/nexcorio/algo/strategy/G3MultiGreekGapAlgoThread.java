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

public class G3MultiGreekGapAlgoThread extends G3BaseClass implements Runnable{

	private static final Logger log = LogManager.getLogger(G3MultiGreekGapAlgoThread.class);
	
	public String greeknames = "above5WhlStrkAvgIv,altAbov5WhlStrkAvgIv,drWhlStrkaccmlTheta";// drITMWhlStrkAvgIv
	// "dr16accmlGama,dr49accmlGama"; // drITMWhlStrkAvgIv,,drOutlierRatio dr49accmlVega  drWhlStrkaccmlTheta ,dr16accmlVega,dr19WholeStrikeAvgIV drITMWhlStrkAvgIv,accmltd5secIVChg,drWhlStrkaccmlTheta
	public float baseDelta = 0.5f;
	
	public boolean exitIfNeutral = false;
	
	public G3MultiGreekGapAlgoThread(Long napAlgoId, String backTestDateStr) {
		super(napAlgoId);
		initializeParameters(backTestDateStr);
		
		fileLogTelegramWriter.write(this.algoname);
		Thread t = new Thread(this, this.mainInstrument.getShortName()+this.algoname);
		t.setPriority(Thread.MAX_PRIORITY);
		t.start();
		printFields(this);
	}
	
	@Override
	public void run() {
		try {
			if (this.placeActualOrder) setLotBasedonAvailableMarginHalfStraddle();
			
			long ceDbId = -1;
			long peDbId = -1;
			
			float maxProfitReached = 0f;
			Date maxProfitReachedAt = getCurrentTime();
			float maxLowestpointReached = 0f;
			Date maxLowestpointReachedAt = getCurrentTime();
			float maxTrailingProfit = 0f;
			
			updateAlgoStatus("Running");
			
			do {
				sleep(5); 
				
				this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
				
				OptionGreek ceOptionGreeks = getOptionGreeks(ceStraddleOptionName);
				OptionGreek peOptionGreeks = getOptionGreeks(peStraddleOptionName);
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
				
				String currentTrend = getSellerDirectionByCompositeGreekGap(); // Neutral, CE, PE
				
				if (currentTrend.equals("Neutral") ) { // Neutral and exitIfNeutral
					if (exitIfNeutral) {
						if (!peStraddleOptionName.equals("")) { // Exit PE
							fileLogTelegramWriter.write( " Exiting ="+peStraddleOptionName );
							// Exit PE
							if (this.placeActualOrder) {
								placeRealOrder( peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
							peStraddleOptionName = "";
						}
						if (!ceStraddleOptionName.equals("")) { // Exit CE
							fileLogTelegramWriter.write( " Exiting ="+ceStraddleOptionName );
							// Exit CE
							if (this.placeActualOrder) {
								placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
							ceStraddleOptionName = "";
						}
					}
				} else if (currentTrend.equals("CE") ) {
					//Exit PE, Enter CE
					if (!peStraddleOptionName.equals("")) { // Exit PE
						fileLogTelegramWriter.write( " Exiting ="+peStraddleOptionName );
						// Exit PE
						if (this.placeActualOrder) {
							placeRealOrder( peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						peStraddleOptionName = "";
					}
					
					if (ceStraddleOptionName.equals("")) { // CE doesn't exist 
						if (this.noOfOrders<maxAllowedNoOfOrders) {
							String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(baseDelta, this.optimalHedgeDistance);
							
							ceStraddleOptionName =  entryStraddleOptionNames[0];
							float cePrice = getPriceFromTicks(ceStraddleOptionName);
						
							fileLogTelegramWriter.write( "Taking CE directional ceStraddleOptionName="+ceStraddleOptionName + "(@" + cePrice +") ceHedgeOptionName="+ceHedgeOptionName);
							ceDbId = createAlgoSellOrder(ceStraddleOptionName, cePrice, noOfLots*lotSize);
							if (this.placeActualOrder) { 
								if (ceHedgeOptionName.equals("")) {
									ceHedgeOptionName =  entryStraddleOptionNames[2];
									placeRealOrder( ceHedgeOptionName, noOfLots*lotSize, "BUY",  true, KiteUtil.USE_NORMAL_ORDER_FALSE);
								}
								placeRealOrder( ceDbId, ceStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
						} else {
							prepareExit("Too many orders");
						}
					}
				} else if (currentTrend.equals("PE") ) {
					//Exit PE, Enter CE
					if (!ceStraddleOptionName.equals("")) { // Exit PE
						fileLogTelegramWriter.write( " Exiting ="+ceStraddleOptionName );
						// Exit PE
						if (this.placeActualOrder) {
							placeRealOrder( ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						ceStraddleOptionName = "";
					}
					
					if (peStraddleOptionName.equals("")) { // PE doesn't exist 
						if (this.noOfOrders<maxAllowedNoOfOrders) {
							String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(baseDelta, this.optimalHedgeDistance);
							
							peStraddleOptionName =  entryStraddleOptionNames[1];
							float pePrice = getPriceFromTicks(peStraddleOptionName);
						
							fileLogTelegramWriter.write( "Taking PE directional peStraddleOptionName="+peStraddleOptionName + "(@" + pePrice +") ");
							peDbId = createAlgoSellOrder(peStraddleOptionName, pePrice, noOfLots*lotSize);
							if (this.placeActualOrder) { 
								if (peHedgeOptionName.equals("")) {
									peHedgeOptionName =  entryStraddleOptionNames[3];
									placeRealOrder( peHedgeOptionName, noOfLots*lotSize, "BUY",  true, KiteUtil.USE_NORMAL_ORDER_FALSE);
								}
								placeRealOrder( peDbId, peStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
						} else {
							prepareExit("Too many orders");
						}
					}
				}
				
				checkExitSignals();
				
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
	
	
	private String getSellerDirectionByCompositeGreekGap() {
		String retVal = "Neutral";
		
		String[] allGreeks = greeknames.split(",");
		
		int ceCount = 0;
		int peCount = 0;
		
		for(int i=0;i<allGreeks.length;i++) {
			String aTrend = getSellerDirectionByGreekGap(allGreeks[i].trim());
			if (aTrend.equals("CE")) ceCount++;
			else if (aTrend.equals("PE")) peCount++;
		}
		
		if (ceCount>peCount) retVal="CE";
		else if (peCount>ceCount) retVal="PE";
		
		return retVal;
	}
	
	private String getSellerDirectionByGreekGap(String greekname) {
		String retVal = "Unknown";
		
		
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			String fieldname = "ceiv as ceGreek, peiv as peGreek";
			if (greekname.equalsIgnoreCase("drWhlStrkaccmlVega")) {
				fieldname = " drWhlStrkaccumulatedchangein5seccevega as peGreek, drWhlStrkaccumulatedchangein5secpevega  as ceGreek";
			} else if (greekname.equalsIgnoreCase("drITMWhlStrkAvgIv")) {
				fieldname = "drITMWhlStrkSameSizeCEAvgIv as ceGreek, drITMWhlStrkSameSizePEAvgIv as peGreek";
			} else if (greekname.equalsIgnoreCase("dr49accmlVega")) {
				fieldname = "dr49accumulatedchangein5seccevega as ceGreek, dr49accumulatedchangein5secpevega as peGreek";
			} else if (greekname.equalsIgnoreCase("drWhlStrkaccmlTheta")) {
				fieldname = "drWhlStrkaccumulatedchangein5secpetheta as peGreek, drWhlStrkaccumulatedchangein5seccetheta as ceGreek";
			} else if (greekname.equalsIgnoreCase("drOutlierRatio")) {
				fieldname = "deltarangeceoutlierratio as ceGreek, deltarangepeoutlierratio as peGreek";
			} else if (greekname.equalsIgnoreCase("dr16accmlVega")) {
				fieldname = "dr16accumulatedchangein5seccevega as ceGreek, dr16accumulatedchangein5secpevega as peGreek";
			} else if (greekname.equalsIgnoreCase("dr19WholeStrikeAvgIV")) {
				fieldname = "dr19WholeStrikeCEAvgIV as ceGreek, dr19WholeStrikePEAvgIV as peGreek";
			} else if (greekname.equalsIgnoreCase("accmltd5secIVChg")) {
				fieldname = "accumulatedChangein5secCeIV as peGreek, accumulatedChangein5secPeIV as ceGreek";
			} else if (greekname.equalsIgnoreCase("dr49accmlIv")) {
				fieldname = "dr49accumulatedchangein5secpeIv as ceGreek, dr49accumulatedchangein5secceIv as peGreek";
			} else if (greekname.equalsIgnoreCase("dr16accmlGama")) {
				fieldname = "dr16accumulatedchangein5seccegamma as ceGreek, dr16accumulatedchangein5secpegamma as peGreek";
			} else if (greekname.equalsIgnoreCase("dr49accmlGama")) {
				fieldname = "dr49accumulatedchangein5seccegamma as ceGreek, dr49accumulatedchangein5secpegamma as peGreek";
			} else if (greekname.equalsIgnoreCase("above5WhlStrkAvgIv")) {
				fieldname = "above5WhlStrkCEAvgIv as ceGreek, above5WhlStrkPEAvgIv as peGreek"; // fieldname = "dr49WhlStrkaccumulatedchangein5secceGamma as ceGreek, dr49WhlStrkaccumulatedchangein5secpeGamma as peGreek"; //
			} else if (greekname.equalsIgnoreCase("altAbov5WhlStrkAvgIv")) {
				fieldname = "altabove5WhlStrkCEAvgIv as ceGreek, altabove5WhlStrkPEAvgIv as peGreek"; // fieldname = "dr49WhlStrkaccumulatedchangein5secceGamma as ceGreek, dr49WhlStrkaccumulatedchangein5secpeGamma as peGreek"; //
			}
			
			Integer instrumentIdToUse = this.mainInstrument.getId().intValue();
			
			String fetchSql = "select " + fieldname + " from nexcorio_option_atm_movement_data where f_main_instrument = " + instrumentIdToUse + ""
					+ " and record_time <= '" + postgresLongDateFormat.format(getCurrentTime()) + "'"
					+ " order by record_time desc limit 5";
			fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
			ResultSet rs = stmt.executeQuery(fetchSql);
			
			int gapCount = 0;
			while (rs.next()) {
				float ceGreek = rs.getFloat("ceGreek");
				float peGreek = rs.getFloat("peGreek");
				
				if (ceGreek>=peGreek) {
					gapCount++;
				}
			}
			rs.close();			
			stmt.close();
			
			if (gapCount == 0) {
				retVal = "PE";
			} else if (gapCount == 5) {
				retVal = "CE";
			}
			fileLogTelegramWriter.write("For "+ greekname + " gapCpunt=" + gapCount +" retVal="+retVal);
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
