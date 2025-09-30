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

public class G3ScalpGreekGapChangeInIVAlgoThread extends G3BaseClass implements Runnable{

	private static final Logger log = LogManager.getLogger(G3ScalpGreekGapChangeInIVAlgoThread.class);
	
	private String ceBuyOptionname = "";
	private String peBuyOptionname = "";
	
	
	public float baseDelta = 0.5f;
	public boolean adjustPosition = false; // Adjust position after steep fall
	
	public Integer dependentInstrumentId = null;
	
	public boolean useMinMax = false;
	
	public G3ScalpGreekGapChangeInIVAlgoThread(Long napAlgoId, String backTestDateStr) {
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
			
			this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
			fileLogTelegramWriter.write( " this.instrumentLtp="+this.instrumentLtp);
			
			String lastKnownTrend = "Unknown";
			
			String currentTrend = null;
			do {
				currentTrend = getSellerDirectionByATMGreekGap(lastKnownTrend);
				if (currentTrend.equals("Unknown")) sleep(15);
			} while (currentTrend.equals(lastKnownTrend));
			
			String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(baseDelta, this.optimalHedgeDistance);
			
			if (currentTrend.equals("CE")) {
				peBuyOptionname = entryStraddleOptionNames[1];
				float pePrice = getPriceFromTicks(peBuyOptionname);
				fileLogTelegramWriter.write( " Entering Long ="+peBuyOptionname +"@" +pePrice );
				peDbId = createAlgoBuyOrder(peBuyOptionname, pePrice, noOfLots*lotSize);
				if (this.placeActualOrder) {
					placeRealOrder(peDbId, peBuyOptionname, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
				}
			} else if (currentTrend.equals("CE")) {
				ceBuyOptionname = entryStraddleOptionNames[0];
				float cePrice = getPriceFromTicks(ceBuyOptionname);
				fileLogTelegramWriter.write( " Entering Long ="+ceBuyOptionname +"@" +cePrice );
				ceDbId = createAlgoBuyOrder(ceBuyOptionname, cePrice, noOfLots*lotSize);
				if (this.placeActualOrder) {
					placeRealOrder(ceDbId, ceBuyOptionname, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
				}	
			}
			
			lastKnownTrend = currentTrend;
			
			float maxProfitReached = 0f;
			Date maxProfitReachedAt = getCurrentTime();
			float maxLowestpointReached = 0f;
			Date maxLowestpointReachedAt = getCurrentTime();
			float maxTrailingProfit = 0f;
			
			updateAlgoStatus("Running");
			
			do {
				sleep(5); // Every 10sec
				
				this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
				
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
				
				currentTrend = getSellerDirectionByATMGreekGap(lastKnownTrend); // StatusQuo, CE, PE
				
				if (!currentTrend.equals(lastKnownTrend)) {
				
					entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(baseDelta, this.optimalHedgeDistance);
					
					if (currentTrend.equals("CE")) { 
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
						} else {
							prepareExit("Too many orders");
						}
					} else if (currentTrend.equals("PE")) {
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
						} else {
							prepareExit("Too many orders");
						}
					}
					lastKnownTrend = currentTrend;
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
			fileLogTelegramWriter.write( " noOfOrders="+noOfOrders + " ROI=" + (currentProfitPerUnit*this.lotSize*100f)/requiredMargin + "% (Max profit/lot reached to "+ (maxProfitReached) +"@" + maxProfitReachedAt+ "\n and Lowest reached to " + (maxLowestpointReached) + "@" + maxLowestpointReachedAt + ")");
		} catch (Exception e) {			
			updateAlgoStatus("Error");
			log.error("Error"+e.getMessage(), e);
			fileLogTelegramWriter.write("Error " + ExceptionUtils.getStackTrace(e));
		} finally {
			fileLogTelegramWriter.close();
		}
	}
	
	private String getSellerDirectionByATMGreekGap( String lastKnownTrend) {
		String retVal = lastKnownTrend;
		
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			String fieldname = "totalChangeInCEIV as ceGreek, totalChangeInPEIV as peGreek";
			
			
			Integer instrumentIdToUse = this.mainInstrument.getId().intValue();
			if (dependentInstrumentId!=null) {
				instrumentIdToUse = dependentInstrumentId;
			}
			
			String fetchSql = "select " + fieldname + " from nexcorio_option_atm_movement_data where f_main_instrument = " + instrumentIdToUse + ""
					+ " and record_time <= '" + postgresLongDateFormat.format(getCurrentTime()) + "'"
					+ " order by record_time desc limit 5";
			fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
			ResultSet rs = stmt.executeQuery(fetchSql);
			
			int ceCount = 0;
			int peCount = 0;
			
			float totalCEIV = 0;
			float totalPEIV = 0;
			while (rs.next()) {
				float ceGreek = rs.getFloat("ceGreek");
				float peGreek = rs.getFloat("peGreek");
				
				if (ceGreek >= 0) ceCount++;
				if (peGreek >= 0) peCount++;
				
				totalCEIV = totalCEIV + ceGreek;
				totalPEIV = totalPEIV + peGreek;
			}
			rs.close();			
			stmt.close();
			
			if (ceCount==5) retVal = "CE";
			if (peCount==5) retVal = "PE";
			
			fileLogTelegramWriter.write("totalCEIV="+totalCEIV+" totalPEIV="+totalPEIV+" retVal="+retVal);
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
