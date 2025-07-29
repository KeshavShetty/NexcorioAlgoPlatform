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

public class G3GreekGapCocktailStrangleAlgoThread extends G3BaseClass implements Runnable{

	private static final Logger log = LogManager.getLogger(G3GreekGapCocktailStrangleAlgoThread.class);
	
	public String firstGreek  = "dr4-9AvgIv";
	public String secondGreek = "deltaRangeAvgIV";
	
	public float upperDelta = 0.6f;
	public float lowerDelta = 0.4f;
	public float deltagap = 0.4f;
	
	public Integer dependentInstrumentId = null;
	
	public G3GreekGapCocktailStrangleAlgoThread(Long napAlgoId, String backTestDateStr) {
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
			
			float maxProfitReached = 0f;
			Date maxProfitReachedAt = getCurrentTime();
			float maxLowestpointReached = 0f;
			Date maxLowestpointReachedAt = getCurrentTime();
			float maxTrailingProfit = 0f;
			
			updateAlgoStatus("Running");
			
			do {
				sleep(5); // Every 10sec
				
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
				
				float ceDelta = ceOptionGreeks==null?0: ceOptionGreeks.getDelta();
				float peDelta = peOptionGreeks==null?0: Math.abs(peOptionGreeks.getDelta());
				
				String currentTrend = getSellerDirectionByOutlierVote(lastKnownTrend); // StatusQuo, CE, PE
				
				boolean needAlignment = false;
				
				if (!currentTrend.equals(lastKnownTrend)) {
					needAlignment = true;
				} else if (Math.abs(ceDelta-peDelta) >= deltagap) {
					needAlignment = true;
				} else if (currentTrend.equals("CE") && ceDelta<=peDelta) {
					needAlignment = true;
				} else if (currentTrend.equals("PE") && peDelta<ceDelta) {
					needAlignment = true;
				}
				
				if (needAlignment) {
				
					String[] lowerEntryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(lowerDelta, this.optimalHedgeDistance);
					String[] upperEntryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(upperDelta, this.optimalHedgeDistance);
					
					String ceOptionname = upperEntryStraddleOptionNames[0]; // if currentTrend = "CE" {
					String peOptionname = lowerEntryStraddleOptionNames[1];
					
					if (currentTrend.equals("PE")) {
						ceOptionname = lowerEntryStraddleOptionNames[0];
						peOptionname = upperEntryStraddleOptionNames[1];
					}
					
					if (!ceStraddleOptionName.equals(ceOptionname)) {
						if (!ceStraddleOptionName.equals("")) { // Exit and re enter
							fileLogTelegramWriter.write( " Exiting ="+ceStraddleOptionName );
							// Exit CE
							if (this.placeActualOrder) {
								placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
							ceStraddleOptionName = "";
						}
						if (this.noOfOrders<maxAllowedNoOfOrders) {
							ceStraddleOptionName =  ceOptionname;
							float cePrice = getPriceFromTicks(ceStraddleOptionName);
							fileLogTelegramWriter.write( " Entering ="+ceStraddleOptionName +"(@"+cePrice+")");
							// Place order
							ceDbId = createAlgoSellOrder(ceStraddleOptionName, cePrice, noOfLots*lotSize);
							if (this.placeActualOrder) {
								if (ceHedgeOptionName.equals("")) {								
									ceHedgeOptionName =  lowerEntryStraddleOptionNames[2];
									placeRealOrder(ceHedgeOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
								}
								placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
						} else {
							prepareExit("Too many orders");
						}
					} else {
						fileLogTelegramWriter.write( " Retaining ="+ceStraddleOptionName);
					}
					
					if (!peStraddleOptionName.equals(peOptionname)) {
						if (!peStraddleOptionName.equals("")) { // Exit and re enter
							fileLogTelegramWriter.write( " Exiting ="+peStraddleOptionName );
							if (this.placeActualOrder) {
								placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
							peStraddleOptionName = "";
						}
						if (this.noOfOrders<maxAllowedNoOfOrders) {
							peStraddleOptionName =  peOptionname;
							float pePrice = getPriceFromTicks(peStraddleOptionName);
							fileLogTelegramWriter.write( "Entering ="+peStraddleOptionName +"(@"+pePrice+")");
							// Place order
							peDbId = createAlgoSellOrder(peStraddleOptionName, pePrice, noOfLots*lotSize);
							if (this.placeActualOrder) {
								if (peHedgeOptionName.equals("")) {
									peHedgeOptionName =  lowerEntryStraddleOptionNames[3];
									placeRealOrder(peHedgeOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
								}
								placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
						} else {
							prepareExit("Too many orders");
						}
					} else {
						fileLogTelegramWriter.write( " Retaining ="+peStraddleOptionName);
					}
				}	
					
				lastKnownTrend = currentTrend;
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
			fileLogTelegramWriter.write( " noOfOrders="+noOfOrders + " ROI=" + (currentProfitPerUnit*this.lotSize*100f)/requiredMargin + "% (Max profit/lot reached to "+ (maxProfitReached) +"@" + maxProfitReachedAt+ "\n and Lowest reached to " + (maxLowestpointReached) + "@" + maxLowestpointReachedAt + ")");
		} catch (Exception e) {			
			updateAlgoStatus("Error");
			log.error("Error"+e.getMessage(), e);
			fileLogTelegramWriter.write("Error " + ExceptionUtils.getStackTrace(e));
		} finally {
			fileLogTelegramWriter.close();
		}
	}
	
	private String getSellerDirectionByOutlierVote(String lastKnownTrend) {
		String retVal = lastKnownTrend;
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			Integer instrumentIdToUse = this.mainInstrument.getId().intValue();
			if (dependentInstrumentId!=null) {
				instrumentIdToUse = dependentInstrumentId;
			}
			
			String fetchSql = "select countCEOutlier as ceGreek, countPEOutlier as peGreek from nexcorio_option_atm_movement_data where f_main_instrument = " + instrumentIdToUse + ""
					+ " and record_time <= '" + postgresLongDateFormat.format(getCurrentTime()) + "'"
					+ " order by record_time desc limit 5";
			fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
			ResultSet rs = stmt.executeQuery(fetchSql);
			
			int gapCount = 0;
			
			while (rs.next()) {
				float ceGreek = rs.getFloat("ceGreek");
				float peGreek = rs.getFloat("peGreek");
				if (Math.abs(ceGreek-peGreek)>6) {
					gapCount++;
				}
			}
			rs.close();			
			stmt.close();
			
			if (gapCount == 5 ) {
				fileLogTelegramWriter.write("gapCount="+gapCount+" using " + this.firstGreek);
				retVal = getSellerDirectionByATMGreekGap(this.firstGreek, lastKnownTrend);
			} else {
				fileLogTelegramWriter.write("gapCount="+gapCount+" using " + this.secondGreek);
				//retVal = getSellerDirectionByATMGreekGap("deltaRangeAvgIV", lastKnownTrend);
				retVal = getSellerDirectionByATMGreekGap(this.secondGreek, lastKnownTrend);
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
	
	private String getSellerDirectionByATMGreekGap(String greekname, String lastKnownTrend) {
		String retVal = lastKnownTrend;
		
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			String fieldname = "ceiv as ceGreek, peiv as peGreek";
			if (greekname.equalsIgnoreCase("ltp")) {
				fieldname = "celtp as ceGreek, peltp as peGreek";
			} else if (greekname.equalsIgnoreCase("gamma")) {
				fieldname = "cegamma as ceGreek, pegamma as peGreek";
			} else if (greekname.equalsIgnoreCase("avggamma")) {
				fieldname = "avgcegamma as ceGreek, avgpegamma as peGreek";
			} else if (greekname.equalsIgnoreCase("selectiveavggamma")) {
				fieldname = "selectivestrike_avgcegamma as ceGreek, selectivestrike_avgpegamma as peGreek";
			} else if (greekname.equalsIgnoreCase("avgiv")) {
				fieldname = "totalceiv as ceGreek, totalpeiv as peGreek";
			} else if (greekname.equalsIgnoreCase("deltaRangeAvgIV")) {
				fieldname = "deltarangeceavgiv as ceGreek, deltarangepeavgiv as peGreek";
			} else if (greekname.equalsIgnoreCase("deltaRangeAvgGamma")) {
				fieldname = "deltarangeceavggamma as ceGreek, deltarangepeavggamma as peGreek";
			} else if (greekname.equalsIgnoreCase("deltaRangeAvgLtp")) {
				fieldname = "deltarangepeavgltp as ceGreek, deltarangeceavgltp as peGreek"; // For ltp peltp as cegreek (similar to gamma reverse)
			} else if (greekname.equalsIgnoreCase("deltaRangeDeltaOI")) {
				fieldname = "deltarangecedeltaoi as ceGreek, deltarangepedeltaoi as peGreek";
			} else if (greekname.equalsIgnoreCase("deltaRangeGmaOI")) { // Gamma name removed as it will inverse with word gamma
				fieldname = "deltarangecegammaoi as ceGreek, deltarangepegammaoi as peGreek";
			} else if (greekname.equalsIgnoreCase("drFullAvgIV")) {
				fieldname = "deltaRangeCEFullAvgIv as ceGreek, deltaRangePEFullAvgIv as peGreek";
			} else if (greekname.equalsIgnoreCase("drHybridAvgIV")) {
				fieldname = "deltaRangeHybridCEAvgIv as ceGreek, deltaRangeHybridPEAvgIv as peGreek";
			} else if (greekname.equalsIgnoreCase("drHybridAvgGamma")) {
				fieldname = "deltaRangeHybridCEAvgGamma as ceGreek, deltaRangeHybridPEAvgGamma as peGreek";
			} else if (greekname.equalsIgnoreCase("drOutlierRatio")) {
				fieldname = "deltarangeceoutlierratio as ceGreek, deltarangepeoutlierratio as peGreek";
			} else if (greekname.equalsIgnoreCase("dr4-9AvgIv")) {
				fieldname = "dr4_9CEAvgIv as ceGreek, dr4_9PEAvgIv as peGreek";
			}
			
			Integer instrumentIdToUse = this.mainInstrument.getId().intValue();
			if (dependentInstrumentId!=null) {
				instrumentIdToUse = dependentInstrumentId;
			}
			
			String fetchSql = "select " + fieldname + " from nexcorio_option_atm_movement_data where f_main_instrument = " + instrumentIdToUse + ""
					+ " and record_time <= '" + postgresLongDateFormat.format(getCurrentTime()) + "'"
					+ " order by record_time desc limit 5";
			fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
			ResultSet rs = stmt.executeQuery(fetchSql);
			
			int gapCount = 0;
			
			while (rs.next()) {
				float ceGreek = rs.getFloat("ceGreek");
				float peGreek = rs.getFloat("peGreek");
				if (ceGreek > peGreek) {
					gapCount++;
				}
			}
			rs.close();			
			stmt.close();
			
			fileLogTelegramWriter.write("gapCpunt="+gapCount);
			
			if (greekname.contains("gamma") || greekname.contains("Gamma")) gapCount = 5-gapCount;
			
			if (gapCount == 0) {
				retVal = "PE";
			} else if (gapCount == 5) {
				retVal = "CE";
			}
			
			if (retVal.equals("Unknown") ) {
				if (gapCount < 2 ) {
					retVal = "PE";
				} else if (gapCount > 3 ) {
					retVal = "CE";
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
		new G3GreekGapCocktailStrangleAlgoThread(23L, null);
	}

	

}
