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

public class G3GreekGapCocktailAlgoThread extends G3BaseClass implements Runnable{

	private static final Logger log = LogManager.getLogger(G3GreekGapCocktailAlgoThread.class);
	
	public String firstGreek  = "dr4-9AvgIv";
	public String secondGreek = "avgiv";
	
	public float baseDelta = 0.5f;
	public boolean adjustPosition = false; // Adjust position after steep fall
	
	public Integer dependentInstrumentId = null;
	
	public G3GreekGapCocktailAlgoThread(Long napAlgoId, String backTestDateStr) {
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
			
			String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(baseDelta, this.optimalHedgeDistance);
			
			String lastKnownTrend = "Unknown";
			
			float soldPrice = 0f;
			
			String currentTrend = null;
			do {
				currentTrend = getSellerDirectionByOutlierVote(lastKnownTrend);
				if (currentTrend.equals("Unknown")) sleep(15);
			} while (currentTrend.equals(lastKnownTrend));
			
			if (currentTrend.equals("CE")) {
				ceStraddleOptionName =  entryStraddleOptionNames[0];
				ceHedgeOptionName =  entryStraddleOptionNames[2];
				
				float cePrice = getPriceFromTicks(ceStraddleOptionName);
			
				fileLogTelegramWriter.write( "Taking CE directional ceStraddleOptionName="+ceStraddleOptionName + "(@" + cePrice +") ceHedgeOptionName="+ceHedgeOptionName);
				ceDbId = createAlgoSellOrder(ceStraddleOptionName, cePrice, noOfLots*lotSize);
				if (this.placeActualOrder) { 
					placeRealOrder( ceHedgeOptionName, noOfLots*lotSize, "BUY",  true, KiteUtil.USE_NORMAL_ORDER_FALSE);
					placeRealOrder( ceDbId, ceStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
				}
				soldPrice = cePrice;
			} else { // PE
				peStraddleOptionName =  entryStraddleOptionNames[1];
				peHedgeOptionName =  entryStraddleOptionNames[3];
				
				float pePrice = getPriceFromTicks(peStraddleOptionName);
				
				fileLogTelegramWriter.write( "Taking PE directional peStraddleOptionName="+peStraddleOptionName + "(@" + pePrice +") peHedgeOptionName="+peHedgeOptionName);
				peDbId = createAlgoSellOrder(peStraddleOptionName, pePrice, noOfLots*lotSize);
				if (this.placeActualOrder) { 
					placeRealOrder( peHedgeOptionName, noOfLots*lotSize, "BUY",  true, KiteUtil.USE_NORMAL_ORDER_FALSE);
					placeRealOrder( peDbId , peStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
				}
				soldPrice = pePrice;
			}
			
			lastKnownTrend = currentTrend;
			
			float maxProfitReached = 0f;
			Date maxProfitReachedAt = getCurrentTime();
			float maxLowestpointReached = 0f;
			Date maxLowestpointReachedAt = getCurrentTime();
			float maxTrailingProfit = 0f;
			
			updateAlgoStatus("Running");
			
			do {
				sleep(15); // Every 10sec
				
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
				
				currentTrend = getSellerDirectionByOutlierVote(lastKnownTrend); // StatusQuo, CE, PE
				
				if (!currentTrend.equals(lastKnownTrend)) {
				
					entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(baseDelta, this.optimalHedgeDistance);
					
					if (currentTrend.equals("CE")) { // Exit PE, Enter CE
						if (!peStraddleOptionName.equals("")) { // Exit PE, taking Directional
							fileLogTelegramWriter.write( " Exiting ="+peStraddleOptionName );
							// Exit PE
							if (this.placeActualOrder) {
								placeRealOrder( peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
							peStraddleOptionName = "";
						}
						if (!ceStraddleOptionName.equals(entryStraddleOptionNames[0])) {
							if (!ceStraddleOptionName.equals("")) { // Exit and re enter
								fileLogTelegramWriter.write( " Exiting ="+ceStraddleOptionName );
								// Exit CE
								if (this.placeActualOrder) {
									placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
								}
								ceStraddleOptionName = "";
							}
							if (this.noOfOrders<maxAllowedNoOfOrders) {
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
								soldPrice = cePrice;
							} else {
								prepareExit("Too many orders");
							}
						} else {
							fileLogTelegramWriter.write( " Retaining ="+ceStraddleOptionName);
						}
					} else if (currentTrend.equals("PE")) { // Exit CE, Enter PE
						if (!ceStraddleOptionName.equals("")) { // Exit CE, taking Directional
							fileLogTelegramWriter.write( " Exiting ="+ceStraddleOptionName );
							// Exit CE
							if (this.placeActualOrder) {
								placeRealOrder( ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
							ceStraddleOptionName = "";
						}
						if (!peStraddleOptionName.equals(entryStraddleOptionNames[1])) {
							if (!peStraddleOptionName.equals("")) { // Exit and re enter
								fileLogTelegramWriter.write( " Exiting ="+peStraddleOptionName );
								if (this.placeActualOrder) {
									placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
								}
								peStraddleOptionName = "";
							}
							if (this.noOfOrders<maxAllowedNoOfOrders) {
								peStraddleOptionName =  entryStraddleOptionNames[1];
								float pePrice = getPriceFromTicks(peStraddleOptionName);
								fileLogTelegramWriter.write( "Entering ="+peStraddleOptionName +"(@"+pePrice+")");
								// Place order
								peDbId = createAlgoSellOrder(peStraddleOptionName, pePrice, noOfLots*lotSize);
								if (this.placeActualOrder) {
									if (peHedgeOptionName.equals("")) {
										peHedgeOptionName =  entryStraddleOptionNames[3];
										placeRealOrder(peHedgeOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
									}
									placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
								}
								soldPrice = pePrice;
							} else {
								prepareExit("Too many orders");
							}
						} else {
							fileLogTelegramWriter.write( " Retaining ="+peStraddleOptionName);
						}
					}
					lastKnownTrend = currentTrend;
				} else {
					if(adjustPosition==true && this.noOfOrders<maxAllowedNoOfOrders) { // Don't try to reposition if already at maxAllowedNoOfOrders 
						if (!ceStraddleOptionName.equals("") || !peStraddleOptionName.equals("")) { // Atleast one leg should present
							float optionPrice = ceOptionGreeks!=null?ceOptionGreeks.getLtp():peOptionGreeks.getLtp();
							if (optionPrice<soldPrice/2f) { // Current price fell below half  
								fileLogTelegramWriter.write( "Price fell significantly, need to respoition to get more premium");
								
								entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(baseDelta, this.optimalHedgeDistance);
								
								if (!ceStraddleOptionName.equals("")) { // CE exist
									fileLogTelegramWriter.write( " Exiting ="+ceStraddleOptionName );
									// Exit CE
									if (this.placeActualOrder) {
										placeRealOrder( ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
									}
									ceStraddleOptionName =  entryStraddleOptionNames[0];
									float cePrice = getPriceFromTicks(ceStraddleOptionName);
									if (cePrice>=10f) {
										fileLogTelegramWriter.write( " Entering ="+ceStraddleOptionName +"(@"+cePrice+")");
										// Place order
										ceDbId = createAlgoSellOrder(ceStraddleOptionName, cePrice, noOfLots*lotSize);
										if (this.placeActualOrder) {
											placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
										}
										soldPrice = cePrice;
									} else {
										prepareExit("Low premium");
									}
								} else { // PE exist
									fileLogTelegramWriter.write( " Exiting ="+peStraddleOptionName );
									if (this.placeActualOrder) {
										placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
									}
									peStraddleOptionName =  entryStraddleOptionNames[1];
									float pePrice = getPriceFromTicks(peStraddleOptionName);
									if (pePrice>=10f) {
										fileLogTelegramWriter.write( "Entering ="+peStraddleOptionName +"(@"+pePrice+")");
										// Place order
										peDbId = createAlgoSellOrder(peStraddleOptionName, pePrice, noOfLots*lotSize);
										if (this.placeActualOrder) {
											placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
										}
										soldPrice = pePrice;
									} else {
										prepareExit("Low premium");
									}
								}
							}
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
		new G3GreekGapCocktailAlgoThread(23L, null);
	}

	

}
