package com.nexcorio.algo.strategy;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.util.KiteUtil;
import com.nexcorio.algo.util.db.HDataSource;

public class G3GreekGapFuturesFilterAlgoThread extends G3BaseClass implements Runnable{

	private static final Logger log = LogManager.getLogger(G3GreekGapFuturesFilterAlgoThread.class);
	
	public String greekname = "iv";
	public float baseDelta = 0.5f;
	public float adjustGap = 0.0f;
	public boolean adjustPosition = false; // Adjust position after steep fall
	
	public boolean bearOnHighVol = false;
	public float futureCutoffVolume = 0f;
	
	public Integer dependentInstrumentId = null;
	
	public boolean useScaledDelta = false;
	
	public G3GreekGapFuturesFilterAlgoThread(Long napAlgoId, String backTestDateStr) {
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
			
			float soldPrice = 0f;
			String currentTrend = null;
			do {
				currentTrend = getSellerDirectionByATMGreekGap(lastKnownTrend);
				if (currentTrend.equals("Unknown")) sleep(15);
				checkExitSignals();
			} while (currentTrend.equals(lastKnownTrend) && this.exitThread==false);
			if (exitThread==true) {
				return;
			}
			
			String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(baseDelta, this.optimalHedgeDistance);
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
				
				currentTrend = getSellerDirectionByATMGreekGap(lastKnownTrend); // StatusQuo, CE, PE
				
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
									placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
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
									placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
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
					} else { // Uncertain, exit
						if (!ceStraddleOptionName.equals("")) { // Exit CE, taking Directional
							fileLogTelegramWriter.write( " Exiting ="+ceStraddleOptionName );
							// Exit CE
							if (this.placeActualOrder) {
								placeRealOrder( ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
							ceStraddleOptionName = "";
						}
						if (!peStraddleOptionName.equals("")) { // Exit and re enter
							fileLogTelegramWriter.write( " Exiting ="+peStraddleOptionName );
							if (this.placeActualOrder) {
								placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
							peStraddleOptionName = "";
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
	
	private String getSellerDirectionByATMGreekGap( String lastKnownTrend) {
		 
		if (greekname.equals("deltaPrbltOiWorth")) {
				return getOptionTrendFromDeltaProbablityAdjustedOIWorth(lastKnownTrend);
		}
		
		String retVal = lastKnownTrend;
		Connection conn = null;
		 try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			String fieldname = "drWhlStrkaccumulatedchangein5secpetheta as peGreek, drWhlStrkaccumulatedchangein5seccetheta as ceGreek";;
			if (this.greekname.equalsIgnoreCase("drWhlStrkaccmlTheta")) {
				fieldname = "drWhlStrkaccumulatedchangein5seccetheta as ceGreek, drWhlStrkaccumulatedchangein5secpetheta as peGreek ";
			} else if (greekname.equalsIgnoreCase("altAbov5WhlStrkAvgIv")) {
				fieldname = "altabove5WhlStrkCEAvgIv as ceGreek, altabove5WhlStrkPEAvgIv as peGreek"; // fieldname = "dr49WhlStrkaccumulatedchangein5secceGamma as ceGreek, dr49WhlStrkaccumulatedchangein5secpeGamma as peGreek"; //
			} else if (greekname.equalsIgnoreCase("dr4-9AvgIv")) {
				fieldname = "dr4_9CEAvgIv as ceGreek, dr4_9PEAvgIv as peGreek";
			} else if (greekname.equalsIgnoreCase("AvgIv")) {
				fieldname = "ceiv as ceGreek, peiv as peGreek";
			} else if (greekname.equalsIgnoreCase("drFullAvgIV")) {
				fieldname = "deltaRangeCEFullAvgIv as ceGreek, deltaRangePEFullAvgIv as peGreek";
			} else if (greekname.equalsIgnoreCase("dr1-6AvgIv")) {
				fieldname = "dr1_6CEAvgIv as ceGreek, dr1_6PEAvgIv as peGreek";
			} else if (this.greekname.equalsIgnoreCase("gamma")) {
				fieldname = "cegamma as peGreek, pegamma as ceGreek";
			} else if (this.greekname.equalsIgnoreCase("deltaRangeAvgIV")) {
				fieldname = "deltarangeceavgiv as ceGreek, deltarangepeavgiv as peGreek";
			} else if (this.greekname.equalsIgnoreCase("cumAvgIVDiff")) {
				fieldname = "cumulativeCEAvgIVDiff as ceGreek, cumulativePEAvgIVDiff as peGreek";
			} else if (greekname.equalsIgnoreCase("accmltd5secIVChg")) {
				fieldname = "accumulatedChangein5secCeIV as ceGreek, accumulatedChangein5secPeIV as peGreek";
			} else if (greekname.equalsIgnoreCase("drOutlierRatio")) {
				fieldname = "deltarangeceoutlierratio as ceGreek, deltarangepeoutlierratio as peGreek";
			} else if (this.greekname.equalsIgnoreCase("dr16accmlTheta")) {
				fieldname = "dr16accumulatedchangein5secpetheta as ceGreek, dr16accumulatedchangein5seccetheta as peGreek";
			} else if (greekname.equalsIgnoreCase("drITMWhlStrkAvgIv")) {
				fieldname = "drITMWhlStrkSameSizeCEAvgIv as ceGreek, drITMWhlStrkSameSizePEAvgIv as peGreek"; // fieldname = "dr49WhlStrkaccumulatedchangein5secceGamma as ceGreek, dr49WhlStrkaccumulatedchangein5secpeGamma as peGreek"; //
			} else if (this.greekname.equalsIgnoreCase("drWhlStrkaccmlVega")) {
				fieldname = "drWhlStrkaccumulatedchangein5seccevega as ceGreek, drWhlStrkaccumulatedchangein5secpevega as peGreek";
			} else if (greekname.equalsIgnoreCase("dr19fxdSizAccmlTheta")) {
				fieldname = "dr19fixedSizeAccmlCETheta as ceGreek, dr19fixedSizeAccmlPETheta as peGreek";
			} else if (this.greekname.equalsIgnoreCase("dr49accmlTheta")) {
				fieldname = "dr49accumulatedchangein5seccetheta as ceGreek, dr49accumulatedchangein5secpetheta as peGreek";
			} else if (this.greekname.equalsIgnoreCase("dr49accmlGama")) {
				fieldname = "dr49accumulatedchangein5seccegamma as ceGreek, dr49accumulatedchangein5secpegamma as peGreek";
			} else if (greekname.equalsIgnoreCase("drITMWhlStrkAvgIv")) {
				fieldname = "drITMWhlStrkSameSizeCEAvgIv as ceGreek, drITMWhlStrkSameSizePEAvgIv as peGreek"; // fieldname = "dr49WhlStrkaccumulatedchangein5secceGamma as ceGreek, dr49WhlStrkaccumulatedchangein5secpeGamma as peGreek"; //
			} else if (greekname.equalsIgnoreCase("above5WhlStrkAvgIv")) {
				fieldname = "above5WhlStrkCEAvgIv as ceGreek, above5WhlStrkPEAvgIv as peGreek"; // fieldname = "dr49WhlStrkaccumulatedchangein5secceGamma as ceGreek, dr49WhlStrkaccumulatedchangein5secpeGamma as peGreek"; //
			} else if (this.greekname.equalsIgnoreCase("drWhlStrkaccmlLtp")) {
				fieldname = "drWhlStrkaccumulatedchangein5secceLtp as ceGreek, drWhlStrkaccumulatedchangein5secpeLtp as peGreek";
			} else if (greekname.equalsIgnoreCase("tmpaccmltheta")) {
				fieldname = "tmpaccmlcetheta as ceGreek, tmpaccmlpetheta as peGreek";
			}
			
			Integer instrumentIdToUse = this.mainInstrument.getId().intValue();
			if (dependentInstrumentId!=null) {
				instrumentIdToUse = dependentInstrumentId;
			}
			
			String fetchSql = "select " + fieldname + ", future_Outstanding_Volume from nexcorio_option_atm_movement_data where f_main_instrument = " + instrumentIdToUse + ""
					+ " and record_time <= '" + postgresLongDateFormat.format(getCurrentTime()) + "'"
					+ " order by record_time desc limit 5";
			fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
			ResultSet rs = stmt.executeQuery(fetchSql);
			
			int gapCount = 0;
			float avgFutureOutstandingVolume = 0f;
			
			while (rs.next()) {
				float ceGreek = rs.getFloat("ceGreek");
				float peGreek = rs.getFloat("peGreek");
				avgFutureOutstandingVolume = avgFutureOutstandingVolume + rs.getInt("future_Outstanding_Volume");
				
				if (ceGreek+adjustGap>=peGreek) {
					gapCount++;
				}
			}
			rs.close();			
			stmt.close();
			avgFutureOutstandingVolume = avgFutureOutstandingVolume /5f;
			
			if (gapCount == 0) {
				retVal = "PE";
			} else if (gapCount == 5) {
				retVal = "CE";
			}
			fileLogTelegramWriter.write("gapCpunt=" + gapCount + " avgFutureOutstandingVolume=" + avgFutureOutstandingVolume+" retVal="+retVal);
			if (retVal.equals("PE") && avgFutureOutstandingVolume < futureCutoffVolume) {
				if (bearOnHighVol==true) retVal = "CE";
				else retVal = "Unknown";
			}
//			if (retVal.equals("CE") && avgFutureOutstandingVolume > 100000f) {
//				if (bearOnHighVol==true) retVal = "PE";
//				else retVal = "Unknown";
//			}
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
	
	private String getOptionTrendFromDeltaProbablityAdjustedOIWorth(String lastKnownOptiontrend) {
		String retVal = "StatusQuo";
		
		Connection conn = null;
		String top4Options ="";
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			String optionnamePrefix = getCurrentWeekExpiryOptionnamePrefix();
			
			float ceOIWorth = 0f;
			float peOIWorth = 0f;
			
			if (backtestDate == null) {
				String opOIFetch = "select trading_symbol, delta, oi as open_interest, oi*ltp/10000000 as worthInCr from nexcorio_option_snapshot where trading_symbol like '" + optionnamePrefix + "%' and record_date = '" + postgresShortDateFormat.format(getCurrentTime()) +"' "
						+ " and oi*ltp/10000000>10"  + " order by oi desc limit 7";
				
				fileLogTelegramWriter.write("opOIFetch="+opOIFetch);
				ResultSet rs = stmt.executeQuery(opOIFetch);
				
				while (rs.next()) {
					String tradingSymbol = rs.getString("trading_symbol");
					float worthInCr = rs.getFloat("worthInCr");
					float delta = rs.getFloat("delta");
					top4Options = top4Options + tradingSymbol +" ";
					
					if (tradingSymbol.endsWith("CE")) {
						if (useScaledDelta) ceOIWorth = ceOIWorth + worthInCr*(1f-delta);				
						else ceOIWorth = ceOIWorth + worthInCr*delta;
					} else {
						if (useScaledDelta) peOIWorth = peOIWorth + worthInCr*(1f+delta);
						else peOIWorth = peOIWorth + worthInCr*-delta;
					}
				}
				rs.close();
			} else {
				List<OptionGreek> optionGreeks = new ArrayList<OptionGreek>();
				// First try to fetch from Snapshot table
				String fetchSql = "select DISTINCT(trading_symbol) as trading_symbol from nexcorio_option_snapshot"
						+ " where trading_symbol like '" + mainInstrument.getShortName() + "%' "
						+ " and record_date = '" + postgresShortDateFormat.format(getCurrentTime()) + "'";
				fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
				
				List<String> optionnames = new ArrayList<>();			
				ResultSet rs = stmt.executeQuery(fetchSql);
				while (rs.next()) {
					optionnames.add(rs.getString("trading_symbol"));
				}
				rs.close();
				
				if (optionnames.size()==0) { // not found in snapshot		
					fetchSql = "select DISTINCT(trading_symbol) as trading_symbol from nexcorio_option_greeks"
							+ " where f_main_instrument = " + mainInstrument.getId() + " "
							+ " and quote_time > '" + postgresShortDateFormat.format(getCurrentTime()) + " 09:15:00'"
							+ " and quote_time < '" + postgresShortDateFormat.format(getCurrentTime()) + " 09:20:00'";
								
					rs = stmt.executeQuery(fetchSql);
					while (rs.next()) {
						optionnames.add(rs.getString("trading_symbol"));
					}
					rs.close();
					
					// Insert to snapshot
					for(String aSymbol: optionnames) {
						String insertSql = "INSERT INTO nexcorio_option_snapshot (id, trading_symbol, record_date)"
								+ " VALUES (nextval('nexcorio_option_snapshot_id_seq'),'" + aSymbol + "','" + postgresShortDateFormat.format(getCurrentTime()) + "')";
						stmt.executeUpdate(insertSql);
					}
				}
				for(String optionname:optionnames ) {
					OptionGreek aGreek = getOptionGreeks(optionname);
					if (aGreek!=null) {
						optionGreeks.add(aGreek);
					}
				}
			
				Collections.sort(optionGreeks, new SortbyOI());
				
				int recProcessed = 0;
				for(OptionGreek aGreek: optionGreeks) {
					if (aGreek.getOi()*aGreek.getLtp()/10000000>10) {
						recProcessed++;
						float delta = aGreek.getDelta();
						float worthInCr = aGreek.getOi()*aGreek.getLtp()/10000000f;
						top4Options = top4Options + aGreek.getTradingSymbol() +" ";
						
						if (aGreek.getTradingSymbol().endsWith("CE")) {
							if (useScaledDelta) ceOIWorth = ceOIWorth + worthInCr*(1f-delta);				
							else ceOIWorth = ceOIWorth + worthInCr*delta;
						} else {
							if (useScaledDelta) peOIWorth = peOIWorth + worthInCr*(1f+delta);
							else peOIWorth = peOIWorth + worthInCr*-delta;
						}
					}
					if (recProcessed>=7) break;
				}
			}
			
			if (ceOIWorth-peOIWorth>10) {
				retVal = "CE";
			} else if (peOIWorth-ceOIWorth>10) {
				retVal = "PE";
			} else {
				retVal = lastKnownOptiontrend;
			}
			
			if (retVal.equals("PE")) { // check for futures outstanding
				String fetchSql = "select future_Outstanding_Volume from nexcorio_option_atm_movement_data where f_main_instrument = " + mainInstrument.getId() + ""
						+ " and record_time <= '" + postgresLongDateFormat.format(getCurrentTime()) + "'"
						+ " order by record_time desc limit 5";
				fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
				ResultSet rs = stmt.executeQuery(fetchSql);
				
				float avgFutureOutstandingVolume = 0f;
				while (rs.next()) {
					avgFutureOutstandingVolume = avgFutureOutstandingVolume + rs.getInt("future_Outstanding_Volume");
				}
				rs.close();			
				stmt.close();
				avgFutureOutstandingVolume = avgFutureOutstandingVolume /5f;
				
				if ( avgFutureOutstandingVolume < futureCutoffVolume) {
					if (bearOnHighVol==true) retVal = "CE";
					else retVal = "Unknown";
				}		
			}
			String logString = " ceOIWorth="+ceOIWorth+" peOIWorth="+peOIWorth +" retVal="+retVal+" top4Options="+top4Options;
			fileLogTelegramWriter.write( logString);
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
	
	public static void main(String[] args) {
		new G3GreekGapFuturesFilterAlgoThread(23L, null);
	}
	
}
