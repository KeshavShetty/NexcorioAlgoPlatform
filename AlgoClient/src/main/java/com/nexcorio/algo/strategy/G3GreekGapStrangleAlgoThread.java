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

public class G3GreekGapStrangleAlgoThread extends G3BaseClass implements Runnable{

	private static final Logger log = LogManager.getLogger(G3GreekGapStrangleAlgoThread.class);
	
	public String greekname = "iv";
	public float baseDelta = 0.5f;
	public float minGap = 1f;
	
	public float adjustGap = 0f;
	
	public G3GreekGapStrangleAlgoThread(Long napAlgoId, String backTestDateStr) {
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
				
				String currentTrend = getSellerDirectionByATMGreekGap(lastKnownTrend, minGap); // StatusQuo, CE, PE
				
				if (!currentTrend.equals(lastKnownTrend)) {
					if (currentTrend.equals("CE") || currentTrend.equals("PE")) {
				
						String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(baseDelta, this.optimalHedgeDistance);
						
						if (currentTrend.equals("CE")) { // Exit PE, Enter CE
							if (!peStraddleOptionName.equals("")) { // Exit PE, taking Directional
								fileLogTelegramWriter.write( " Exiting ="+peStraddleOptionName );
								// Exit PE
								if (this.placeActualOrder) {
									placeRealOrder( peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
								}
								peStraddleOptionName = "";
							}
							if (ceStraddleOptionName.equals("")) {
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
								} else {
									prepareExit("Too many orders");
								}
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
							if (peStraddleOptionName.equals("")) {
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
								} else {
									prepareExit("Too many orders");
								}
							}
						}
					} else { // Neutral
						float deltaToUse = baseDelta;
						if (ceOptionGreeks!=null) deltaToUse = Math.abs(ceOptionGreeks.getDelta());
						else if (peOptionGreeks!=null) deltaToUse = Math.abs(peOptionGreeks.getDelta());
						
						String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(deltaToUse, this.optimalHedgeDistance);
						
						if (ceStraddleOptionName.equals("")) {
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
							} else {
								prepareExit("Too many orders");
							}
						}
						if (peStraddleOptionName.equals("")) {
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
							} else {
								prepareExit("Too many orders");
							}
						}
					}
				}
					
				lastKnownTrend = currentTrend;
				
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
	
	private String getSellerDirectionByATMGreekGap(String lastKnownTrend, float minGap) {
		String retVal = lastKnownTrend;
		
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			String fieldname = "ceiv as ceGreek, peiv as peGreek";
			if (this.greekname.equalsIgnoreCase("ltp")) {
				fieldname = "celtp as ceGreek, peltp as peGreek";
			} else if (this.greekname.equalsIgnoreCase("gamma")) {
				fieldname = "cegamma as ceGreek, pegamma as peGreek";
			} else if (this.greekname.equalsIgnoreCase("avggamma")) {
				fieldname = "avgcegamma as ceGreek, avgpegamma as peGreek";
			} else if (this.greekname.equalsIgnoreCase("selectiveavggamma")) {
				fieldname = "selectivestrike_avgcegamma as ceGreek, selectivestrike_avgpegamma as peGreek";
			} else if (this.greekname.equalsIgnoreCase("avgiv")) {
				fieldname = "totalceiv as ceGreek, totalpeiv as peGreek";
			} else if (this.greekname.equalsIgnoreCase("deltaRangeAvgIV")) {
				fieldname = "deltarangeceavgiv as ceGreek, deltarangepeavgiv as peGreek";
			} else if (this.greekname.equalsIgnoreCase("deltaRangeAvgGamma")) {
				fieldname = "deltarangeceavggamma as ceGreek, deltarangepeavggamma as peGreek";
			} else if (this.greekname.equalsIgnoreCase("deltaRangeAvgLtp")) {
				fieldname = "deltarangepeavgltp as ceGreek, deltarangeceavgltp as peGreek"; // For ltp peltp as cegreek (similar to gamma reverse)
			} else if (this.greekname.equalsIgnoreCase("deltaRangeDeltaOI")) {
				fieldname = "deltarangecedeltaoi as ceGreek, deltarangepedeltaoi as peGreek";
			} else if (this.greekname.equalsIgnoreCase("deltaRangeGmaOI")) { // Gamma name removed as it will inverse with word gamma
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
			} else if (greekname.equalsIgnoreCase("drTop5DeltaOI")) {
				fieldname = "ceDeltaOIWorth as ceGreek, peDeltaOIWorth as peGreek";
			} else if (greekname.equalsIgnoreCase("dr49TotalIV")) {
				fieldname = "outliercemedianiv-outlierceminiv as ceGreek, outlierpemedianiv-outlierpeminiv as peGreek";
			} else if (greekname.equalsIgnoreCase("dr19WholeStrikeAvgIV")) {
				fieldname = "dr19WholeStrikeCEAvgIV as ceGreek, dr19WholeStrikePEAvgIV as peGreek";
			} else if (greekname.equalsIgnoreCase("gammaExposure")) {
				fieldname = "maxGammaExposure as ceGreek, minGammaExposure as peGreek";
			} else if (greekname.equalsIgnoreCase("netGammaExposure")) {
				fieldname = "netGammaExposure as ceGreek, netGammaExposure as peGreek";
			} else if (greekname.equalsIgnoreCase("accmltd5secIVChg")) {
				fieldname = "accumulatedChangein5secCeIV as ceGreek, accumulatedChangein5secPeIV as peGreek";
			} else if (greekname.equalsIgnoreCase("dr1-6AvgIv")) {
				fieldname = "dr1_6CEAvgIv as ceGreek, dr1_6PEAvgIv as peGreek";
			} else if (greekname.equalsIgnoreCase("gammaExposureWthStrk")) {
				fieldname = "maxGammaExposureWithStrike as ceGreek, minGammaExposureWithStrike as peGreek";
			} else if (greekname.equalsIgnoreCase("netGamaExpWthStrk")) {
				fieldname = "netgammaexposurewithstrike as ceGreek, netgammaexposurewithstrike as peGreek";
			} else if (greekname.equalsIgnoreCase("netGameXpTopN")) {
				fieldname = "netgammaexposuretopn as ceGreek, netgammaexposuretopn as peGreek";
			} else if (this.greekname.equalsIgnoreCase("resDRAvgIV")) {
				fieldname = "resDeltaRangeCEAvgIv as ceGreek, resDeltaRangePEAvgIv as peGreek";
			} else if (this.greekname.equalsIgnoreCase("adjCeAtmIv")) {
				fieldname = "adjustedceatmiv as ceGreek, adjustedpeatmiv as peGreek";
			} else if (this.greekname.equalsIgnoreCase("cumAvgIVDiff")) {
				fieldname = "cumulativeCEAvgIVDiff as ceGreek, cumulativePEAvgIVDiff as peGreek";
			} else if (this.greekname.equalsIgnoreCase("dr49accmlGama")) {
				fieldname = "dr49accumulatedchangein5seccegamma as ceGreek, dr49accumulatedchangein5secpegamma as peGreek";
			} else if (this.greekname.equalsIgnoreCase("dr49accmlVega")) {
				fieldname = "dr49accumulatedchangein5seccevega as ceGreek, dr49accumulatedchangein5secpevega as peGreek";
			} else if (this.greekname.equalsIgnoreCase("dr49accmlDelta")) {
				fieldname = "dr49accumulatedchangein5secpedelta as ceGreek, dr49accumulatedchangein5seccedelta as peGreek";
			} else if (this.greekname.equalsIgnoreCase("dr49accmlTheta")) {
				fieldname = "dr49accumulatedchangein5seccetheta as ceGreek, dr49accumulatedchangein5secpetheta as peGreek";
			} else if (this.greekname.equalsIgnoreCase("dr49accmlIv")) {
				fieldname = "dr49accumulatedchangein5secpeIv as ceGreek, dr49accumulatedchangein5secceIv as peGreek";
			} else if (this.greekname.equalsIgnoreCase("dr49accmlLtp")) {
				fieldname = "dr49accumulatedchangein5secceLtp as ceGreek, dr49accumulatedchangein5secpeLtp as peGreek";
			} else if (this.greekname.equalsIgnoreCase("dr16accmlGama")) {
				fieldname = "dr16accumulatedchangein5seccegamma as ceGreek, dr16accumulatedchangein5secpegamma as peGreek";
			} else if (this.greekname.equalsIgnoreCase("dr16accmlVega")) {
				fieldname = "dr16accumulatedchangein5seccevega as ceGreek, dr16accumulatedchangein5secpevega as peGreek";
			} else if (this.greekname.equalsIgnoreCase("dr16accmlDelta")) {
				fieldname = "dr16accumulatedchangein5seccedelta as ceGreek, dr16accumulatedchangein5secpedelta as peGreek";
			} else if (this.greekname.equalsIgnoreCase("dr16accmlTheta")) {
				fieldname = "dr16accumulatedchangein5secpetheta as ceGreek, dr16accumulatedchangein5seccetheta as peGreek";
			} else if (this.greekname.equalsIgnoreCase("dr16accmlIv")) {
				fieldname = "dr16accumulatedchangein5secpeIv as ceGreek, dr16accumulatedchangein5secceIv as peGreek";
			} else if (this.greekname.equalsIgnoreCase("dr16accmlLtp")) {
				fieldname = "dr16accumulatedchangein5secceLtp as ceGreek, dr16accumulatedchangein5secpeLtp as peGreek";
			} else if (this.greekname.equalsIgnoreCase("drWhlStrkaccmlGama")) {
				fieldname = "drWhlStrkaccumulatedchangein5seccegamma as ceGreek, drWhlStrkaccumulatedchangein5secpegamma as peGreek";
			} else if (this.greekname.equalsIgnoreCase("drWhlStrkaccmlVega")) {
				fieldname = "drWhlStrkaccumulatedchangein5seccevega as ceGreek, drWhlStrkaccumulatedchangein5secpevega as peGreek";
			} else if (this.greekname.equalsIgnoreCase("drWhlStrkaccmlDelta")) {
				fieldname = "drWhlStrkaccumulatedchangein5seccedelta as ceGreek, drWhlStrkaccumulatedchangein5secpedelta as peGreek";
			} else if (this.greekname.equalsIgnoreCase("drWhlStrkaccmlTheta")) {
				fieldname = "drWhlStrkaccumulatedchangein5secpetheta as peGreek, drWhlStrkaccumulatedchangein5seccetheta as ceGreek";
			} else if (this.greekname.equalsIgnoreCase("drWhlStrkaccmlIv")) {
				fieldname = "drWhlStrkaccumulatedchangein5secpeIv as peGreek, drWhlStrkaccumulatedchangein5secceIv as ceGreek";
			} else if (this.greekname.equalsIgnoreCase("drWhlStrkaccmlLtp")) {
				fieldname = "drWhlStrkaccumulatedchangein5secceLtp as ceGreek, drWhlStrkaccumulatedchangein5secpeLtp as peGreek";
			} else if (this.greekname.equalsIgnoreCase("strk250AvgIv")) {
				fieldname = "strk250CEAvgIv as peGreek, strk250PEAvgIv as ceGreek";
			} else if (this.greekname.equalsIgnoreCase("outlierStrkDist")) {
				fieldname = "ceOutlierStrikeDistance as peGreek, peOutlierStrikeDistance as ceGreek";
			} else if (this.greekname.equalsIgnoreCase("delta2_8Count")) {
				fieldname = "ceDelta8_9Count as ceGreek, peDelta8_9Count as peGreek";
			} else if (greekname.equalsIgnoreCase("gammaExposureTopN")) {
				fieldname = "maxGammaExposureTopN as ceGreek, minGammaExposureTopN as peGreek";
			} else if (greekname.equalsIgnoreCase("drITMWhlStrkAvgIv")) {
				fieldname = "drITMWhlStrkSameSizeCEAvgIv as ceGreek, drITMWhlStrkSameSizePEAvgIv as peGreek"; // fieldname = "dr49WhlStrkaccumulatedchangein5secceGamma as ceGreek, dr49WhlStrkaccumulatedchangein5secpeGamma as peGreek"; //
			} else if (greekname.equalsIgnoreCase("above5WhlStrkAvgIv")) {
				fieldname = "above5WhlStrkCEAvgIv as ceGreek, above5WhlStrkPEAvgIv as peGreek"; // fieldname = "dr49WhlStrkaccumulatedchangein5secceGamma as ceGreek, dr49WhlStrkaccumulatedchangein5secpeGamma as peGreek"; //
			} else if (greekname.equalsIgnoreCase("altAbov5WhlStrkAvgIv")) {
				fieldname = "altabove5WhlStrkCEAvgIv as ceGreek, altabove5WhlStrkPEAvgIv as peGreek"; // fieldname = "dr49WhlStrkaccumulatedchangein5secceGamma as ceGreek, dr49WhlStrkaccumulatedchangein5secpeGamma as peGreek"; //
			} else if (greekname.equalsIgnoreCase("abv5WhlStrkCEAvgGama")) {
				fieldname = "altAbove5WhlStrkCEAvgGama as ceGreek, altAbove5WhlStrkPEAvgGama as peGreek"; // fieldname = "dr49WhlStrkaccumulatedchangein5secceGamma as ceGreek, dr49WhlStrkaccumulatedchangein5secpeGamma as peGreek"; //
			}
			
			Integer instrumentIdToUse = this.mainInstrument.getId().intValue();
			
			String fetchSql = "select " + fieldname + " from nexcorio_option_atm_movement_data where f_main_instrument = " + instrumentIdToUse + ""
					+ " and record_time <= '" + postgresLongDateFormat.format(getCurrentTime()) + "'"
					+ " order by record_time desc limit 1";
			fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
			ResultSet rs = stmt.executeQuery(fetchSql);
			
			
			while (rs.next()) {
				float ceGreek = rs.getFloat("ceGreek");
				float peGreek = rs.getFloat("peGreek");
				
				if (Math.abs(ceGreek-peGreek) < minGap-2) {
					retVal = "Neutral";
				} else {
					if ( ceGreek-peGreek >= minGap ) {
						retVal = "CE";
					}
					if ( peGreek-ceGreek >= minGap ) {
						retVal = "PE";
					}
				}
				fileLogTelegramWriter.write(" ceGreek=" + ceGreek + " peGreek=" + peGreek +" retVal="+retVal);
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
			
		return retVal;
	}
	
	public static void main(String[] args) {
		new G3GreekGapStrangleAlgoThread(23L, null);
	}

}
