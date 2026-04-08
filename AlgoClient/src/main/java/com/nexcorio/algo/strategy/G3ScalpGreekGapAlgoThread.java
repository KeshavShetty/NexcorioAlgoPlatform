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

public class G3ScalpGreekGapAlgoThread extends G3BaseClass implements Runnable{

	private static final Logger log = LogManager.getLogger(G3ScalpGreekGapAlgoThread.class);
	
	public String greekname = "altAbov5WhlStrkAvgIv";
	public float baseDelta = 0.5f;
	
	public float percentDiff = 20f;
	public float targetPts = 25f;
	public float stoplossPts = 25f;
	public int exitMinute = 15;
	
	public boolean useScaledDelta = true;
	
	public G3ScalpGreekGapAlgoThread(Long napAlgoId, String backTestDateStr) {
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
			
			String ceBuyOptionname = "";
			String peBuyOptionname = "";
			
			this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
			fileLogTelegramWriter.write( " this.instrumentLtp="+this.instrumentLtp);
			
			
			float maxProfitReached = 0f;
			Date maxProfitReachedAt = getCurrentTime();
			float maxLowestpointReached = 0f;
			Date maxLowestpointReachedAt = getCurrentTime();
			float maxTrailingProfit = 0f;
			
			updateAlgoStatus("Running");
			
			float purchasePrice = 0f;
			Date exitAt = getCurrentTime();
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
				
				String currentTrend = getBuyerDirectionByATMGreekGap(); // StatusQuo, CE, PE
				
				if (currentTrend.equals("PE") || currentTrend.equals("CE")) {
					if (currentTrend.equals("PE")) { // Exit CE, enter PE
						if (!ceBuyOptionname.equals("")) {
							fileLogTelegramWriter.write( " Exiting ="+ceBuyOptionname );
							// Exit PE
							if (this.placeActualOrder) {
								placeRealOrder( ceDbId, ceBuyOptionname, noOfLots*lotSize, "SELL", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
							ceBuyOptionname = "";
						}
						if (peBuyOptionname.equals("")) { // no position exist
							String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(baseDelta, this.optimalHedgeDistance);
							peBuyOptionname = entryStraddleOptionNames[1];							
							purchasePrice = getPriceFromTicks(peBuyOptionname);
							fileLogTelegramWriter.write( " Entering ="+peBuyOptionname +"@" + purchasePrice);
							exitAt = getCurrentTime(this.exitMinute);
							peDbId = createAlgoBuyOrder(peBuyOptionname, purchasePrice, noOfLots*lotSize);
							if (this.placeActualOrder) {
								placeRealOrder( peDbId, peBuyOptionname, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
						}
					} else { // PE -> Exit PE, enter CE
						if (!peBuyOptionname.equals("")) {
							fileLogTelegramWriter.write( " Exiting ="+peBuyOptionname );
							// Exit PE
							if (this.placeActualOrder) {
								placeRealOrder( peDbId, peBuyOptionname, noOfLots*lotSize, "SELL", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
							peBuyOptionname = "";
						}
						if (ceBuyOptionname.equals("")) { // no position exist
							String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(baseDelta, this.optimalHedgeDistance);
							ceBuyOptionname = entryStraddleOptionNames[0];
							purchasePrice = getPriceFromTicks(ceBuyOptionname);
							fileLogTelegramWriter.write( " Entering ="+ceBuyOptionname +"@" + purchasePrice);
							exitAt = getCurrentTime(this.exitMinute);
							ceDbId = createAlgoBuyOrder(ceBuyOptionname, purchasePrice, noOfLots*lotSize);
							if (this.placeActualOrder) {
								placeRealOrder( ceDbId, ceBuyOptionname, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
						}
					}
				} 
				
				// Check existing position, exit after 15min
				if ( !ceBuyOptionname.equals("") || !peBuyOptionname.equals("")) {
					float currentPrice = getPriceFromTicks(!ceBuyOptionname.equals("")?ceBuyOptionname:peBuyOptionname);
					
					fileLogTelegramWriter.write( "currentPrice="+currentPrice+" purchasePrice="+purchasePrice+" getCurrentTime="+getCurrentTime()+" exitAt="+exitAt);
					
					if (currentPrice-purchasePrice > this.targetPts 
							|| currentPrice-purchasePrice < this.stoplossPts
							|| getCurrentTime().after(exitAt)) {
						if (!ceBuyOptionname.equals("")) {
							fileLogTelegramWriter.write( " Exiting ="+ceBuyOptionname );
							// Exit PE
							if (this.placeActualOrder) {
								placeRealOrder( ceDbId, ceBuyOptionname, noOfLots*lotSize, "SELL", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
							ceBuyOptionname = "";
						}
						if (!peBuyOptionname.equals("")) {
							fileLogTelegramWriter.write( " Exiting ="+peBuyOptionname );
							// Exit PE
							if (this.placeActualOrder) {
								placeRealOrder( peDbId, peBuyOptionname, noOfLots*lotSize, "SELL", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
							peBuyOptionname = "";
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
	
	private String getBuyerDirectionByATMGreekGap() {
		String retVal = "Unknown";
		
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
			} else if (greekname.equalsIgnoreCase("dr19fixedSizeAvgIV")) {
				fieldname = "dr19fixedSizeCEAvgIV as ceGreek, dr19fixedSizePEAvgIV as peGreek"; // fieldname = "dr49WhlStrkaccumulatedchangein5secceGamma as ceGreek, dr49WhlStrkaccumulatedchangein5secpeGamma as peGreek"; //
			} else if (greekname.equalsIgnoreCase("whlStrkATMAccmlTheta")) {
				fieldname = "whlStrkOTMAccmlCETheta as ceGreek, whlStrkOTMAccmlPETheta as peGreek"; // fieldname = "dr49WhlStrkaccumulatedchangein5secceGamma as ceGreek, dr49WhlStrkaccumulatedchangein5secpeGamma as peGreek"; //
			} else if (greekname.equalsIgnoreCase("otmX_YAvgIv")) {
				fieldname = "otm250_0AccmlCeTheta as peGreek, "
						+ " otm250_0AccmlPeTheta as ceGreek"; // fieldname = "dr49WhlStrkaccumulatedchangein5secceGamma as ceGreek, dr49WhlStrkaccumulatedchangein5secpeGamma as peGreek"; //
			} else if (greekname.equalsIgnoreCase("1000_750AccmlTheta")) {
				fieldname = "(otm1000_750avgCeiv+otm750_500avgCeiv)/2  as ceGreek, (otm1000_750avgpeiv+otm750_500avgpeiv)/2  as peGreek"; // fieldname = "dr49WhlStrkaccumulatedAccmlceGamma as ceGreek, dr49WhlStrkaccumulatedAccmlpeGamma as peGreek"; //
			} else if (greekname.equalsIgnoreCase("750_500AccmlTheta")) {
				fieldname = "otm750_500avgCeiv  as ceGreek, otm750_500avgPeiv  as peGreek"; // fieldname = "dr49WhlStrkaccumulatedAccmlceGamma as ceGreek, dr49WhlStrkaccumulatedAccmlpeGamma as peGreek"; //
			} else if (greekname.equalsIgnoreCase("500_250AccmlTheta")) {
				fieldname = "otm500_250avgCeiv  as ceGreek, otm500_250avgPeiv  as peGreek"; // fieldname = "dr49WhlStrkaccumulatedAccmlceGamma as ceGreek, dr49WhlStrkaccumulatedAccmlpeGamma as peGreek"; //
			} else if (greekname.equalsIgnoreCase("250_0AccmlTheta")) {
				fieldname = "otm250_0avgCeiv  as ceGreek, otm250_0avgPeiv  as peGreek"; // fieldname = "dr49WhlStrkaccumulatedAccmlceGamma as ceGreek, dr49WhlStrkaccumulatedAccmlpeGamma as peGreek"; //
			} else if (greekname.equalsIgnoreCase("0_250AccmlTheta")) {
				fieldname = "otm0_250avgCeiv  as ceGreek, otm0_250avgPeiv  as peGreek"; // fieldname = "dr49WhlStrkaccumulatedAccmlceGamma as ceGreek, dr49WhlStrkaccumulatedAccmlpeGamma as peGreek"; //
			} else if (greekname.equalsIgnoreCase("250_500AccmlTheta")) {
				fieldname = "otm250_500avgCeiv  as ceGreek, otm250_500avgPeiv  as peGreek"; // fieldname = "dr49WhlStrkaccumulatedAccmlceGamma as ceGreek, dr49WhlStrkaccumulatedAccmlpeGamma as peGreek"; //
			} else if (greekname.equalsIgnoreCase("500_750AccmlTheta")) {
				fieldname = "otm500_750avgCeiv  as ceGreek, otm500_750avgPeiv  as peGreek"; // fieldname = "dr49WhlStrkaccumulatedAccmlceGamma as ceGreek, dr49WhlStrkaccumulatedAccmlpeGamma as peGreek"; //
			} else if (greekname.equalsIgnoreCase("750_1000AccmlTheta")) {
				fieldname = "otm750_1000avgCeiv  as ceGreek, otm750_1000avgPeiv  as peGreek"; // fieldname = "dr49WhlStrkaccumulatedAccmlceGamma as ceGreek, dr49WhlStrkaccumulatedAccmlpeGamma as peGreek"; //
			} else if (greekname.equalsIgnoreCase("newGamaExpStrk")) {
				fieldname = "maxGammaExposure0_250StrikeDistance as ceGreek, minGammaExposure0_250StrikeDistance as peGreek";
			} else if (greekname.equalsIgnoreCase("otm250x750AccmlTheta")) {
				fieldname = "otm250x750AccmlCeTheta as ceGreek, otm250x750AccmlPeTheta as peGreek";
			} else if (greekname.equalsIgnoreCase("itm1000x500AvgIv")) {
				fieldname = "itm1000x500AvgCeIv as ceGreek, itm1000x500AvgPeIv as peGreek";
			} else if (greekname.equalsIgnoreCase("dr19fxdSizAccmlTheta")) {
				fieldname = "dr19fixedSizeAccmlCETheta as ceGreek, dr19fixedSizeAccmlPETheta as peGreek";
			}
			
			Integer instrumentIdToUse = this.mainInstrument.getId().intValue();
						
			String fetchSql = "select " + fieldname + " from nexcorio_option_atm_movement_data where f_main_instrument = " + instrumentIdToUse + ""
					+ " and record_time <= '" + postgresLongDateFormat.format(getCurrentTime()) + "'"
					+ " order by record_time desc limit 1";
			fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
			ResultSet rs = stmt.executeQuery(fetchSql);
			
			float currentCEGreek = 0f;
			float currentPEGreek = 0f;
			
			while (rs.next()) {
				currentCEGreek = rs.getFloat("ceGreek");
				currentPEGreek = rs.getFloat("peGreek");
			}
			rs.close();
			
			fetchSql = "select " + fieldname + " from nexcorio_option_atm_movement_data where f_main_instrument = " + instrumentIdToUse + ""
					+ " and record_time <= '" + postgresLongDateFormat.format(getCurrentTime(-2)) + "'"
					+ " order by record_time desc limit 1";
			fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
			rs = stmt.executeQuery(fetchSql);
			
			float prevCEGreek = 0f;
			float prevPEGreek = 0f;
			
			while (rs.next()) {
				prevCEGreek = rs.getFloat("ceGreek");
				prevPEGreek = rs.getFloat("peGreek");
			}
			rs.close();
			
			float cePercentDiff = getPercentDiff(prevCEGreek, currentCEGreek);
			float pePercentDiff = getPercentDiff(prevPEGreek, currentPEGreek);
			
			//if ( cePercentDiff > this.percentDiff || pePercentDiff > this.percentDiff ) {
			if (cePercentDiff+pePercentDiff > 1.5f*this.percentDiff) {
				if ( cePercentDiff > this.percentDiff) {
					if (currentCEGreek > prevCEGreek) retVal = "PE";
					else retVal = "CE";
				} else if ( pePercentDiff > this.percentDiff) {
					if (currentPEGreek > prevPEGreek) retVal = "CE";
					else retVal = "PE";
				}
			}
			stmt.close();
			
			fileLogTelegramWriter.write("cePercentDiff="+cePercentDiff+" pePercentDiff="+pePercentDiff + " currentCEGreek=" + currentCEGreek + " currentPEGreek=" + currentPEGreek + " prevCEGreek="+prevCEGreek+" prevPEGreek"+prevPEGreek);
			
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
		new G3ScalpGreekGapAlgoThread(23L, null);
	}

	

}
