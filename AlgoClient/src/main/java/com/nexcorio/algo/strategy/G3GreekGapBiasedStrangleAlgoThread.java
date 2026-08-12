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

public class G3GreekGapBiasedStrangleAlgoThread extends G3BaseClass implements Runnable {

	private static final Logger log = LogManager.getLogger(G3PriceParityIVBasedAlgoThread.class);
	
	public float upperDelta = 0.6f;
	public float lowerDelta = 0.4f;
	
	public String greekname = "";
	public String mainGreek4MultiOI = "";
	
	public float  indexRollingPts = 0f;
	
	public boolean maintainBias = false;
	public float oppositeDeltaDiff= 0f;
	
	public G3GreekGapBiasedStrangleAlgoThread(Long napAlgoId, String backTestDateStr) {
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
			
			String lastKnownTrend = "UnKnown";
			
			float indexWhenStrangleFormed = this.instrumentLtp;
			do {
				sleep(5); // Quick to react
				
				this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
				
				OptionGreek ceOptionGreeks = !ceStraddleOptionName.equals("")?getOptionGreeks(ceStraddleOptionName):null;
				OptionGreek peOptionGreeks = !peStraddleOptionName.equals("")?getOptionGreeks(peStraddleOptionName):null;
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
				
				String currentDirection = getSellerDirection(lastKnownTrend);
				
				boolean needRepositioning = false;
				
				if (ceStraddleOptionName.equals("") || !currentDirection.equalsIgnoreCase(lastKnownTrend)) {
					fileLogTelegramWriter.write("Realigning first time or trend change");
					needRepositioning = true; // Just starting, no open positions
				} else if ( indexRollingPts > 0 && (this.instrumentLtp > indexWhenStrangleFormed + indexRollingPts || this.instrumentLtp < indexWhenStrangleFormed - indexRollingPts) ) {
					fileLogTelegramWriter.write("Realigning " + indexRollingPts + " pt range broken. indexWhenStrangleFormed="+indexWhenStrangleFormed+" index now at "+this.instrumentLtp);
					needRepositioning = true;
				} else if (this.maintainBias == true && 
						( ( currentDirection.equals("CE")  && Math.abs(ceOptionGreeks.getDelta()) < Math.abs(peOptionGreeks.getDelta()) )
								|| ( currentDirection.equals("PE")  && Math.abs(peOptionGreeks.getDelta()) < Math.abs(ceOptionGreeks.getDelta()) )
						)
					) {
					fileLogTelegramWriter.write("Realigning maintainBias breached");
					needRepositioning = true;
				} else if(this.oppositeDeltaDiff > 0f) {
					float originalDeltaGap = upperDelta-lowerDelta;
					float currentGap = Math.abs(ceOptionGreeks.getDelta()) - Math.abs(peOptionGreeks.getDelta());
					if (currentDirection.equalsIgnoreCase("PE")) {
						currentGap = Math.abs(peOptionGreeks.getDelta()) - Math.abs(ceOptionGreeks.getDelta());
					}
					if (currentGap > originalDeltaGap+oppositeDeltaDiff) { // 0.15f
						fileLogTelegramWriter.write("Realigning oppositeDeltaDiff breached. currentGap="+currentGap);
						needRepositioning = true;
					}
				}
				
				if (needRepositioning) {
					String[] entryStraddleOptionNames1 = getStraddleOptionNamesByDeltaOptimised(this.upperDelta, this.optimalHedgeDistance); // getStraddleOptionNamesByGreekOptimised("ltp", this.baseDelta, this.optimalHedgeDistance);
					String[] entryStraddleOptionNames2 = getStraddleOptionNamesByDeltaOptimised(this.lowerDelta , 0);
					
					String ceOptionname = entryStraddleOptionNames1[0];
					String peOptionname = entryStraddleOptionNames2[1];
					
					if (currentDirection.equalsIgnoreCase("PE")) {
						ceOptionname = entryStraddleOptionNames2[0];
						peOptionname = entryStraddleOptionNames1[1];
					}
					
					if (!ceStraddleOptionName.equals(ceOptionname)) {
						if (!ceStraddleOptionName.equals(ceOptionname)) {
							if (!ceStraddleOptionName.equals("")) { // Exit and re enter
								fileLogTelegramWriter.write( " Exiting ="+ceStraddleOptionName );
								// Exit CE
								if (this.placeActualOrder) {
									placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
								}
								ceStraddleOptionName = "";
							}
						}
						if (this.noOfOrders<maxAllowedNoOfOrders) {
							ceStraddleOptionName =  ceOptionname;
							float cePrice = getPriceFromTicks(ceStraddleOptionName);
							fileLogTelegramWriter.write( " Entering ="+ceStraddleOptionName +"(@"+cePrice+")");
							// Place order
							ceDbId = createAlgoSellOrder(ceStraddleOptionName, cePrice, noOfLots*lotSize);
							if (this.placeActualOrder) {
								if (ceHedgeOptionName.equals("")) {								
									ceHedgeOptionName =  entryStraddleOptionNames1[2];
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
						if (!peStraddleOptionName.equals(peOptionname)) {
							if (!peStraddleOptionName.equals("")) { // Exit and re enter
								fileLogTelegramWriter.write( " Exiting ="+peStraddleOptionName );
								if (this.placeActualOrder) {
									placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
								}
								peStraddleOptionName = "";
							}
						}
						if (this.noOfOrders<maxAllowedNoOfOrders) {
							peStraddleOptionName =  peOptionname;
							float pePrice = getPriceFromTicks(peStraddleOptionName);
							fileLogTelegramWriter.write( "Entering ="+peStraddleOptionName +"(@"+pePrice+")");
							// Place order
							peDbId = createAlgoSellOrder(peStraddleOptionName, pePrice, noOfLots*lotSize);
							if (this.placeActualOrder) {
								if (peHedgeOptionName.equals("")) {
									peHedgeOptionName =  entryStraddleOptionNames1[3];
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
					indexWhenStrangleFormed = this.instrumentLtp;
				}
				
				lastKnownTrend = currentDirection;
				
				if ( (runningCePrice+runningPePrice)>0 && (runningCePrice+runningPePrice)<10f ) {
					prepareExit( "Nothing much left in premium");
				}
				
				checkExitSignals();
				
				if (exitThread==true) {
					if (!ceStraddleOptionName.equals("")) {
						updateCurrentOrderStatus(ceStraddleOptionName, ceDbId, "LegClosed");
					} 
					if (!peStraddleOptionName.equals("")) {
						updateCurrentOrderStatus(peStraddleOptionName, peDbId, "LegClosed");
					}
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
	
	private String getSellerDirection(String lastKnownTrend) {

		String retVal = lastKnownTrend;
		
		if (greekname.equals("multiOIAltAbove5")) {
			return getSellerDirectionByMultiOI(lastKnownTrend);
		}
		
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
			} else if (greekname.equalsIgnoreCase("resDRAvgIV")) {
				fieldname = "resDeltaRangeCEAvgIv as ceGreek, resDeltaRangePEAvgIv as peGreek";
			} else if (greekname.equalsIgnoreCase("adjCeAtmIv")) {
				fieldname = "adjustedceatmiv as ceGreek, adjustedpeatmiv as peGreek";
			} else if (greekname.equalsIgnoreCase("cumAvgIVDiff")) {
				fieldname = "cumulativeCEAvgIVDiff as ceGreek, cumulativePEAvgIVDiff as peGreek";
			} else if (greekname.equalsIgnoreCase("dr49accmlGama")) {
				fieldname = "dr49accumulatedchangein5seccegamma as ceGreek, dr49accumulatedchangein5secpegamma as peGreek";
			} else if (greekname.equalsIgnoreCase("dr49accmlVega")) {
				fieldname = "dr49accumulatedchangein5seccevega as ceGreek, dr49accumulatedchangein5secpevega as peGreek";
			} else if (greekname.equalsIgnoreCase("dr49accmlDelta")) {
				fieldname = "dr49accumulatedchangein5secpedelta as ceGreek, dr49accumulatedchangein5seccedelta as peGreek";
			} else if (greekname.equalsIgnoreCase("dr49accmlTheta")) {
				fieldname = "dr49accumulatedchangein5seccetheta as ceGreek, dr49accumulatedchangein5secpetheta as peGreek";
			} else if (greekname.equalsIgnoreCase("dr49accmlIv")) {
				fieldname = "dr49accumulatedchangein5secpeIv as ceGreek, dr49accumulatedchangein5secceIv as peGreek";
			} else if (greekname.equalsIgnoreCase("dr49accmlLtp")) {
				fieldname = "dr49accumulatedchangein5secceLtp as ceGreek, dr49accumulatedchangein5secpeLtp as peGreek";
			} else if (greekname.equalsIgnoreCase("dr16accmlGama")) {
				fieldname = "dr16accumulatedchangein5seccegamma as ceGreek, dr16accumulatedchangein5secpegamma as peGreek";
			} else if (greekname.equalsIgnoreCase("dr16accmlVega")) {
				fieldname = "dr16accumulatedchangein5seccevega as ceGreek, dr16accumulatedchangein5secpevega as peGreek";
			} else if (greekname.equalsIgnoreCase("dr16accmlDelta")) {
				fieldname = "dr16accumulatedchangein5seccedelta as ceGreek, dr16accumulatedchangein5secpedelta as peGreek";
			} else if (greekname.equalsIgnoreCase("dr16accmlTheta")) {
				fieldname = "dr16accumulatedchangein5secpetheta as ceGreek, dr16accumulatedchangein5seccetheta as peGreek";
			} else if (greekname.equalsIgnoreCase("dr16accmlIv")) {
				fieldname = "dr16accumulatedchangein5secpeIv as ceGreek, dr16accumulatedchangein5secceIv as peGreek";
			} else if (greekname.equalsIgnoreCase("dr16accmlLtp")) {
				fieldname = "dr16accumulatedchangein5secceLtp as ceGreek, dr16accumulatedchangein5secpeLtp as peGreek";
			} else if (greekname.equalsIgnoreCase("drWhlStrkaccmlGama")) {
				fieldname = "drWhlStrkaccumulatedchangein5seccegamma as ceGreek, drWhlStrkaccumulatedchangein5secpegamma as peGreek";
			} else if (greekname.equalsIgnoreCase("drWhlStrkaccmlVega")) {
				fieldname = "drWhlStrkaccumulatedchangein5seccevega as peGreek, drWhlStrkaccumulatedchangein5secpevega as ceGreek";
			} else if (greekname.equalsIgnoreCase("drWhlStrkaccmlDelta")) {
				fieldname = "drWhlStrkaccumulatedchangein5seccedelta as ceGreek, drWhlStrkaccumulatedchangein5secpedelta as peGreek";
			} else if (greekname.equalsIgnoreCase("drWhlStrkaccmlTheta")) {
				fieldname = "drWhlStrkaccumulatedchangein5secpetheta as peGreek, drWhlStrkaccumulatedchangein5seccetheta as ceGreek";
			} else if (greekname.equalsIgnoreCase("drWhlStrkaccmlIv")) {
				fieldname = "drWhlStrkaccumulatedchangein5secpeIv as peGreek, drWhlStrkaccumulatedchangein5secceIv as ceGreek";
			} else if (greekname.equalsIgnoreCase("drWhlStrkaccmlLtp")) {
				fieldname = "drWhlStrkaccumulatedchangein5secceLtp as ceGreek, drWhlStrkaccumulatedchangein5secpeLtp as peGreek";
			} else if (greekname.equalsIgnoreCase("strk250AvgIv")) {
				fieldname = "strk250CEAvgIv as peGreek, strk250PEAvgIv as ceGreek";
			} else if (greekname.equalsIgnoreCase("outlierStrkDist")) {
				fieldname = "ceOutlierStrikeDistance as peGreek, peOutlierStrikeDistance as ceGreek";
			} else if (greekname.equalsIgnoreCase("delta2_8Count")) {
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
			} else if (greekname.equalsIgnoreCase("itm1000x500AvgIv")) {
				fieldname = "itm1000x500AvgCeIv as ceGreek, itm1000x500AvgPeIv as peGreek";
			} else if (greekname.equalsIgnoreCase("dr19fxdSizAccmlTheta")) {
				fieldname = "dr19fixedSizeAccmlCETheta as ceGreek, dr19fixedSizeAccmlPETheta as peGreek";
			} else if (greekname.equalsIgnoreCase("dr49BalancedDelta")) {
				fieldname = "drCEAvgIV as peGreek, drPEAvgIV as ceGreek";
			} else if (greekname.equalsIgnoreCase("altAbv5WhlStrkTheta")) {
				fieldname = "altAbove5WhlStrkAccmltCETheta as ceGreek, altAbove5WhlStrkAccmltPETheta as peGreek";
			} else if (greekname.equalsIgnoreCase("futureOutstanding")) {
				fieldname = "future_Outstanding_Volume as peGreek, 0 as ceGreek";
			} else if (greekname.equalsIgnoreCase("tmpaccmltheta")) {
				//fieldname = "dr16accumulatedchangein5secpetheta as peGreek, dr16accumulatedchangein5seccetheta as ceGreek";
				fieldname = "allITMCeAvgIv as ceGreek, allITMPeAvgIv as peGreek";
			} else if (greekname.equalsIgnoreCase("tmpaccmlvega")) {
				fieldname = "tmpaccmlcetheta/tmpaccmlcegamma as ceGreek, tmpaccmlpetheta/tmpaccmlpegamma as peGreek";
			} else if (greekname.equalsIgnoreCase("tmpaccmlgamma")) {
				fieldname = "tmpaccmlcevega/tmpaccmlcegamma as ceGreek, tmpaccmlpevega/tmpaccmlpegamma as peGreek";
			} else if (greekname.equalsIgnoreCase("drWhlStrkaccmlVegGam")) {
				fieldname = "drWhlStrkaccumulatedchangein5secpevega/drWhlStrkaccumulatedchangein5secpegamma as peGreek, "
						  + "drWhlStrkaccumulatedchangein5seccevega/drWhlStrkaccumulatedchangein5seccegamma as ceGreek";
			} else if (greekname.equalsIgnoreCase("drWhlStrkacmlThetGam")) {
				fieldname = "drWhlStrkaccumulatedchangein5secpetheta/drWhlStrkaccumulatedchangein5secpegamma as peGreek, "
						+ "drWhlStrkaccumulatedchangein5seccetheta/drWhlStrkaccumulatedchangein5seccegamma as ceGreek";
			} else if (greekname.equalsIgnoreCase("range350AvgGamma")) {
				fieldname = "range350CEAvgGamma as peGreek, range350PEAvgGamma  as ceGreek";
			} else if (greekname.equalsIgnoreCase("range350AvgIv")) {
				fieldname = "range350CEAvgIv as peGreek, range350PEAvgIv as ceGreek";
			} else if (greekname.equalsIgnoreCase("range350AvgTheta")) {
				fieldname = "range350CEAvgTheta as ceGreek, range350PEAvgTheta as peGreek";
			} else if (greekname.equalsIgnoreCase("range350AvgVega")) {
				fieldname = "range350CEAvgVega as peGreek, range350PEAvgVega as ceGreek";
			} else if (greekname.equalsIgnoreCase("tmp250x750accmlTheta")) {
				fieldname = "otm250x500accmlcetheta as peGreek, otm250x500accmlpetheta as ceGreek";
			} else if (greekname.equalsIgnoreCase("tmp0x350AvgIv")) {
				fieldname = "itm0_350CEAvgIv as ceGreek, itm0_350PEAvgIv as peGreek";
			} else if (greekname.equalsIgnoreCase("otm250x750AccmlTheta")) {
				fieldname = "otm250x750AccmlCeTheta as ceGreek, otm250x750AccmlPeTheta as peGreek";
			} else if (greekname.equalsIgnoreCase("altAbv5AvgTimevalue")) {
				fieldname = "altAbove5WhlStrkCEAvgTimevalue as ceGreek, altAbove5WhlStrkPEAvgTimevalue as peGreek";
			} else if (greekname.equalsIgnoreCase("fullOtm0x600OI")) {
				fieldname = "fullOtm0x600CEGreeks as ceGreek, fullOtm0x600PEGreeks as peGreek";
			} else if (greekname.equalsIgnoreCase("fullOtm0x200OI")) {
				fieldname = "full200CEList as ceGreek, full200PEList as peGreek";
			} else if (greekname.equalsIgnoreCase("fullOtm50x400OI")) {
				fieldname = "fullOtm50x400CEGreeks as ceGreek, fullOtm50x400PEGreeks as peGreek";
			} else if (greekname.equalsIgnoreCase("lowerOtm0x300Greeks")) {
				fieldname = "lowerOtm0x300CEGreeks as ceGreek, lowerOtm0x300PEGreeks as peGreek";
			}  
			
			Integer instrumentIdToUse = this.mainInstrument.getId().intValue();
			
			String fetchSql = "select " + fieldname + " from nexcorio_option_atm_movement_data where f_main_instrument = " + instrumentIdToUse + ""
					+ " and record_time <= '" + postgresLongDateFormat.format(getCurrentTime()) + "'"
					+ " order by record_time desc limit 5";
			fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
			ResultSet rs = stmt.executeQuery(fetchSql);
			
			int gapCount = 0;
			int gapCountWithoutAdjust = 0;
			while (rs.next()) {
				float ceGreek = rs.getFloat("ceGreek");
				float peGreek = rs.getFloat("peGreek");
				
				if (ceGreek>=peGreek) {
					gapCount++;
				}
				fileLogTelegramWriter.write("ceGreek="+ceGreek+" peGreek="+peGreek+" gapCount="+gapCount);
			}
			rs.close();			
			stmt.close();
			
			fileLogTelegramWriter.write("gapCpunt=" + gapCount + " gapCountWithoutAdjust=" + gapCountWithoutAdjust );
			
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
	
	private String getSellerDirectionByMultiOI(String lastKnownOptiontrend) {
		String retVal = lastKnownOptiontrend;
		
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			Integer instrumentIdToUse = this.mainInstrument.getId().intValue();
			
			String greekSelectFields = " fullOtm0x600CEGreeks as ceGreek, fullOtm0x600PEGreeks as peGreek";
			if (mainGreek4MultiOI.equalsIgnoreCase("fullOtm0x600")) {
				greekSelectFields = " fullOtm0x600CEGreeks as ceGreek, fullOtm0x600PEGreeks as peGreek";
			} else if (mainGreek4MultiOI.equalsIgnoreCase("fullOtm0x500")) {
				greekSelectFields = " fullOtm0x500CEGreeks as ceGreek, fullOtm0x500PEGreeks as peGreek";
			} else if (mainGreek4MultiOI.equalsIgnoreCase("fullOtm50x400")) {
				greekSelectFields = " fullOtm50x400CEGreeks as ceGreek, fullOtm50x400PEGreeks as peGreek";
			} else if (mainGreek4MultiOI.equalsIgnoreCase("deltaRangeAvgIV")) {
				greekSelectFields = " deltarangeceavgiv as ceGreek, deltarangepeavgiv as peGreek";
			} else if (mainGreek4MultiOI.equalsIgnoreCase("drWhlStrkaccmlTheta")) {
				greekSelectFields = "drWhlStrkaccumulatedchangein5secpetheta as peGreek, drWhlStrkaccumulatedchangein5seccetheta as ceGreek";
			} else if (mainGreek4MultiOI.equalsIgnoreCase("dr49accmlLtp")) {
				greekSelectFields = "dr49accumulatedchangein5secceLtp as ceGreek, dr49accumulatedchangein5secpeLtp as peGreek";
			}
			String fetchSql = "select " + greekSelectFields + ", altabove5WhlStrkCEAvgIv, altabove5WhlStrkPEAvgIv from nexcorio_option_atm_movement_data where f_main_instrument = " + instrumentIdToUse + ""
					+ " and record_time <= '" + postgresLongDateFormat.format(getCurrentTime()) + "'"
					+ " order by record_time desc limit 5";
			fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
			ResultSet rs = stmt.executeQuery(fetchSql);
			
			float ceGreek = 0f;
			float peGreek = 0f;
			
			int altGapCount = 0;
			while (rs.next()) {
				ceGreek = ceGreek + rs.getFloat("ceGreek");
				peGreek = peGreek + rs.getFloat("peGreek");
				
				float altCeGreek = rs.getFloat("altabove5WhlStrkCEAvgIv");
				float altPeGreek = rs.getFloat("altabove5WhlStrkPEAvgIv");
				
				if (altCeGreek > altPeGreek) altGapCount++;
			}
			rs.close();			
			stmt.close();
			
			ceGreek = ceGreek/5f;
			peGreek = peGreek/5f;
			
			float fullPercentDiff = 0f;
			String directionByFullOI = ""; 
					
			if (ceGreek > peGreek) {
				fullPercentDiff = (ceGreek-peGreek)*100f/peGreek;
				directionByFullOI = "CE";
			} else {
				fullPercentDiff = (peGreek - ceGreek)*100f/ceGreek;
				directionByFullOI = "PE";
			}
			
			String directionAltAbove5 = ""; 
			if (altGapCount == 0) {
				directionAltAbove5 = "PE";
			} else if (altGapCount == 5) {
				directionAltAbove5 = "CE";
			}
			
			if (directionAltAbove5.equals(directionByFullOI)) {
				retVal = directionAltAbove5;
			} else {
				if (fullPercentDiff > 20f) {
					retVal = directionByFullOI;
				} else {
					retVal = directionAltAbove5;
				}
			}
			fileLogTelegramWriter.write("fullOtm0x600CEGreeks="+ceGreek+" fullOtm0x600PEGreeks="+peGreek+" fullPercentDiff="+fullPercentDiff+" directionByFullOI="+directionByFullOI+" directionAltAbove5="+directionAltAbove5 + "retVal="+ retVal);
			
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
}
