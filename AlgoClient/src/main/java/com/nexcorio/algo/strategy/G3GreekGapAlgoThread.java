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

public class G3GreekGapAlgoThread extends G3BaseClass implements Runnable{

	private static final Logger log = LogManager.getLogger(G3GreekGapAlgoThread.class);
	
	public String greekname = "iv";
	public float baseDelta = 0.5f;	
	public boolean inverse = false;
	public float adjustGap = 0.0f;
	public boolean adjustPosition = false; // Adjust position after steep fall
	
	public boolean oiWorthDirection = false;
	public Integer dependentInstrumentId = null;
	
	public int peCheckCEOutlier = 0;
	
	public boolean resetAdjustGap = false;
	
	public boolean useScaledDelta = true;
	
	public boolean avoidTurbulace = false;
	
	public float ivTolerance = 0.5f;
	
	private float straddleAt920 = -1f;
	private float ceGammaExposureAt920 = -1f;
	private float peGammaExposureAt920 = -1f;
	private boolean gammaExposureAt920Set = false;
	
	private float lowetStrikeCeAvgIVAt920 = -1f;
	private float lowetStrikePeAvgIVAt920 = -1f;
	private boolean lowetStrikeValuesSet = false;
	
	public String mainGreek4MultiOI = "";
	
	public G3GreekGapAlgoThread(Long napAlgoId, String backTestDateStr) {
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
			fileLogTelegramWriter.write( " this.instrumentLtp="+this.instrumentLtp+" quickGainNotifTarget="+quickGainNotifTarget);
			
			
			String lastKnownTrend = "Unknown";
			
			float soldPrice = 0f;
			String currentTrend = null;
			do {
				currentTrend = getSellerDirectionByATMGreekGap(this.greekname, lastKnownTrend);
				if (currentTrend.equals("Unknown")) sleep(15);
				checkExitSignals();
			} while (currentTrend.equals(lastKnownTrend) && this.exitThread==false);
			if (exitThread==true) {
				return;
			}
			
			if (this.greekname.equals("GammaExposureRise")) {
				sleep(60*5);
				this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
				currentTrend = getSellerDirectionByATMGreekGap(this.greekname, lastKnownTrend);
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
				
				if (this.avoidTurbulace ==  true && isVolatile()) {
					if (!peStraddleOptionName.equals("")) { // Exit PE, taking Directional
						fileLogTelegramWriter.write( "Turbulance Exiting ="+peStraddleOptionName );
						// Exit PE
						if (this.placeActualOrder) {
							placeRealOrder( peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						peStraddleOptionName = "";
					}
					if (!ceStraddleOptionName.equals("")) { // Exit and re enter
						fileLogTelegramWriter.write( " Exiting ="+ceStraddleOptionName );
						// Exit CE
						if (this.placeActualOrder) {
							placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						ceStraddleOptionName = "";
					}
				} else {
					currentTrend = getSellerDirectionByATMGreekGap(this.greekname, lastKnownTrend); // StatusQuo, CE, PE
					
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
	
	private String getSellerDirectionByATMGreekGap(String greekname, String lastKnownTrend) {
		String retVal = lastKnownTrend;
		
		if (greekname.equals("Top5OiWorth")) {
			return getOptionTrendFromOIWorth(lastKnownTrend);
		} else if (greekname.equals("deltaPrbltOiWorth")) {
			return getOptionTrendFromDeltaProbablityAdjustedOIWorth(lastKnownTrend);
		} else if (greekname.equals("drMedianAvgIV")) {
			return getSellerDirectionByMedianAndAvgIVGreekGap(lastKnownTrend);
		} else if (greekname.equals("lowerStrikeAvgIV")) {
			return getSellerDirectionByLowerStrikeAvgIV();
		} else if (greekname.equals("Top5OIDeltaBiased")) {
			return getOptionTrendFromTop5OIDeltaBiased(lastKnownTrend);
		} else if (greekname.equals("PremiumBasedGreekGap")) {
			return getOptionTrendFromPremiumBasedGreekGap(lastKnownTrend);
		} else if (greekname.equals("GammaExposureRise")) {
			return getOptionTrendFromGammaExposureRise(lastKnownTrend);
		} else if (greekname.equals("ComboOutlier")) {
			return getOptionTrendFromComboOutlier(lastKnownTrend);
		} else if (greekname.equals("OiSupportStrength")) {
			return getSellerDirectionBySupportStrength();
		} else if (greekname.equals("multiOIAltAbove5")) {
			return getSellerDirectionByMultiOI(lastKnownTrend);
		}else if (greekname.equals("minMaxIvAltAbove5")) {
			return getSellerDirectionByMinMaxIv(lastKnownTrend);
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
				fieldname = "tmpaccmlcetheta as ceGreek, tmpaccmlpetheta as peGreek";
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
			}  
			
			Integer instrumentIdToUse = this.mainInstrument.getId().intValue();
			if (dependentInstrumentId!=null) {
				instrumentIdToUse = dependentInstrumentId;
			}
			
			String fetchSql = "select " + fieldname + ", countceoutlier, countpeoutlier from nexcorio_option_atm_movement_data where f_main_instrument = " + instrumentIdToUse + ""
					+ " and record_time <= '" + postgresLongDateFormat.format(getCurrentTime()) + "'"
					+ " order by record_time desc limit 5";
			fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
			ResultSet rs = stmt.executeQuery(fetchSql);
			
			int gapCount = 0;
			int gapCountWithoutAdjust = 0;
			int ceOutlierCount = 0;
			int peOutlierCount = 0;
			while (rs.next()) {
				float ceGreek = rs.getFloat("ceGreek");
				float peGreek = rs.getFloat("peGreek");
				ceOutlierCount = rs.getInt("countceoutlier");
				peOutlierCount = rs.getInt("countpeoutlier");
				if (greekname.equalsIgnoreCase("netGammaExposure")||greekname.equalsIgnoreCase("netGamaExpWthStrk")||greekname.equalsIgnoreCase("netGameXpTopN")) {
					if (ceGreek < -adjustGap) gapCount++;
				} else {
					if (ceGreek+adjustGap>=peGreek) {
						gapCount++;
					}
					if (ceGreek>=peGreek) {
						gapCountWithoutAdjust++;
					}
				}
				fileLogTelegramWriter.write("ceGreek="+ceGreek+" peGreek="+peGreek+" gapCount="+gapCount);
			}
			rs.close();			
			stmt.close();
			
			fileLogTelegramWriter.write("gapCpunt=" + gapCount + " gapCountWithoutAdjust=" + gapCountWithoutAdjust + " peOutlierCount="+peOutlierCount);
			
			if (greekname.contains("gamma") || greekname.contains("Gamma")) gapCount = 5-gapCount;
			
			if(this.inverse) {
				gapCount = 5-gapCount;
			}
			
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
			if(this.peCheckCEOutlier > 0) {
				if (retVal.equals("CE")  && peOutlierCount >= peCheckCEOutlier) { // && gapCountWithoutAdjust==5
					retVal = "PE";
				}
			}
			if(this.resetAdjustGap && retVal.equals("PE") && gapCount==gapCountWithoutAdjust) {
				this.adjustGap = this.adjustGap/2;
				this.resetAdjustGap = false;
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
	
	private String getSellerDirectionByMedianAndAvgIVGreekGap( String lastKnownTrend) {
		String retVal = lastKnownTrend;
		
		
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			Integer instrumentIdToUse = this.mainInstrument.getId().intValue();
			if (dependentInstrumentId!=null) {
				instrumentIdToUse = dependentInstrumentId;
			}
			
			String fetchSql = "select drCEMedianIV, drPEMedianIV, drCEAvgIV, drPEAvgIV, drCEPeak2IvDiff, drPEPeak2IvDiff,"
					+ " drWhlStrkaccumulatedchangein5seccetheta as ceGreek, drWhlStrkaccumulatedchangein5secpetheta as peGreek"
					+ " from nexcorio_option_atm_movement_data"
					+ " where f_main_instrument = " + instrumentIdToUse + ""
					+ " and record_time <= '" + postgresLongDateFormat.format(getCurrentTime()) + "'"
					+ " order by record_time desc limit 5";
			fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
			ResultSet rs = stmt.executeQuery(fetchSql);
			
			float drCEMedianIV = 0f;
			float drPEMedianIV = 0f;
			
			float drCEAvgIV = 0f;
			float drPEAvgIV = 0f;
			
			float drCEPeak2IvDiff = 0f;
			float drPEPeak2IvDiff = 0f;
			
			float otm250x750AccmlCeTheta = 0f;
			float otm250x750AccmlPeTheta = 0f;
			int otm250x750AccmlThetaCount = 0;
			
			int avgCount = 0;
			int peakCount = 0;
			int medianCount = 0;
			int diffCount = 0;
			while (rs.next()) {
				drCEMedianIV = rs.getFloat("drCEMedianIV");
				drPEMedianIV = rs.getFloat("drPEMedianIV");
				
				drCEAvgIV = rs.getFloat("drCEAvgIV");
				drPEAvgIV = rs.getFloat("drPEAvgIV");
				
				drCEPeak2IvDiff = rs.getFloat("drCEPeak2IvDiff");
				drPEPeak2IvDiff = rs.getFloat("drPEPeak2IvDiff");	
				
				otm250x750AccmlCeTheta = rs.getFloat("ceGreek");
				otm250x750AccmlPeTheta = rs.getFloat("peGreek");
				
				float ceDiff = Math.abs(drCEMedianIV-drCEAvgIV);
				float peDiff = Math.abs(drPEMedianIV-drPEAvgIV);
				
				if (drCEAvgIV > drPEAvgIV + adjustGap) avgCount++;
				if (drCEPeak2IvDiff > drPEPeak2IvDiff) peakCount++;
				if (drCEMedianIV > drPEMedianIV) medianCount++;
				if (otm250x750AccmlCeTheta > otm250x750AccmlPeTheta) otm250x750AccmlThetaCount++;
				if(ceDiff > peDiff) diffCount++;
			}
			rs.close();			
			stmt.close();
			
			if (avgCount==5 && medianCount==5 ) retVal = "CE"; //- confirmed for Jan 26
			else if (avgCount==0 && medianCount==0 ) retVal = "PE"; // Modifid - confirmed for Jan 26
			else {
				if (otm250x750AccmlCeTheta==5) retVal = "CE"; 
				else if (otm250x750AccmlCeTheta==0) retVal = "PE";
			}
			
//			else if (avgCount==0 && medianCount==5 ) {
//				if (diffCount==5) retVal = "PE";//Modifirf - confirmed for Jan 26	- this needs further refinement
//				else if (diffCount==0) retVal = "CE";
//			}
//			else if (avgCount==5 && medianCount==0 ) {
//				if (otm250x750AccmlThetaCount==5) retVal = "CE";//Modifirf - confirmed for Jan 26	- this needs further refinement
//				else if (otm250x750AccmlThetaCount==0) retVal = "PE";
//			}


			
//			else if (avgCount==0 && medianCount==0 && peakCount==0) retVal = "PE";
//			else if (avgCount==5 && medianCount==5 && peakCount==0) retVal = "PE";
//			else if (avgCount==5 && medianCount==0 && peakCount==0) retVal = "CE";			
//			else if (avgCount==0 && medianCount==5 && peakCount==0) retVal = "PE";			
			
			
			
			
			if      (avgCount==0 && medianCount==0 && peakCount==0) fileLogTelegramWriter.write("1");
			else if (avgCount==0 && medianCount==0 && peakCount==5) fileLogTelegramWriter.write("2");
			else if (avgCount==0 && medianCount==5 && peakCount==0) fileLogTelegramWriter.write("3");
			else if (avgCount==0 && medianCount==5 && peakCount==5) fileLogTelegramWriter.write("4");
			
			else if (avgCount==5 && medianCount==0 && peakCount==0) fileLogTelegramWriter.write("5");
			else if (avgCount==5 && medianCount==0 && peakCount==5) fileLogTelegramWriter.write("6");
			else if (avgCount==5 && medianCount==5 && peakCount==0) fileLogTelegramWriter.write("7");
			else if (avgCount==5 && medianCount==5 && peakCount==5) fileLogTelegramWriter.write("8");
			
//			if (avgCount==5) {
//				if (medianCount==5) retVal = "PE";
//				else if (medianCount==0) retVal = "CE";
////				if (peakCount==5) retVal = "CE";
////				else if (peakCount==0) retVal = "PE";
////				else {
////					if (medianCount==5) retVal = "CE";
////					else if (medianCount==0) retVal = "PE";
////				}
//			} else if (avgCount==0) {
//				if (medianCount==5) retVal = "CE";
//				else if (medianCount==0) retVal = "PE";
////				if (peakCount==5) retVal = "PE";
////				else if (peakCount==0) retVal = "CE";
////				else {
////					if (medianCount==5) retVal = "CE";
////					else if (medianCount==0) retVal = "PE";
////				}				
//			}
			
			fileLogTelegramWriter.write("drCEMedianIV=" + drCEMedianIV + " drPEMedianIV="+drPEMedianIV+" drCEAvgIV=" + drCEAvgIV+" drPEAvgIV="+drPEAvgIV);
			
			
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
	
	private String getOptionTrendFromOIWorth(String lastKnownOptiontrend) {
		String retVal = "StatusQuo";
		
		Connection conn = null;
		String top4Options ="";
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			List<OptionGreek> optionGreeks = new ArrayList<OptionGreek>();
			
			if (this.backtestDate == null) { // Live
				optionGreeks = getSnapshotGreeksFromCache();
			} else {
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
			}
			stmt.close();
			
			Collections.sort(optionGreeks, new SortbyOI());
			
			float ceOIWorth = 0f;
			float peOIWorth = 0f;
			
			float ceOICount = 0;
			float peOICount = 0;
			
			int recProcessed = 0;
			for(OptionGreek aGreek: optionGreeks) {
				if (aGreek.getOi()*aGreek.getLtp()/10000000>10) {
					recProcessed++;
					
					String tradingSymbol = aGreek.getTradingSymbol();
					float worthInCr = aGreek.getOi()*aGreek.getLtp()/10000000f;
					float openInterest = aGreek.getOi();
					top4Options = top4Options + tradingSymbol +" ";
					if (tradingSymbol.endsWith("CE")) {
						ceOIWorth = ceOIWorth + worthInCr;
						ceOICount = ceOICount + openInterest;
					} else {
						peOIWorth = peOIWorth + worthInCr;
						peOICount = peOICount + openInterest;
					}
				}
				if (recProcessed >= 7) break;
			}
			
			if (ceOIWorth-peOIWorth>10) {
				retVal = "CE";
			} else if (peOIWorth-ceOIWorth>10) {
				retVal = "PE";
			} else {
				retVal = lastKnownOptiontrend;
			}
			String logString = " ceOIWorth="+ceOIWorth+" peOIWorth="+peOIWorth +" retVal="+retVal+" top4Options="+top4Options;
			fileLogTelegramWriter.write( logString);
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
	
	private String getOptionTrendFromTop5OIDeltaBiased(String lastKnownOptiontrend) {
		String retVal = "StatusQuo";
		
		Connection conn = null;
		String top4Options ="";
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			List<OptionGreek> optionGreeks = new ArrayList<OptionGreek>();
			
			if (this.backtestDate == null) { // Live
				optionGreeks = getSnapshotGreeksFromCache();
			} else {
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
			}
			stmt.close();
			
			Collections.sort(optionGreeks, new SortbyOI());
			
			float ceOIWorth = 0f;
			float peOIWorth = 0f;
			
			float ceOICount = 0;
			float peOICount = 0;
			
			float ceOIDeltaBias = 0f;
			float peOIDeltaBias = 0f;
			
			List<OptionGreek> ceGreeksTopOi = new ArrayList<OptionGreek>(); 
			List<OptionGreek> peGreeksTopOi = new ArrayList<OptionGreek>();
			
			int recProcessed = 0;
			
			for(OptionGreek aGreek: optionGreeks) {
				if (aGreek.getOi()*aGreek.getLtp()/10000000>10) {
					recProcessed++;
					
					String tradingSymbol = aGreek.getTradingSymbol();
					float worthInCr = aGreek.getOi()*aGreek.getLtp()/10000000f;
					float openInterest = aGreek.getOi();
					float delta = Math.abs(aGreek.getDelta());
					top4Options = top4Options + tradingSymbol +" ";
					if (tradingSymbol.endsWith("CE")) {
						ceOIWorth = ceOIWorth + worthInCr;
						ceOICount = ceOICount + openInterest;
						ceOIDeltaBias = ceOIDeltaBias + openInterest/(1f-delta);
						ceGreeksTopOi.add(aGreek);
					} else {
						peOIWorth = peOIWorth + worthInCr;
						peOICount = peOICount + openInterest;
						peOIDeltaBias = peOIDeltaBias + openInterest/(1f-delta);
						peGreeksTopOi.add(aGreek);
					}
				}
				if (recProcessed >= 5) break;
			}
			
			float avgCeDistance = 0f;
			float avgPeDistance = 0f;
			
			for(OptionGreek aGreek: ceGreeksTopOi) {
				avgCeDistance = avgCeDistance + aGreek.getOi()/(aGreek.getStrike() - this.instrumentLtp);
			}
			for(OptionGreek aGreek: peGreeksTopOi) {
				avgPeDistance = avgPeDistance + aGreek.getOi()/(this.instrumentLtp - aGreek.getStrike());
			}
			if (ceGreeksTopOi.size() > 0 ) avgCeDistance = avgCeDistance/(float)ceGreeksTopOi.size();
			else avgCeDistance = 2000f;
			if (peGreeksTopOi.size() > 0 ) avgPeDistance = avgPeDistance/(float)peGreeksTopOi.size();
			else avgPeDistance = 2000f;
			
			
//			if (ceOIDeltaBias - peOIDeltaBias > 0) {
//				retVal = "CE";
//			} else {
//				retVal = "PE";
//			}
			
			if (avgCeDistance < avgPeDistance) {
				retVal = "CE";
			} else {
				retVal = "PE";
			}
			
			
			String logString = " ceOIWorth="+ceOIWorth+" peOIWorth="+peOIWorth +" retVal="+retVal + "ceOIDeltaBias="+ceOIDeltaBias+" peOIDeltaBias="+peOIDeltaBias+" top4Options="+top4Options;
			fileLogTelegramWriter.write( logString);
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
	
	private String getOptionTrendFromPremiumBasedGreekGap(String lastKnownOptiontrend) {
		String retVal = "StatusQuo";
		
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
						
			//String fieldname = "drWhlStrkaccumulatedchangein5secpetheta as peGreek, drWhlStrkaccumulatedchangein5seccetheta as ceGreek";
			String fieldname = "altabove5WhlStrkCEAvgIv as ceGreek, altabove5WhlStrkPEAvgIv as peGreek";
			Integer instrumentIdToUse = this.mainInstrument.getId().intValue();
			if (dependentInstrumentId!=null) {
				instrumentIdToUse = dependentInstrumentId;
			}
			
			String fetchSql = "select " + fieldname + ", celtp+peltp as premium,lowerStrikeCEAvgIv, lowerStrikePEAvgIv, upperStrikeCEAvgIv, upperStrikePEAvgIv, tmpaccmlcetheta, tmpaccmlpetheta from nexcorio_option_atm_movement_data where f_main_instrument = " + instrumentIdToUse + ""
					+ " and record_time <= '" + postgresLongDateFormat.format(getCurrentTime()) + "'"
					+ " order by record_time desc limit 5";
			fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
			ResultSet rs = stmt.executeQuery(fetchSql);
			
			float lowerStrikeCEAvgIv = 0f;
			float lowerStrikePEAvgIv = 0f;
			
			float upperStrikeCEAvgIv = 0f;
			float upperStrikePEAvgIv = 0f;
			
			float fullCEAvgIv = 0f;
			float fullPEAvgIv = 0f;
			
			int gapCount = 0;
			float curPremium = -1f;
			
			while (rs.next()) {
				float ceGreek = rs.getFloat("ceGreek");
				float peGreek = rs.getFloat("peGreek");
				
				lowerStrikeCEAvgIv = lowerStrikeCEAvgIv + rs.getFloat("lowerStrikeCEAvgIv");
				lowerStrikePEAvgIv = lowerStrikePEAvgIv + rs.getFloat("lowerStrikePEAvgIv");
				
				upperStrikeCEAvgIv = upperStrikeCEAvgIv + rs.getFloat("upperStrikeCEAvgIv");
				upperStrikePEAvgIv = upperStrikePEAvgIv + rs.getFloat("upperStrikePEAvgIv");
				
				fullCEAvgIv = fullCEAvgIv + rs.getFloat("tmpaccmlcetheta");
				fullPEAvgIv = fullPEAvgIv + rs.getFloat("tmpaccmlpetheta");
				
				if (curPremium < 0f) {
					curPremium = rs.getFloat("premium");
					if (this.straddleAt920 <= 0f) straddleAt920 = curPremium;
				}
				
				if (ceGreek+adjustGap>=peGreek) {
					gapCount++;
				}
			}
			rs.close();			
			stmt.close();
			
			fileLogTelegramWriter.write("gapCpunt=" + gapCount +" straddleAt920="+straddleAt920+" curPremium="+curPremium);
			
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
			if (fullPEAvgIv > fullCEAvgIv && lowerStrikePEAvgIv > upperStrikePEAvgIv ) {
				retVal = "CE";
			}
//			if (curPremium > this.straddleAt920 + 5f) {
//				if (retVal.equals("CE")) retVal = "PE";
//				else if (retVal.equals("PE")) retVal = "CE";
//			} else if (curPremium > this.straddleAt920) {
//				if (!lastKnownOptiontrend.equals(retVal)) retVal = lastKnownOptiontrend;
//			}
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
	private String getOptionTrendFromGammaExposureRise(String lastKnownOptiontrend) {
		String retVal = "StatusQuo";
		
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
						
			//String fieldname = "dr16AccumulatedChangein5secCeTheta as peGreek, dr16AccumulatedChangein5secPeTheta as ceGreek";
			String fieldname = "tmpaccmlcetheta as peGreek, tmpaccmlpetheta as ceGreek";
			//String fieldname = "drWhlStrkaccumulatedchangein5seccetheta as ceGreek, drWhlStrkaccumulatedchangein5secpetheta as peGreek";
			Integer instrumentIdToUse = this.mainInstrument.getId().intValue();
			if (dependentInstrumentId!=null) {
				instrumentIdToUse = dependentInstrumentId;
			}
			
			String fetchSql = "select " + fieldname + " from nexcorio_option_atm_movement_data where f_main_instrument = " + instrumentIdToUse + ""
					+ " and record_time <= '" + postgresLongDateFormat.format(getCurrentTime()) + "'"
					+ " order by record_time desc limit 5";
			fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
			ResultSet rs = stmt.executeQuery(fetchSql);
			
			float ceGreek = 0f;
			float peGreek = 0f;
			
			while (rs.next()) {
				ceGreek = ceGreek + rs.getFloat("ceGreek");
				peGreek = peGreek + rs.getFloat("peGreek");
			}
			rs.close();			
			stmt.close();
			
			ceGreek = ceGreek/5f;
			peGreek = peGreek/5f;
			
			if (gammaExposureAt920Set == false) {
				ceGammaExposureAt920 = ceGreek;
				peGammaExposureAt920 = peGreek;
				gammaExposureAt920Set = true;
			}
			
			float ceGain = (ceGreek-ceGammaExposureAt920);//*100f/ceGammaExposureAt920;
			float peGain = (peGreek-peGammaExposureAt920);//*100f/peGammaExposureAt920;
			if (ceGain < peGain) {
				retVal = "CE";
			} else {
				retVal = "PE";
			}
			
			fileLogTelegramWriter.write("ceGammaExposureAt920=" + ceGammaExposureAt920 +" peGammaExposureAt920="+peGammaExposureAt920+" ceGreek="+ceGreek+" peGreek="+peGreek+" CeGain="+ceGain+" peGain="+peGain);
			
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
	
	private String getOptionTrendFromComboOutlier(String lastKnownOptiontrend) {
		String retVal = "StatusQuo";
		
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			Integer instrumentIdToUse = this.mainInstrument.getId().intValue();
			if (dependentInstrumentId!=null) {
				instrumentIdToUse = dependentInstrumentId;
			}
			
			String fetchSql = "select fullOtm0x600CEGreeks, fullOtm0x600PEGreeks, altabove5WhlStrkCEAvgIv, altabove5WhlStrkPEAvgIv, countceoutlier, countpeoutlier from nexcorio_option_atm_movement_data where f_main_instrument = " + instrumentIdToUse + ""
					+ " and record_time <= '" + postgresLongDateFormat.format(getCurrentTime()) + "'"
					+ " order by record_time desc limit 5";
			fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
			ResultSet rs = stmt.executeQuery(fetchSql);
			
			int fullGapCount = 0;
			int altGapCount = 0;
			float ceAvgOutlier = 0f;
			float peAvgOutlier = 0f;
			
			while (rs.next()) {
				float fullCeGreek = rs.getFloat("fullOtm0x600CEGreeks");
				float fullPeGreek = rs.getFloat("fullOtm0x600PEGreeks");
				
				float altCeGreek = rs.getFloat("altabove5WhlStrkCEAvgIv");
				float altPeGreek = rs.getFloat("altabove5WhlStrkPEAvgIv");
				
				ceAvgOutlier = ceAvgOutlier + rs.getInt("countceoutlier");
				peAvgOutlier = peAvgOutlier + rs.getInt("countpeoutlier");
				
				if (fullCeGreek + 2500f > fullPeGreek) fullGapCount++;
				if (altCeGreek > altPeGreek) altGapCount++;
				
			}
			rs.close();			
			stmt.close();
			
			ceAvgOutlier = ceAvgOutlier/5f;
			peAvgOutlier = peAvgOutlier/5f;
			
			if (ceAvgOutlier >= 6f || peAvgOutlier >= 6f) {
				if (fullGapCount == 0) {
					retVal = "PE";
				} else if (fullGapCount == 5) {
					retVal = "CE";
				}
				fileLogTelegramWriter.write("Using Full -> retVal=" + retVal);
			} else if (ceAvgOutlier <= 4f && peAvgOutlier <= 4f) {
				if (altGapCount == 0) {
					retVal = "PE";
				} else if (altGapCount == 5) {
					retVal = "CE";
				}
				fileLogTelegramWriter.write("Using Alt -> retVal=" + retVal);
			} else {
				retVal = lastKnownOptiontrend;
			}
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
	
	private String getSellerDirectionByMultiOI(String lastKnownOptiontrend) {
		String retVal = lastKnownOptiontrend;
		
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			Integer instrumentIdToUse = this.mainInstrument.getId().intValue();
			if (dependentInstrumentId!=null) {
				instrumentIdToUse = dependentInstrumentId;
			}
			
			String greekSelectFields = " fullOtm0x600CEGreeks as ceGreek, fullOtm0x600PEGreeks as peGreek";
			if (mainGreek4MultiOI.equalsIgnoreCase("fullOtm0x500")) {
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
	
	private String getSellerDirectionByMinMaxIv(String lastKnownOptiontrend) {
		String retVal = lastKnownOptiontrend;
		
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			Integer instrumentIdToUse = this.mainInstrument.getId().intValue();
			if (dependentInstrumentId!=null) {
				instrumentIdToUse = dependentInstrumentId;
			}
			
			String fetchSql = "select upperDeltaPeMinIv, upperDeltaPeAvgIv,lowerDeltaPeMinIv, lowerDeltaPeAvgIv, altabove5WhlStrkCEAvgIv, altabove5WhlStrkPEAvgIv from nexcorio_option_atm_movement_data where f_main_instrument = " + instrumentIdToUse + ""
					+ " and record_time <= '" + postgresLongDateFormat.format(getCurrentTime()) + "'"
					+ " order by record_time desc limit 5";
			fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
			ResultSet rs = stmt.executeQuery(fetchSql);
			
			float upperDeltaPeMinIv = 0f;
			float upperDeltaPeAvgIv = 0f;
			
			float lowerDeltaPeMinIv = 0f;
			float lowerDeltaPeAvgIv = 0f;
			
			int altGapCount = 0;
			int skewCount = 0;
			
			while (rs.next()) {
				upperDeltaPeMinIv = rs.getFloat("upperDeltaPeMinIv");
				upperDeltaPeAvgIv = rs.getFloat("upperDeltaPeAvgIv");
				
				lowerDeltaPeMinIv = rs.getFloat("lowerDeltaPeMinIv");
				lowerDeltaPeAvgIv = rs.getFloat("lowerDeltaPeAvgIv");
				
//				if (lowerDeltaPeMinIv > upperDeltaPeAvgIv) {
//					skewCount++;
//				} else 
					if (upperDeltaPeMinIv > lowerDeltaPeAvgIv) {
					skewCount++;
				}
				
				float ceGreek = rs.getFloat("altabove5WhlStrkCEAvgIv");
				float peGreek = rs.getFloat("altabove5WhlStrkPEAvgIv");
				
				if (ceGreek > peGreek) altGapCount++;
			}
			rs.close();			
			stmt.close();
			
			String directionAltAbove5 = ""; 
			if (altGapCount == 0) {
				directionAltAbove5 = "PE";
			} else if (altGapCount == 5) {
				directionAltAbove5 = "CE";
			}
			
			retVal = directionAltAbove5;
			
			if (directionAltAbove5.equals("PE")) {
				if (skewCount==5) retVal = "CE";
			}
			
			fileLogTelegramWriter.write("lowerDeltaPeMinIv="+lowerDeltaPeMinIv+" upperDeltaPeAvgIv="+upperDeltaPeAvgIv+" directionAltAbove5="+directionAltAbove5 + " skewCount="+skewCount + "retVal="+ retVal);
			
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
			stmt.close();
			if (ceOIWorth-peOIWorth>10) {
				retVal = "CE";
			} else if (peOIWorth-ceOIWorth>10) {
				retVal = "PE";
			} else {
				retVal = lastKnownOptiontrend;
			}
			String logString = " ceOIWorth="+ceOIWorth+" peOIWorth="+peOIWorth +" retVal="+retVal+" top4Options="+top4Options;
			fileLogTelegramWriter.write( logString);
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
	private String getOiWorthSellerDirection() {
		String retStr = "Neutral";
		
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			List<OptionGreek> optionGreeks = new ArrayList<OptionGreek>();
			
			if (this.backtestDate == null) { // Live
				optionGreeks = getSnapshotGreeksFromCache();
			} else {
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
			}
			stmt.close();
			
			Collections.sort(optionGreeks, new SortbyOI());
			
			int ceCount = 0;
			int peCount = 0;
			float ceWorth =0f;
			float peWorth =0f;
			StringBuffer topOptions = new StringBuffer();
			for(OptionGreek aGreek: optionGreeks) {
				if (aGreek.getOi()*aGreek.getLtp()/10000000>10) {
					
					topOptions.append(aGreek.getTradingSymbol()+" " + aGreek.getOi() + " ");
					if (aGreek.getTradingSymbol().endsWith("CE")) {
						ceCount++;
						ceWorth = ceWorth + aGreek.getOi()*aGreek.getLtp()/10000000f;
					} else {
						peCount++;
						peWorth = peWorth + aGreek.getOi()*aGreek.getLtp()/10000000f;
					}
					if(ceCount+peCount>=7 ) break; 
				}
			}
			
			if (ceWorth-peWorth>=50) {
				retStr = "CE";
			} else if (peWorth-ceWorth>=50) {
				retStr = "PE";
			}
			fileLogTelegramWriter.write("topOptions="+topOptions + " retStr="+retStr);
			
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error"+e.getMessage(),e);
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				log.error(e);
			}
		}	
		return retStr;
	}
	
	private boolean isVolatile() {
		boolean retVal = false;
		
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			String fetchSql = "select celtp+peltp as curPremium from nexcorio_option_atm_movement_data where f_main_instrument = " + mainInstrument.getId() + ""
					+ " and record_time <= '" + postgresLongDateFormat.format(getCurrentTime()) + "' order by record_time desc limit 1";
			fileLogTelegramWriter.write("2. fetchSql="+fetchSql);
			
			ResultSet rs = stmt.executeQuery(fetchSql);
			float curValue = 0f;
			while (rs.next()) {
				curValue = rs.getFloat("curPremium");
			}
			rs.close();
			
			if (this.straddleAt920 <= 0f) straddleAt920 = curValue;			
			if (curValue > this.straddleAt920 + 5f) retVal = true;
			fileLogTelegramWriter.write("this.straddleAt920="+this.straddleAt920+" curValue="+curValue+" retVal="+retVal);
			
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
	
	private String getSellerDirectionByLowerStrikeAvgIV() {
		String retVal = "Unknown";
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			String fieldname = "tmpaccmlcetheta as ceGreek, tmpaccmlpetheta as peGreek";
			
			Integer instrumentIdToUse = this.mainInstrument.getId().intValue();
			if (dependentInstrumentId!=null) {
				instrumentIdToUse = dependentInstrumentId;
			}
			
//			if (lowetStrikeValuesSet == false) {
//				String fetchSql = "select max(tmpaccmlcetheta) as ceGreek, max(tmpaccmlpetheta) as peGreek from nexcorio_option_atm_movement_data where f_main_instrument = " + instrumentIdToUse + ""
//						+ " and record_time >= '" + postgresShortDateFormat.format(getCurrentTime()) + " 09:20:00'"
//						+ " and record_time <= '" + postgresShortDateFormat.format(getCurrentTime()) + " 09:35:00'";
//				ResultSet rs = stmt.executeQuery(fetchSql);
//				
//				while (rs.next()) {
//					lowetStrikeCeAvgIVAt920 = rs.getFloat("ceGreek");
//					lowetStrikePeAvgIVAt920 = rs.getFloat("peGreek");
//					lowetStrikeValuesSet = true;
//				}
//			}
			
			
			String fetchSql = "select lowerStrikeCEAvgIv, lowerStrikePEAvgIv, upperStrikeCEAvgIv, upperStrikePEAvgIv, fullOtm0x600CEGreeks, fullOtm0x600PEGreeks, altabove5WhlStrkCEAvgIv, altabove5WhlStrkPEAvgIv from nexcorio_option_atm_movement_data where f_main_instrument = " + instrumentIdToUse + ""
					+ " and record_time <= '" + postgresLongDateFormat.format(getCurrentTime()) + "'"
					+ " order by record_time desc limit 5";
			fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
			ResultSet rs = stmt.executeQuery(fetchSql);
			
			float lowerStrikeCEAvgIv = 0f;
			float lowerStrikePEAvgIv = 0f;
			
			float upperStrikeCEAvgIv = 0f;
			float upperStrikePEAvgIv = 0f;
			
			float fullOtm0x600CEGreeks = 0f;
			float fullOtm0x600PEGreeks = 0f;
			
			float altabove5WhlStrkCEAvgIv = 0f;
			float altabove5WhlStrkPEAvgIv = 0f;
			
			int peCondition = 0;
			
			while (rs.next()) {
				lowerStrikeCEAvgIv = rs.getFloat("lowerStrikeCEAvgIv");
				lowerStrikePEAvgIv = rs.getFloat("lowerStrikePEAvgIv");
				
				upperStrikeCEAvgIv = rs.getFloat("upperStrikeCEAvgIv");
				upperStrikePEAvgIv = rs.getFloat("upperStrikePEAvgIv");
				
				fullOtm0x600CEGreeks = rs.getFloat("fullOtm0x600CEGreeks");
				fullOtm0x600PEGreeks = rs.getFloat("fullOtm0x600PEGreeks");
				
				altabove5WhlStrkCEAvgIv = rs.getFloat("altabove5WhlStrkCEAvgIv");
				altabove5WhlStrkPEAvgIv = rs.getFloat("altabove5WhlStrkPEAvgIv");
				
//				if (upperStrikeCEAvgIv + adjustGap > upperStrikePEAvgIv ) {
//					peCondition++;
//				} else if (upperStrikePEAvgIv + adjustGap > lowerStrikePEAvgIv) {
//					peCondition++;
//				} else if (fullOtm0x600PEGreeks > fullOtm0x600CEGreeks + 5000) {
//					peCondition++;
//				}
				
					
//				} else if (upperStrikeCEAvgIv + 0.25 > lowerStrikeCEAvgIv) {
//					peCondition++;
//				}
				
				if (fullOtm0x600PEGreeks > fullOtm0x600CEGreeks + 5000) {
					peCondition++;
				} else if (fullOtm0x600CEGreeks > fullOtm0x600PEGreeks + 5000) {
					peCondition--;
				} else if (altabove5WhlStrkPEAvgIv > + adjustGap+ altabove5WhlStrkCEAvgIv) {
					peCondition++;
				} else {
					peCondition--;
				}
				
				fileLogTelegramWriter.write("lowerStrikeCEAvgIv="+lowerStrikeCEAvgIv+" lowerStrikePEAvgIv="+lowerStrikePEAvgIv+" upperStrikeCEAvgIv="+upperStrikeCEAvgIv+" upperStrikePEAvgIv="+upperStrikePEAvgIv+" peCondition="+peCondition);
			}
			rs.close();			
			stmt.close();
			
			if (peCondition >= 5 ) {
				retVal = "PE";
			} else {
				retVal = "CE";
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
	
	private String getSellerDirectionBySupportStrength() {
		String retVal = "Neutral";
		Connection conn = null;
		String top4Options ="";
		String logString = "";
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			List<OptionGreek> optionGreeks = new ArrayList<OptionGreek>();
			
			if (this.backtestDate == null) { // Live
				optionGreeks = getSnapshotGreeksFromCache();
			} else {
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
			}
			stmt.close();
			
			Collections.sort(optionGreeks, new SortbyOI());
			
			int ceCount = 0;
			int peCount = 0;
			
			float totalCeOI = 0;
			float totalPeOI = 0;
			
			float totalCeOIWorth = 0;
			float totalPeOIWorth = 0;
			
			int top5CeCount = 0;
			int top5PeCount = 0;
			
			int recCount = 0;
			List<Integer> ceStrikes = new ArrayList<Integer>();
			List<Integer> peStrikes = new ArrayList<Integer>();
			for(OptionGreek aGreek:optionGreeks ) {
				if (aGreek.getOi()*aGreek.getLtp()/10000000 > 10) {
					
					String tradingSymbol = aGreek.getTradingSymbol();
					int strikePrice = aGreek.getStrike();
					float openInterest = aGreek.getOi();
					float oiWorth = aGreek.getOi()*aGreek.getLtp()/10000000;
					top4Options = top4Options + tradingSymbol +" ";
					if (aGreek.getStrike() - this.instrumentLtp < 600 || aGreek.getStrike() - this.instrumentLtp > -600) {
						if (tradingSymbol.endsWith("CE")) {
							ceCount++;
							totalCeOI = totalCeOI + openInterest;
							totalCeOIWorth =  totalCeOIWorth + oiWorth;
							if (recCount<5) top5CeCount++;
							ceStrikes.add(strikePrice);
						} else {
							peCount++;
							totalPeOI = totalPeOI + openInterest;
							totalPeOIWorth =  totalPeOIWorth + oiWorth;
							if (recCount<5) top5PeCount++;
							peStrikes.add(strikePrice);
						}
					}
					recCount++;
				}
				if (recCount >= 5) break;
			}
			
			Collections.sort(ceStrikes);
			Collections.sort(peStrikes, Collections.reverseOrder());
			
			fileLogTelegramWriter.write(" Printing ordered CE Strikes");
			//print(ceStrikes);
			fileLogTelegramWriter.write(" Printing ordered PE Strikes");
			//print(peStrikes);
			
			int ceGap = 0;
			int peGap = 0;
			float ceSupprotDistance4mIndex = 0f;
			float peSupprotDistance4mIndex = 0f;
			if (ceStrikes.size()>1) {
				ceGap = ceStrikes.get(1) - ceStrikes.get(0);
				ceSupprotDistance4mIndex = ceStrikes.get(0) - this.instrumentLtp;
			} else if (ceStrikes.size()>0) {
				ceGap = (int) (ceStrikes.get(0) - this.instrumentLtp);
				ceSupprotDistance4mIndex = ceStrikes.get(0) - this.instrumentLtp;
			} else {
				ceGap = 2000;
				ceSupprotDistance4mIndex = 2000f;
			}
			
			if (peStrikes.size()>1) {
				peGap = peStrikes.get(0) - peStrikes.get(1);
				peSupprotDistance4mIndex = this.instrumentLtp - peStrikes.get(0);
			} else if (peStrikes.size()>0) {
				peGap = (int) (this.instrumentLtp - peStrikes.get(0));
				peSupprotDistance4mIndex = this.instrumentLtp - peStrikes.get(0);
			} else {
				peGap = 2000;
				peSupprotDistance4mIndex = 2000f;
			}
			
			float gapRatio = ceGap>peGap?((float)peGap/(float)ceGap):((float)ceGap/(float)peGap);
			fileLogTelegramWriter.write(" ceGap="+ceGap+" peGap="+peGap+" gapRatio="+gapRatio+" ceSupprotDistance4mIndex="+ceSupprotDistance4mIndex+" peSupprotDistance4mIndex="+peSupprotDistance4mIndex);
			
			if (ceSupprotDistance4mIndex <=peSupprotDistance4mIndex) {
				retVal = "CE";
			} else if (ceSupprotDistance4mIndex>peSupprotDistance4mIndex) {
				retVal = "PE";
			} else {
				if (top5CeCount>top5PeCount) retVal = "CE";
				else retVal = "PE";
			}
			logString = " ceCount="+ceCount+" peCount="+peCount+" top5CeCount="+top5CeCount+" top5PeCount="+top5PeCount 
					+" totalCeOI="+totalCeOI+" totalPeOI="+totalPeOI+" CPRatio="+(totalCeOI/totalPeOI) + " totalCeOIWorth="+totalCeOIWorth+" totalPeOIWorth="+totalPeOIWorth;
			fileLogTelegramWriter.write( logString +" topOptions="+top4Options+" retVal="+retVal);
			
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
		new G3GreekGapAlgoThread(23L, null);
	}

	

}
