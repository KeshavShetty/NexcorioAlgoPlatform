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

public class G3V2GreekGapBiasedStrangleAlgoThread extends G3BaseClass implements Runnable {

	private static final Logger log = LogManager.getLogger(G3PriceParityIVBasedAlgoThread.class);
	
	public float upperDelta = 0.6f;
	public float lowerDelta = 0.4f;
	
	public String greekname = "";
	public String mainGreek4MultiOI = "";
	
	public float  indexRollingPts = 0f;
	
	public boolean maintainBias = false;
	public float oppositeDeltaDiff= 0f;
	
	public G3V2GreekGapBiasedStrangleAlgoThread(Long napAlgoId, String backTestDateStr) {
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
			
			String fieldname = this.instrumentLtp + "-lowerstrike as ceGreek, upperstrike-" + this.instrumentLtp+ " as peGreek";
			if (greekname.equalsIgnoreCase("selectedAllGreeksAvgIv")) {
				fieldname = "selectedallcegreeksavgiv as ceGreek, selectedallpegreeksavgiv as peGreek";
			} else if (greekname.equalsIgnoreCase("LimitedOTMGreeks")) {
				fieldname = "limitedotmcegreeks as ceGreek, limitedotmpegreeks as peGreek";
			} else if (greekname.equalsIgnoreCase("MixGreeks")) {
				fieldname = "limitedotmcegreeks as peGreek, limiteditmpegreeks as ceGreek";
			} else if (greekname.equalsIgnoreCase("OTMAccmlChangeInTheta")) {
				fieldname = "drOTMAccumulatedChangein5secCeTheta as peGreek, drOTMAccumulatedChangein5secPeTheta as ceGreek";
			}
			
			
			Integer instrumentIdToUse = this.mainInstrument.getId().intValue();
			
			String fetchSql = "select " + fieldname + " from nexcorio_option_greek_movement_data where f_main_instrument = " + instrumentIdToUse + ""
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
