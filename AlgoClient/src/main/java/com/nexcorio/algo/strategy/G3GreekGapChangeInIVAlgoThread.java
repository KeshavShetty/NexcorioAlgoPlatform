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

public class G3GreekGapChangeInIVAlgoThread extends G3BaseClass implements Runnable{

	private static final Logger log = LogManager.getLogger(G3GreekGapChangeInIVAlgoThread.class);
	
	public float baseDelta = 0.5f;
	public boolean adjustPosition = false; // Adjust position after steep fall
	
	public Integer dependentInstrumentId = null;
	
	public boolean useMinMax = false;
	
	public G3GreekGapChangeInIVAlgoThread(Long napAlgoId, String backTestDateStr) {
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
			} while (currentTrend.equals(lastKnownTrend));
			
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
