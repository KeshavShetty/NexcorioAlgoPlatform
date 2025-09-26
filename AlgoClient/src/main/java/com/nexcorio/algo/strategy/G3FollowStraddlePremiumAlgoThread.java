package com.nexcorio.algo.strategy;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.util.KiteUtil;
import com.nexcorio.algo.util.db.HDataSource;

public class G3FollowStraddlePremiumAlgoThread extends G3BaseClass implements Runnable{

	private static final Logger log = LogManager.getLogger(G3PriceParityIVBasedAlgoThread.class);
	
	public float baseDelta = 0.5f;
	public String greekname = "ltp";
	
	public float diffFromAtmPremium = 10f;
	
	public boolean useMinReached = Boolean.FALSE;
	
	public float spikeProtection = 0f;
	
	public int avoidCeOutliersAbove = 0;
	public int avoidPeOutliersAbove = 0;
	
	public boolean wait4IdealPremium = Boolean.FALSE;
	
	private float idealPremium = 0f;
	
	public G3FollowStraddlePremiumAlgoThread(Long napAlgoId, String backTestDateStr) {
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
			
			boolean isitFirstAttempt = true;
			if (this.wait4IdealPremium) {
				
				boolean foundIdealpremium = false;
				do {
					float currentPremium = getStradlePremium();
					if (idealPremium==0f) {
						this.idealPremium = getIdealPremiumBasedOnPreviousStradlePremium();
					}
					if (currentPremium >= idealPremium) foundIdealpremium = true; 
					else {
						fileLogTelegramWriter.write("currentPremium="+currentPremium+" idealStraddlePremium="+idealPremium+" sleep");
						sleep(10);
						checkExitSignals();
						isitFirstAttempt = false;
					}
				} while(foundIdealpremium==false && exitThread==false);
				if (isitFirstAttempt==false) sleep(120);
				
			}
			
			if (exitThread==true) {
				return;
			}
			
			float straddlePremiumWhenFormed = 0f;
			float straddlePremiumMinReached = 0f;
			float straddlePremiumMaxReached = 0f;
				
			straddlePremiumMinReached = getStradlePremium();
			straddlePremiumMaxReached = straddlePremiumMinReached*1.2f;
			
			
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
				
				float currentATMStraddlePremium = getStradlePremium();
				float currentStraddlePositionPremium = (ceOptionGreeks==null?0: ceOptionGreeks.getLtp()) + (peOptionGreeks==null?0: peOptionGreeks.getLtp());
				
				fileLogTelegramWriter.write( "currentATMStraddlePremium="+currentATMStraddlePremium+" currentStraddlePositionPremium="+currentStraddlePositionPremium);
				
				boolean needRepositioning = false;
				
				if (ceStraddleOptionName.equals("")) {
					if (spikeProtection > 0f && currentATMStraddlePremium < straddlePremiumMaxReached*(100f-spikeProtection)/100f) {
						needRepositioning = true; // Just starting, no open positions
					} else if (avoidCeOutliersAbove+avoidPeOutliersAbove > 0) {
						if (getOutlierCount("CE", avoidCeOutliersAbove) <  avoidCeOutliersAbove && getOutlierCount("PE", avoidPeOutliersAbove) <  avoidPeOutliersAbove) {
							needRepositioning = true;
						}						
					} else if (spikeProtection < 1f) {
						needRepositioning = true; // Just starting, no open positions
					}
					if (needRepositioning==true) {
						straddlePremiumMinReached = currentATMStraddlePremium;
						straddlePremiumMaxReached = currentATMStraddlePremium;
					}
				} 
				if (needRepositioning==false && !ceStraddleOptionName.equals("")) {
					float actualATMThetaDecay = straddlePremiumWhenFormed - currentATMStraddlePremium;
					float capturedThetaDecay  = straddlePremiumWhenFormed - currentStraddlePositionPremium;
					
					if (actualATMThetaDecay-capturedThetaDecay > diffFromAtmPremium) {
						fileLogTelegramWriter.write(" Realigning 1.0, actualATMThetaDecay="+actualATMThetaDecay+" capturedThetaDecay="+capturedThetaDecay+" (Diff)="+(actualATMThetaDecay-capturedThetaDecay));
						needRepositioning = true;	
					}
				} 
				if (needRepositioning==false && useMinReached && !ceStraddleOptionName.equals("")) { // Check spike in premium
					float actualATMThetaDecay = straddlePremiumMinReached - currentATMStraddlePremium;
					float capturedThetaDecay  = straddlePremiumMinReached - currentStraddlePositionPremium;
					
					if (actualATMThetaDecay-capturedThetaDecay > diffFromAtmPremium) {
						fileLogTelegramWriter.write(" Realigning 2.0, actualATMThetaDecay="+actualATMThetaDecay+" capturedThetaDecay="+capturedThetaDecay+" (Diff)="+(actualATMThetaDecay-capturedThetaDecay)); 
						needRepositioning = true;	
					}
				}
				
				if (!ceStraddleOptionName.equals("") && avoidCeOutliersAbove+avoidPeOutliersAbove > 0
						&& ( getOutlierCount("CE", avoidCeOutliersAbove) >  avoidCeOutliersAbove || getOutlierCount("PE", avoidPeOutliersAbove) >  avoidPeOutliersAbove) ) {
					// Exit Exit Exit
					if (!ceStraddleOptionName.equals("")) { 
						fileLogTelegramWriter.write( " Exiting ="+ceStraddleOptionName );
						// Exit CE
						if (this.placeActualOrder) {
							placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						ceStraddleOptionName = "";
						ignoredOrders++;
					}
					if (!peStraddleOptionName.equals("")) { 
						fileLogTelegramWriter.write( " Exiting ="+peStraddleOptionName );
						if (this.placeActualOrder) {
							placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						peStraddleOptionName = "";
						ignoredOrders++;
					}
				} else if  (spikeProtection > 0 && !ceStraddleOptionName.equals("") && currentATMStraddlePremium > straddlePremiumMinReached*(100f+spikeProtection)/100f) {
					// Exit Exit Exit
					fileLogTelegramWriter.write( "Straddle Premium Spike, Exit Exit Exit");
					if (!ceStraddleOptionName.equals("")) { 
						fileLogTelegramWriter.write( " Exiting ="+ceStraddleOptionName );
						// Exit CE
						if (this.placeActualOrder) {
							placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						ceStraddleOptionName = "";
					}
					if (!peStraddleOptionName.equals("")) { 
						fileLogTelegramWriter.write( " Exiting ="+peStraddleOptionName );
						if (this.placeActualOrder) {
							placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						peStraddleOptionName = "";
					}
					sleep(15*60);
					currentATMStraddlePremium = getStradlePremium();
					straddlePremiumMinReached = currentATMStraddlePremium;
					straddlePremiumMaxReached = currentATMStraddlePremium;
					straddlePremiumWhenFormed = currentATMStraddlePremium;
					String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(this.baseDelta, this.optimalHedgeDistance);
					
					if (this.noOfOrders-ignoredOrders<maxAllowedNoOfOrders) {
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
					
					if (this.noOfOrders-ignoredOrders<maxAllowedNoOfOrders) {
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
					
				} else if (needRepositioning) {
					String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(this.baseDelta, this.optimalHedgeDistance);
					
					String ceOptionname = entryStraddleOptionNames[0];
					
					float ceGreekValue = 0f;
					if (!ceStraddleOptionName.equals(ceOptionname)) {
						if (!ceStraddleOptionName.equals("")) { // Exit and re enter
							fileLogTelegramWriter.write( " Exiting ="+ceStraddleOptionName );
							// Exit CE
							if (this.placeActualOrder) {
								placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
							ceStraddleOptionName = "";
						}
						if (this.noOfOrders-ignoredOrders<maxAllowedNoOfOrders) {
							ceStraddleOptionName =  ceOptionname;
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
					} else {
						fileLogTelegramWriter.write( " Retaining ="+ceStraddleOptionName);
					}
					
					String peOptionname = entryStraddleOptionNames[1];
					float peGreekValue = 0f;
					if (!peStraddleOptionName.equals(peOptionname)) {
						if (!peStraddleOptionName.equals("")) { // Exit and re enter
							fileLogTelegramWriter.write( " Exiting ="+peStraddleOptionName );
							if (this.placeActualOrder) {
								placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
							peStraddleOptionName = "";
						}
						if (this.noOfOrders-ignoredOrders<maxAllowedNoOfOrders) {
							peStraddleOptionName =  peOptionname;
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
					} else {
						fileLogTelegramWriter.write( " Retaining ="+peStraddleOptionName);
					}
					
					if (!ceStraddleOptionName.equals("")) {
						ceOptionGreeks = getOptionGreeks(ceStraddleOptionName);
						if (this.greekname.equalsIgnoreCase("delta")) {
							ceGreekValue =  Math.abs(ceOptionGreeks.getDelta());	
						} else if (this.greekname.equalsIgnoreCase("iv")) {
							ceGreekValue = ceOptionGreeks.getIv();	
						} else if (this.greekname.equalsIgnoreCase("ltp")) {
							ceGreekValue = ceOptionGreeks.getLtp();	
						}
					}
					if (!peStraddleOptionName.equals("")) {
						peOptionGreeks = getOptionGreeks(peStraddleOptionName);
						if (this.greekname.equalsIgnoreCase("delta")) {
							peGreekValue = Math.abs(peOptionGreeks.getDelta());	
						} else if (this.greekname.equalsIgnoreCase("iv")) {
							peGreekValue = peOptionGreeks.getIv();	
						} else if (this.greekname.equalsIgnoreCase("ltp")) {
							peGreekValue = peOptionGreeks.getLtp();	
						}
					}
					straddlePremiumWhenFormed = ceGreekValue + peGreekValue;
				}
				
				if (currentATMStraddlePremium < straddlePremiumMinReached) {
					straddlePremiumMinReached = currentATMStraddlePremium;
				}
				if (currentATMStraddlePremium > straddlePremiumMaxReached) {
					straddlePremiumMaxReached = currentATMStraddlePremium;
				}
				
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
	
	private int getOutlierCount(String optionType, int allowedCount) {
		int retVal = 0;
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			String fetchSql = "select countceoutlier, countpeoutlier from nexcorio_option_atm_movement_data where f_main_instrument = " + this.mainInstrument.getId() + ""
					+ " and record_time <= '" + postgresLongDateFormat.format(getCurrentTime()) + "'"
					+ " order by record_time desc limit 5";
			fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
			ResultSet rs = stmt.executeQuery(fetchSql);
			
			int ceCount = 0;
			int peCount = 0;
			while (rs.next()) {
				float ceGreek = rs.getFloat("countceoutlier");
				float peGreek = rs.getFloat("countpeoutlier");
				
				fileLogTelegramWriter.write("ceGreek="+ceGreek+" peGreek="+peGreek);
				
				ceCount = (int) (ceCount + ceGreek);
				peCount = (int) (peCount + peGreek);
			}
			rs.close();			
			stmt.close();
			
			ceCount = ceCount/5;
			peCount = peCount/5;
			
			fileLogTelegramWriter.write("ceCount="+ceCount+" peCount="+peCount);
			
			if (optionType.equals("CE") && ceCount >= allowedCount) retVal = allowedCount+1;
			else if (optionType.equals("PE") && peCount >= allowedCount) retVal = allowedCount+1;
			else if (optionType.equals("CE")) retVal = ceCount;
			else if (optionType.equals("PE")) retVal = peCount;
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
