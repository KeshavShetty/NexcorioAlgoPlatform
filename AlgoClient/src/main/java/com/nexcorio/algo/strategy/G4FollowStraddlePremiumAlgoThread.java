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

public class G4FollowStraddlePremiumAlgoThread extends G3BaseClass implements Runnable{

	private static final Logger log = LogManager.getLogger(G3PriceParityIVBasedAlgoThread.class);
	
	public float baseDelta = 0.5f;
	
	public float diffFromAtmPremium = 10f;
		
	public boolean wait4IdealPremium = Boolean.FALSE;
	
	public float suddenSpikeLimit = 0f;
	
	private float idealPremium = 0f;
	
	 
	
	public G4FollowStraddlePremiumAlgoThread(Long napAlgoId, String backTestDateStr) {
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
					else if (idealPremium > 100f + currentPremium) foundIdealpremium = true; // Check for last day expiry
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
				
				if (suddenSpikeLimit > 0f && !ceStraddleOptionName.equals("") && currentATMStraddlePremium > straddlePremiumMinReached+suddenSpikeLimit) {
					// Exit Exit Exit
					if (!ceStraddleOptionName.equals("")) { 
						fileLogTelegramWriter.write( "Sudden Spike Exiting ="+ceStraddleOptionName );
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
					sleep(30*60); // 30 minute
					this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
					currentATMStraddlePremium = getStradlePremium();
				}
				
				if (!isVolatilitySafe()) {
					if (!ceStraddleOptionName.equals("")) { // Exit and re enter
						fileLogTelegramWriter.write( "Unsafe Exiting ="+ceStraddleOptionName );
						// Exit CE
						if (this.placeActualOrder) {
							placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						ceStraddleOptionName = "";
						this.ignoredOrders++;
					}
					if (!peStraddleOptionName.equals("")) { // Exit and re enter
						fileLogTelegramWriter.write( "Unsafe Exiting ="+peStraddleOptionName );
						if (this.placeActualOrder) {
							placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						peStraddleOptionName = "";
						this.ignoredOrders++;
					}
				} else {
				
					boolean needRepositioning = false;
					
					if (ceStraddleOptionName.equals("")) {
						
						needRepositioning = true; // Just starting, no open positions
						
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
					
					if (needRepositioning) {
						String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(this.baseDelta, this.optimalHedgeDistance);
						
						String ceOptionname = entryStraddleOptionNames[0];
						
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
						straddlePremiumWhenFormed = currentATMStraddlePremium;
					}
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
	
	private boolean isVolatilitySafe() {
		boolean retVal = false;
		
		Connection conn = null;
		try {
			conn = HDataSource.getConnection();
			Statement stmt = conn.createStatement();
			
			float currentStraddlePremium = 0f;
			String fetchSql = "SELECT celtp+peltp as premium, adjustedCEATMLtp+adjustedPEATMLtp as adjustedPremium FROM nexcorio_option_atm_movement_data"
					+ " WHERE record_time <= '" + postgresLongDateFormat.format(getCurrentTime()) + "'"
					+ " AND f_main_instrument=" + this.mainInstrument.getId() 
					+ " ORDER BY record_time DESC LIMIT 1";
			fileLogTelegramWriter.write( "1. " + fetchSql); 
					
			ResultSet rs = stmt.executeQuery(fetchSql);
			while(rs.next()) {
				currentStraddlePremium = rs.getFloat("adjustedPremium");
			}
			rs.close();
			
			fetchSql = "SELECT min(adjustedCEATMLtp+adjustedPEATMLtp) as minPremium, max(adjustedCEATMLtp+adjustedPEATMLtp) as maxPremium FROM nexcorio_option_atm_movement_data"
					+ " WHERE record_time <= '" + postgresLongDateFormat.format(getCurrentTime()) + "'"
					+ " AND record_time > '" + postgresLongDateFormat.format(getCurrentTime(-5)) + "'"
					+ " AND f_main_instrument=" + this.mainInstrument.getId() ;
			fileLogTelegramWriter.write( "2. " + fetchSql);
			
			float minPremium = 0f;
			float maxPremium = 0f;
			rs = stmt.executeQuery(fetchSql);
			while(rs.next()) {
				minPremium = rs.getFloat("minPremium");
				maxPremium = rs.getFloat("maxPremium");
			}
			rs.close();
			
			float avgPoint = (minPremium+maxPremium)/2f;
			if (maxPremium-minPremium < 10f) {
				retVal = true;
			} else  if (currentStraddlePremium < avgPoint) {
				retVal = true;
			}
			fileLogTelegramWriter.write( "currentStraddlePremium="+currentStraddlePremium+" minPremium="+minPremium+" maxPremium="+maxPremium + " range="+ (maxPremium-minPremium)+ " avgPoint="+avgPoint+" isVolatilitySafe="+retVal);
			
			stmt.close();
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error"+e.getMessage(),e);
		} finally {
			try {
				if (conn!=null) conn.close();
			} catch (SQLException e) {
				log.error(e);
			}
		}
		
		return retVal;
	}
	
}
