package com.nexcorio.algo.strategy;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.util.KiteUtil;
import com.nexcorio.algo.util.db.HDataSource;

public class G3GreekParityPremiumSpikeAversionAlgoThread extends G3BaseClass implements Runnable{

	private static final Logger log = LogManager.getLogger(G3GreekParityPremiumSpikeAversionAlgoThread.class);
		
	public float baseDelta = 0.5f;
	
	
	public String greekname = "iv";	
	public float premiumSpikePercent = 8f;
	public float ivDiffCutoffPercent = 5f;
	
	public G3GreekParityPremiumSpikeAversionAlgoThread(Long napAlgoId, String backTestDateStr) {
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
			
			float lowestATMStraddlePremium = getATMStraddlePremium();
			float highestATMStraddlePremium = lowestATMStraddlePremium;
			
			String lastKnownTrend = "Unknown";
			
			do {
				sleep(15); // Quick to react
				
				this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
				
				OptionGreek ceOptionGreeks = getOptionGreeks(ceStraddleOptionName);
				OptionGreek peOptionGreeks = getOptionGreeks(peStraddleOptionName );
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
				fileLogTelegramWriter.write( " instrumentLtp=" + this.instrumentLtp +" currentProfit="+currentProfitPerUnit+" maxLowestpointReachedPerUnit="+(maxLowestpointReached/lotSize)+" maxTrailingProfit="+maxTrailingProfit);
				
				fileLogTelegramWriter.write("lowestATMStraddlePremium="+ lowestATMStraddlePremium+" highestATMStraddlePremium="+highestATMStraddlePremium+" Entry at "
						+ (highestATMStraddlePremium*(100f - premiumSpikePercent)/100f) + " Exit at " + ( lowestATMStraddlePremium*(100f + premiumSpikePercent)/100f) );  
				
				float currentATMStraddlePremium = getATMStraddlePremium();
				
				if (lastKnownTrend.equals("Unknown")) { // No open position
					if (currentATMStraddlePremium < highestATMStraddlePremium*(100f - premiumSpikePercent)/100f) {
						
						fileLogTelegramWriter.write(" Forming condition 1");
						
						String currentTrend = getSellerDirectionByATMIVParity( lastKnownTrend); // CE, PE
						String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised( baseDelta, this.hedgeDistance);
						
						if (currentTrend.equals("CE")) {
							ceStraddleOptionName =  entryStraddleOptionNames[0];
							
							float cePrice = getPriceFromTicks(ceStraddleOptionName);
							
							String logString = "Taking CE directional ceStraddleOptionName="+ceStraddleOptionName + "(@" + cePrice +") ceHedgeOptionName="+ceHedgeOptionName; 
							log.info(logString);
							fileLogTelegramWriter.write( " "+logString);
							ceDbId = createAlgoSellOrder(ceStraddleOptionName, cePrice, noOfLots*lotSize);
							
							if (ceHedgeOptionName.equals("")) {
								ceHedgeOptionName =  entryStraddleOptionNames[2];
								if (this.placeActualOrder) {
									placeRealOrder(ceHedgeOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);	
								}
							}							
							if (this.placeActualOrder) { // Place the order with Kite
								placeRealOrder( ceDbId, ceStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
						} else { // PE
							peStraddleOptionName =  entryStraddleOptionNames[1];
														
							float pePrice = getPriceFromTicks(peStraddleOptionName);
							String logString = "Taking PE directional peStraddleOptionName="+peStraddleOptionName + "(@" + pePrice +") peHedgeOptionName="+peHedgeOptionName; 
							log.info(logString);
							fileLogTelegramWriter.write( " "+logString);
							peDbId = createAlgoSellOrder(peStraddleOptionName, pePrice, noOfLots*lotSize);
							
							if (peHedgeOptionName.equals("")) {
								peHedgeOptionName =  entryStraddleOptionNames[3];
								if (this.placeActualOrder) {
									placeRealOrder(peHedgeOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);	
								}
							} 
							if (this.placeActualOrder) { 
								placeRealOrder( peDbId , peStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
						}
						highestATMStraddlePremium = currentATMStraddlePremium;
						lowestATMStraddlePremium  = currentATMStraddlePremium;
						
						lastKnownTrend = currentTrend;
					} else {
						fileLogTelegramWriter.write( "Wait to cool down premium spike");
					}
				} else { // Position already exist
					if (currentATMStraddlePremium > lowestATMStraddlePremium*(100f + 2.5f*premiumSpikePercent)/100f) { // Straddle Premium Spike, exit position
						
						fileLogTelegramWriter.write(" Forming condition 2");
						
						if (!ceStraddleOptionName.equals("")) {
							fileLogTelegramWriter.write( " Exiting ="+ceStraddleOptionName );
							if (this.placeActualOrder) {
								placeRealOrder( ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
							updateCurrentOrderStatus(ceStraddleOptionName, ceDbId, "LegClosed");
							ceStraddleOptionName = "";
						}
						if (!peStraddleOptionName.equals("")) {
							fileLogTelegramWriter.write( " Exiting ="+peStraddleOptionName );
							if (this.placeActualOrder) {
								placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
							updateCurrentOrderStatus(peStraddleOptionName, peDbId, "LegClosed");
							peStraddleOptionName = "";
						}
						highestATMStraddlePremium = currentATMStraddlePremium;
						lowestATMStraddlePremium  = currentATMStraddlePremium;
						lastKnownTrend = "Unknown";
					} else { // Check change in direction
						String currentTrend = getSellerDirectionByATMIVParity(lastKnownTrend);
						if (!currentTrend.equals(lastKnownTrend)) {
							fileLogTelegramWriter.write(" Forming condition 3");
							String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised( baseDelta, this.hedgeDistance);
							
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
												placeRealOrder(this.userId, ceHedgeOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
											}
											placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
										}
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
										placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
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
									} else {
										prepareExit("Too many orders");
									}
								} else {
									fileLogTelegramWriter.write( " Retaining ="+peStraddleOptionName);
								}
							}
							lastKnownTrend = currentTrend;
						}
					}	
				}
						
				if (currentATMStraddlePremium > highestATMStraddlePremium) highestATMStraddlePremium = currentATMStraddlePremium;
				if (currentATMStraddlePremium < lowestATMStraddlePremium)  lowestATMStraddlePremium  = currentATMStraddlePremium;
				
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
			fileLogTelegramWriter.write( " noOfOrders="+noOfOrders + " ROI=" + (currentProfitPerUnit*this.lotSize*100f)/requiredMargin + "% (Max profit reached to "+ (maxProfitReached) +"@" + maxProfitReachedAt+ "\n and Lowest reached to " + (maxLowestpointReached) + "@" + maxLowestpointReachedAt + ")");
		} catch (Exception e) {			
			updateAlgoStatus("Error");
			log.error("Error"+e.getMessage(), e);
			fileLogTelegramWriter.write("Error " + ExceptionUtils.getStackTrace(e));
		} finally {
			fileLogTelegramWriter.close();
		}
	}
	
	private String getSellerDirectionByATMIVParity(String lastKnownTrend) {
		String retVal = lastKnownTrend;
		
		Connection conn = null;
		try {
			conn = HDataSource.getConnection();
			Statement stmt = conn.createStatement();
			
			String fieldname = "ceiv as ceGreek, peiv as peGreek";
			if (this.greekname.equalsIgnoreCase("ltp")) {
				fieldname = "celtp as ceGreek, peltp as peGreek";
			} else if (this.greekname.equalsIgnoreCase("gamma")) {
				fieldname = "cegamma as ceGreek, pegamma as peGreek";
			} 
			
			String fetchSql = "select " + fieldname + " from nexcorio_option_atm_movement_data where f_main_instrument = " + this.mainInstrument.getId() + ""
					+ " and record_time <= '" + postgresLongDateFormat.format(getCurrentTime()) + "'"
					+ " order by record_time desc limit 1";
			fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
			ResultSet rs = stmt.executeQuery(fetchSql);
			
			
			float ceGreek = 0f;
			float peGreek = 0f;
			while (rs.next()) {
				ceGreek = rs.getFloat("ceGreek");
				peGreek = rs.getFloat("peGreek");
			}
			rs.close();			
			stmt.close();
			
			float greekDiffPercent = getPercentDiff(ceGreek, peGreek);
			if (ceGreek < peGreek)
				greekDiffPercent = -greekDiffPercent;
			
			fileLogTelegramWriter.write("ceGreek="+ceGreek + " peGreek="+peGreek + " greekDiffPercent="+greekDiffPercent);
			
			if (greekDiffPercent >= ivDiffCutoffPercent || greekDiffPercent <= -ivDiffCutoffPercent ) {
				fileLogTelegramWriter.write("Cutoff breached");
				if (greekDiffPercent > 0f) {
					retVal = "PE";
				} else {
					retVal = "CE";
				}
			}
			
			if (retVal.equals("Unknown") ) {
				if (greekDiffPercent > 0f) {
					retVal = "PE";
				} else {
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
	
	private float getATMStraddlePremium() {
		float avgeAtmPremium = 0f;
		
		Connection conn = null;
		try {
			conn = HDataSource.getConnection();
			Statement stmt = conn.createStatement();
			
			String fetchSql = "select celtp+peltp as atmPremium from nexcorio_option_atm_movement_data where f_main_instrument = " + this.mainInstrument.getId() +""
					+ " and base_delta > 0.49 and base_delta < 0.51 ";
			
			if (this.backtestDate!=null) {
				fetchSql = fetchSql + " and record_time <= '" + postgresLongDateFormat.format(getCurrentTime()) + "'";
			}
			fetchSql = fetchSql + " order by record_time desc limit 3";
			
			fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
			ResultSet rs = stmt.executeQuery(fetchSql);
			
			// We only need avg(2 closest among 3), this is because we observed some sharp big spike/fall in atm premium lasting 1 sec or burst found, to eliminate such outlier 
			
			List<Float> allNumbers = new ArrayList<>();
			while (rs.next()) {
				float currentAtmPremium = rs.getFloat("atmPremium");
				allNumbers.add(currentAtmPremium);
				avgeAtmPremium = avgeAtmPremium + currentAtmPremium;
			}
			rs.close();			
			stmt.close();
			
			avgeAtmPremium = avgeAtmPremium/3f;
			
			List<Float> leftSideNumbers = new ArrayList<>();
			List<Float> rightSideNumbers = new ArrayList<>();
			
			for(int i=0;i<allNumbers.size();i++) {
				if (allNumbers.get(i) > avgeAtmPremium) leftSideNumbers.add(allNumbers.get(i));
				else rightSideNumbers.add(allNumbers.get(i));
			}
			
			if (leftSideNumbers.size()>1) {
				avgeAtmPremium = (leftSideNumbers.get(0) + leftSideNumbers.get(1))/2f;
			} else {
				avgeAtmPremium = (rightSideNumbers.get(0) + rightSideNumbers.get(1))/2f;
			}
			fileLogTelegramWriter.write("In getATMStraddlePremium returning="+avgeAtmPremium);
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
			
		return avgeAtmPremium;
	}
	
	public static void main(String[] args) {
		
		new G3GreekParityPremiumSpikeAversionAlgoThread(525L, "2025-03-06 09:50:00" );
	
	}
}
