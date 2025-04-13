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

public class G3SpikeAversionStrangleAlgoThread extends G3BaseClass implements Runnable{

	private static final Logger log = LogManager.getLogger(G3PriceParityIVBasedAlgoThread.class);
	
	public float baseDelta = 0.25f;
	public String greekname = "delta";
	
	public float premiumSpikePercent = 8f;
	
	public float premiumDiff = 0f;
	public float acceptableDiff = 0f;	
	public float rollingPoints = 0f;
	
	public G3SpikeAversionStrangleAlgoThread(Long napAlgoId, String backTestDateStr) {
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
			float currentProfitPerLot =0f;
			float maxTrailingProfit = 0f;
			
			updateAlgoStatus("Running");
			
			float lowestATMStraddlePremium = getATMStraddlePremium();
			float highestATMStraddlePremium = lowestATMStraddlePremium;
			
			float totalPremiumCaptured = 0f;
			float indexWhenStraddleFormed = 0f;
			do {
				sleep(15); // Quick to react
				
				this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
				
				OptionGreek ceOptionGreeks = !ceStraddleOptionName.equals("")?getOptionGreeks(ceStraddleOptionName):null;
				OptionGreek peOptionGreeks = !peStraddleOptionName.equals("")?getOptionGreeks(peStraddleOptionName):null;
				print(ceOptionGreeks, peOptionGreeks);
				
				float runningCePrice = ceOptionGreeks==null?0: ceOptionGreeks.getLtp();
				float runningPePrice = peOptionGreeks==null?0: peOptionGreeks.getLtp();
				
				if (!ceStraddleOptionName.equals("")) updateCurrentOrderBuyPrice(ceStraddleOptionName, ceDbId, runningCePrice);
				if (!peStraddleOptionName.equals("")) updateCurrentOrderBuyPrice(peStraddleOptionName, peDbId, runningPePrice);
				
				currentProfitPerUnit = getProfitFromDB();
				currentProfitPerLot = currentProfitPerUnit*lotSize;
				if (currentProfitPerLot>maxProfitReached) {
					maxProfitReached=currentProfitPerLot;
					maxProfitReachedAt = getCurrentTime();
				}
				if (currentProfitPerLot<maxLowestpointReached) {
					maxLowestpointReached=currentProfitPerLot;
					maxLowestpointReachedAt = getCurrentTime();
				}
				trailingProfit = (currentProfitPerLot-maxProfitReached)/lotSize;
				if (trailingProfit<maxTrailingProfit) {
					maxTrailingProfit = trailingProfit;
				}
				fileLogTelegramWriter.write( " instrumentLtp=" + this.instrumentLtp +" currentProfit="+currentProfitPerUnit+" maxLowestpointReachedPerUnit="+(maxLowestpointReached/lotSize)+" maxTrailingProfit="+maxTrailingProfit);
				
				fileLogTelegramWriter.write("lowestATMStraddlePremium="+ lowestATMStraddlePremium+" highestATMStraddlePremium="+highestATMStraddlePremium+" Entry at "
						+ (highestATMStraddlePremium*(100f - premiumSpikePercent)/100f) + " Exit at " + ( lowestATMStraddlePremium*(100f + premiumSpikePercent)/100f) );  
				
				float currentATMStraddlePremium = getATMStraddlePremium();
				
				if (ceStraddleOptionName.equals("")) { // No open position
					if (currentATMStraddlePremium < highestATMStraddlePremium*(100f - premiumSpikePercent)/100f) {
						String[] entryStraddleOptionNames = getStraddleOptionNamesByGreekOptimised(greekname, baseDelta, this.hedgeDistance);
						
						ceStraddleOptionName =  entryStraddleOptionNames[0];
						peStraddleOptionName =  entryStraddleOptionNames[1];
						
						ceOptionGreeks = !ceStraddleOptionName.equals("")?getOptionGreeks(ceStraddleOptionName):null;
						peOptionGreeks = !peStraddleOptionName.equals("")?getOptionGreeks(peStraddleOptionName):null;
						print(ceOptionGreeks, peOptionGreeks);
						
						String logString = "Forming straddleceStraddleOptionName="+ceStraddleOptionName + "(@" + ceOptionGreeks.getLtp() +") ceHedgeOptionName="+ceHedgeOptionName+" " + peStraddleOptionName + "(@" + peOptionGreeks.getLtp() +") peHedgeOptionName="+peHedgeOptionName; 
						fileLogTelegramWriter.write( " "+logString);
						
						ceDbId = createAlgoSellOrder(ceStraddleOptionName, ceOptionGreeks.getLtp(), noOfLots*lotSize);
						peDbId = createAlgoSellOrder(peStraddleOptionName, peOptionGreeks.getLtp(), noOfLots*lotSize);
						
						if (ceHedgeOptionName.equals("")) {
							ceHedgeOptionName =  entryStraddleOptionNames[2];
							if (this.placeActualOrder) {
								placeRealOrder(ceHedgeOptionName, noOfLots*lotSize, "BUY",  false, KiteUtil.USE_NORMAL_ORDER_FALSE);	
							}
						}
						if (peHedgeOptionName.equals("")) {
							peHedgeOptionName =  entryStraddleOptionNames[3];
							if (this.placeActualOrder) {
								placeRealOrder(peHedgeOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);	
							}
						}
						
						if (this.placeActualOrder) { // Place the straddle order with Kite
							placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						
						totalPremiumCaptured = ceOptionGreeks.getLtp() + peOptionGreeks.getLtp();
						
						highestATMStraddlePremium = currentATMStraddlePremium;
						lowestATMStraddlePremium  = currentATMStraddlePremium;
						indexWhenStraddleFormed = this.instrumentLtp;
						
						fileLogTelegramWriter.write( "Forming indexWhenStraddleFormed="+indexWhenStraddleFormed);
					}
				} else { // Already positions running, check for exit rule
					if (currentATMStraddlePremium > lowestATMStraddlePremium*(100f + premiumSpikePercent)/100f
							) { // && currentATMStraddlePremium > atmPremiumWhenStraddleFormed
						fileLogTelegramWriter.write( " Exiting running straddle="+ceStraddleOptionName +" and " + peStraddleOptionName);
						if (this.placeActualOrder) {
							placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						updateCurrentOrderStatus(ceStraddleOptionName, ceDbId, "LegClosed");
						updateCurrentOrderStatus(peStraddleOptionName, peDbId, "LegClosed");
						ceStraddleOptionName = "";
						peStraddleOptionName = "";
						
						highestATMStraddlePremium = currentATMStraddlePremium;
						lowestATMStraddlePremium  = currentATMStraddlePremium;
						
						if (this.noOfOrders >= maxAllowedNoOfOrders) {
							prepareExit("Too many orders");
						}
					}
				}
				
				if (currentATMStraddlePremium > highestATMStraddlePremium) highestATMStraddlePremium = currentATMStraddlePremium;
				if (currentATMStraddlePremium < lowestATMStraddlePremium)  lowestATMStraddlePremium  = currentATMStraddlePremium;
				
				if (!ceStraddleOptionName.equals("")) {
					
					boolean realignmentRequired = false;
					if (premiumDiff > 0f) {
						float currentPremium = ceOptionGreeks.getLtp() + peOptionGreeks.getLtp();
						if (currentPremium - totalPremiumCaptured > premiumDiff) realignmentRequired = true;
					}
					fileLogTelegramWriter.write( "After 1. realignment required? " + realignmentRequired);
					if (realignmentRequired == false && acceptableDiff > 0f) {
						float greekSum = 0f;
						float greekDiff = 0f;
						if (greekname.equals("delta")) {					
							greekSum = Math.abs(ceOptionGreeks.getDelta()) + Math.abs(peOptionGreeks.getDelta());
							greekDiff = Math.abs( Math.abs(ceOptionGreeks.getDelta()) - Math.abs(peOptionGreeks.getDelta()) );
						} else if (greekname.equals("ltp")) {
							float ceLtp = Math.abs(ceOptionGreeks.getLtp());
							float peLtp = Math.abs(peOptionGreeks.getLtp());
							greekSum = ceLtp + peLtp;
							greekDiff = Math.abs( ceLtp - peLtp );
						} else if (greekname.equals("vega")) {					
							greekSum = Math.abs(ceOptionGreeks.getVega()) + Math.abs(peOptionGreeks.getVega());
							greekDiff = Math.abs( Math.abs(ceOptionGreeks.getVega()) - Math.abs(peOptionGreeks.getVega()) );
						}
						
						float diffRatio = greekDiff/greekSum;
						fileLogTelegramWriter.write(" greekSum="+greekSum+" greekDiff=" + " DiffRatio="+diffRatio);
						
						if (diffRatio > this.acceptableDiff) {
							realignmentRequired = true;
						}
					}
					fileLogTelegramWriter.write( "After 2. realignment required? " + realignmentRequired);
					
					if (realignmentRequired == false && rollingPoints > 0f) {
						fileLogTelegramWriter.write( "this.instrumentLtp="+this.instrumentLtp+ " indexWhenStraddleFormed="+indexWhenStraddleFormed+" upper="+(indexWhenStraddleFormed+this.rollingPoints) +
								"lower="+ (indexWhenStraddleFormed-this.rollingPoints));
						if (this.instrumentLtp > indexWhenStraddleFormed+this.rollingPoints
								|| this.instrumentLtp < indexWhenStraddleFormed-this.rollingPoints) {
							realignmentRequired = true;
						}
					}
					fileLogTelegramWriter.write( "After 3. realignment required? " + realignmentRequired);
					
					fileLogTelegramWriter.write( "After 4. realignment required? " + realignmentRequired);
					
					if (realignmentRequired == true) {
						fileLogTelegramWriter.write( "Realignment required");
						if (this.noOfOrders<maxAllowedNoOfOrders) {
							fileLogTelegramWriter.write( " Exiting running straddle="+ceStraddleOptionName +" and " + peStraddleOptionName);
							// Exit PE
							if (this.placeActualOrder) {
								placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
								placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
							updateCurrentOrderStatus(ceStraddleOptionName, ceDbId, "LegClosed");
							updateCurrentOrderStatus(peStraddleOptionName, peDbId, "LegClosed");
							
							String[] entryStraddleOptionNames = getStraddleOptionNamesByGreekOptimised(greekname, baseDelta, this.hedgeDistance);
							
							ceStraddleOptionName =  entryStraddleOptionNames[0];					
							peStraddleOptionName =  entryStraddleOptionNames[1];
								
							float cePrice = getPriceFromTicks(ceStraddleOptionName);
							float pePrice = getPriceFromTicks(peStraddleOptionName);
							
							fileLogTelegramWriter.write("Forming straddleceStraddleOptionName="+ceStraddleOptionName + "(@" + cePrice +") "+peStraddleOptionName + "(@" + pePrice +")");
							
							ceDbId = createAlgoSellOrder(ceStraddleOptionName, cePrice, noOfLots*lotSize);
							peDbId = createAlgoSellOrder(peStraddleOptionName, pePrice, noOfLots*lotSize);
							
							if (this.placeActualOrder) { // Place the straddle order with Kite
								placeRealOrder( ceDbId, ceStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
								placeRealOrder( peDbId, peStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
							totalPremiumCaptured = cePrice + pePrice;
							indexWhenStraddleFormed = this.instrumentLtp;
						} else {
							prepareExit("Need alignment, But too many orders");
						}
					}
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
				saveAlgoDailySummary(currentProfitPerLot, maxProfitReached, maxProfitReachedAt, maxLowestpointReached, maxLowestpointReachedAt, maxTrailingProfit);
			} while(!exitThread);
			updateAlgoStatus("Terminated");
			String logString = "Exiting Strddle ceStraddleOptionName="+ceStraddleOptionName + " peStraddleOptionName="+peStraddleOptionName; 
			log.info(logString);
			fileLogTelegramWriter.write( " " + logString);
			// exit all positions
			if (this.placeActualOrder) exitStraddle(ceDbId, peDbId);
			fileLogTelegramWriter.write( " noOfOrders="+noOfOrders + " ROI=" + (currentProfitPerLot*100f)/requiredMargin + "% (Max profit/lot reached to "+ (maxProfitReached) +"@" + maxProfitReachedAt+ "\n and Lowest reached to " + (maxLowestpointReached) + "@" + maxLowestpointReachedAt + ")");
			
		} catch (Exception e) {			
			updateAlgoStatus("Error");
			log.error("Error"+e.getMessage(), e);
			fileLogTelegramWriter.write("Error " + ExceptionUtils.getStackTrace(e));
		} finally {
			fileLogTelegramWriter.close();
		}
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
	
}
