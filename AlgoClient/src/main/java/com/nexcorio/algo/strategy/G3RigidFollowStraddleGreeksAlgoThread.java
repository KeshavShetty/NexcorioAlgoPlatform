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

public class G3RigidFollowStraddleGreeksAlgoThread extends G3BaseClass implements Runnable{

	private static final Logger log = LogManager.getLogger(G3PriceParityIVBasedAlgoThread.class);
	
	public float baseDelta = 0.5f;
	
	public float diffFromAtmGreeks = 10f;
	public boolean exitOnQuickChrun = false;
	public String greekname = "Theta";
	
	
	private String guidingStraddleCeOptionName = "";
	private String guidingStraddlePeOptionName = "";
	
	public G3RigidFollowStraddleGreeksAlgoThread(Long napAlgoId, String backTestDateStr) {
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
			Date lastOrderAt = null;
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
				
				boolean needRepositioning = false;
				
				if (ceStraddleOptionName.equals("")) { // Fresh entry
					needRepositioning = true; // Just starting, no open positions
				} else {
					float currentStradlleCenter = getStradlePremium()/2f;
					float runningCenter = (getOptionGreeks(guidingStraddleCeOptionName).getLtp()+getOptionGreeks(guidingStraddlePeOptionName).getLtp())/2f; 
					
					//float diffPercent = Math.abs(currentStradlleCenter - runningCenter)*100f/currentStradlleCenter;
					
					OptionGreek guidingCEGreek = getOptionGreeks(guidingStraddleCeOptionName);
					OptionGreek guidingPEGreek = getOptionGreeks(guidingStraddlePeOptionName);
					
					String[] entryStraddleOptionNames = getStraddleOptionNamesFromAtmData();
					
					OptionGreek currentCEGreek = getOptionGreeks(entryStraddleOptionNames[0]);
					OptionGreek currentPEGreek = getOptionGreeks(entryStraddleOptionNames[1]);
					
					float guidingCeGreekValue = Math.abs(guidingCEGreek.getTheta());
					float guidingPeGreekValue = Math.abs(guidingPEGreek.getTheta());
					
					float currentCeGreekValue = Math.abs(currentCEGreek.getTheta());
					float currentPeGreekValue = Math.abs(currentPEGreek.getTheta());
					
					if (greekname.equalsIgnoreCase("Vega")) {
						guidingCeGreekValue = Math.abs(guidingCEGreek.getVega());
						guidingPeGreekValue = Math.abs(guidingPEGreek.getVega());
						
						currentCeGreekValue = Math.abs(currentCEGreek.getVega());
						currentPeGreekValue = Math.abs(currentPEGreek.getVega());
					}
					
					float current2Use = currentCeGreekValue < currentPeGreekValue ? currentCeGreekValue : currentPeGreekValue;					
					float guiding2Use = guidingCeGreekValue > guidingPeGreekValue ? guidingCeGreekValue : guidingPeGreekValue;
					
					float diffPercent  = (current2Use - guiding2Use)*100f/current2Use;
					
					if (diffPercent>diffFromAtmGreeks) needRepositioning = true;
					fileLogTelegramWriter.write("currentStradlleCenter="+currentStradlleCenter+" runningCenter="+runningCenter+" diffPercent="+diffPercent+" needRepositioning="+needRepositioning 
							+ " currentCeTheta="+currentCeGreekValue+" currentPeTheta="+currentPeGreekValue+" guidingCeTheta="+guidingCeGreekValue+" guidingPeTheta="+guidingPeGreekValue
							+ " current2Use="+current2Use+" guiding2Use="+guiding2Use);
					
					if (needRepositioning==true && exitOnQuickChrun==true && getTimeDiff(lastOrderAt, getCurrentTime()) < 15) {
						// Exit all, Sleep 15 minutes, Reenter
						if (!ceStraddleOptionName.equals("")) { // Exit and re enter
							fileLogTelegramWriter.write( " Exiting ="+ceStraddleOptionName );
							// Exit CE
							if (this.placeActualOrder) {
								placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
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
						sleep(5*12*15);
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
					
					entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(0.5f, 0);
					guidingStraddleCeOptionName = entryStraddleOptionNames[0];
					guidingStraddlePeOptionName = entryStraddleOptionNames[1];
					lastOrderAt = getCurrentTime();
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
	
	private String[] getStraddleOptionNamesFromAtmData() {
		String[] retStr = null;
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			Integer instrumentIdToUse = this.mainInstrument.getId().intValue();
			
			String fetchSql = "select ceoptionname, peoptionname  from nexcorio_option_atm_movement_data where f_main_instrument = " + instrumentIdToUse + ""
					+ " and record_time <= '" + postgresLongDateFormat.format(getCurrentTime()) + "'"
					+ " order by record_time desc limit 5";
			
			fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
			
			String ceoptionname = "";
			String peoptionname = "";
			
			ResultSet rs = stmt.executeQuery(fetchSql);
			while (rs.next()) {
				ceoptionname = rs.getString("ceoptionname");
				peoptionname = rs.getString("peoptionname");
			}
			rs.close();
			
			retStr = new String[]{ceoptionname, peoptionname};
			
		} catch(Exception ex) {
			ex.printStackTrace();
			log.error("Error"+ex.getMessage(),ex);
		}finally {
			try {
				if (conn!=null) conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return retStr;
	}
	
}
