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

public class G3OISupportStrengthAlgoThread extends G3BaseClass implements Runnable{

	private static final Logger log = LogManager.getLogger(G3OISupportStrengthAlgoThread.class);
		
	public float baseDelta = 0.5f;	
	public int topOIs = 5;
	
	public G3OISupportStrengthAlgoThread(Long napAlgoId, String backTestDateStr) {
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
			
			String lastKnownTrend = "Unknown";
			
			do {
				sleep(5); // Quick to react
				
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
				fileLogTelegramWriter.write( " instrumentLtp=" + this.instrumentLtp +" currentProfit="+currentProfitPerUnit+" maxLowestpointReachedPerUnit="+(maxLowestpointReached)+" maxTrailingProfit="+maxTrailingProfit);
				
				String currentTrend = getSellerDirectionBySupportStrength();
				
				if (!lastKnownTrend.equals(currentTrend)) {
					// Exit existing leg
					fileLogTelegramWriter.write(" Exiting");
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
					
					fileLogTelegramWriter.write(" Forming condition 1");
					
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
					lastKnownTrend = currentTrend;
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
			fileLogTelegramWriter.write( " noOfOrders="+noOfOrders + " ROI=" + (currentProfitPerUnit*this.lotSize*100f)/requiredMargin + "% (Max profit reached to "+ (maxProfitReached) +"@" + maxProfitReachedAt+ "\n and Lowest reached to " + (maxLowestpointReached) + "@" + maxLowestpointReachedAt + ")");
		} catch (Exception e) {			
			updateAlgoStatus("Error");
			log.error("Error"+e.getMessage(), e);
			fileLogTelegramWriter.write("Error " + ExceptionUtils.getStackTrace(e));
		} finally {
			fileLogTelegramWriter.close();
		}
	}
	
	private String getSellerDirectionBySupportStrength() {
		String retVal = "Neutral";
		Connection conn = null;
		String top4Options ="";
		String logString = "";
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			boolean filterOptionWorth = true;
			
			String optionnamePrefix = getCurrentWeekExpiryOptionnamePrefix();
			
			String opOIFetch = "select trading_symbol, oi as open_interest, strike, oi*ltp/10000000 as worthInCr from nexcorio_option_snapshot"
					+ " where trading_symbol like '" + optionnamePrefix + "%' and record_date = '" + postgresShortDateFormat.format(getCurrentTime()) +"' "
					+ (filterOptionWorth==true?" and oi*ltp/10000000>10":"")  + " order by oi desc limit "+this.topOIs;
			
			// Todo:backtest support pending
			
			log.info("opOIFetch="+opOIFetch);
			int ceCount = 0;
			int peCount = 0;
			
			int top5CeCount = 0;
			int top5PeCount = 0;
			
			float totalCeOI = 0;
			float totalPeOI = 0;
			
			float totalCeOIWorth = 0;
			float totalPeOIWorth = 0;
			
			ResultSet rs = stmt.executeQuery(opOIFetch);
			int recCount = 0;
			List<Integer> ceStrikes = new ArrayList<Integer>();
			List<Integer> peStrikes = new ArrayList<Integer>();
			while (rs.next()) {
				String tradingSymbol = rs.getString("trading_symbol");
				int strikePrice = (int) rs.getFloat("strike");
				float openInterest = rs.getFloat("open_interest");
				float oiWorth = rs.getFloat("worthInCr");
				top4Options = top4Options + tradingSymbol +" ";
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
				recCount++;
			}
			rs.close();			
			stmt.close();
			
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
			} else {
				ceGap = 2000;
				ceSupprotDistance4mIndex = 2000f;
			}
			
			if (peStrikes.size()>1) {
				peGap = peStrikes.get(0) - peStrikes.get(1);
				peSupprotDistance4mIndex = this.instrumentLtp - peStrikes.get(0);
			} else {
				peGap = 2000;
				peSupprotDistance4mIndex = 2000f;
			}
			
			float gapRatio = ceGap>peGap?((float)peGap/(float)ceGap):((float)ceGap/(float)peGap);
			fileLogTelegramWriter.write(" ceGap="+ceGap+" peGap="+peGap+" gapRatio="+gapRatio+" ceSupprotDistance4mIndex="+ceSupprotDistance4mIndex+" peSupprotDistance4mIndex="+peSupprotDistance4mIndex);
			
			if (ceGap>peGap) {
				retVal = "CE";
			} else if (ceGap<peGap) {
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
		
		new G3OISupportStrengthAlgoThread(525L, "2025-03-06 09:50:00" );
	
	}
}
