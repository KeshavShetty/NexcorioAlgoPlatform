package com.nexcorio.algo.strategy;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.util.KiteUtil;
import com.nexcorio.algo.util.db.HDataSource;

public class G3BiasdStrangleTriangularisationFuturesTrendAlgoThread extends G3BaseClass implements Runnable{

	private static final Logger log = LogManager.getLogger(G3BiasdStrangleTriangularisationFuturesTrendAlgoThread.class);
	
	public float baseDelta = 0.5f;
	public float deltaBias = 0.1f;
	
	public G3BiasdStrangleTriangularisationFuturesTrendAlgoThread(Long napAlgoId, String backTestDateStr) {
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
			
			String currentTrend = getSellerDirectionByFuturesTrend();
			
			String[] entryStraddleOptionNames1 = getStraddleOptionNamesByDeltaOptimised( baseDelta+deltaBias, this.optimalHedgeDistance);
			String[] entryStraddleOptionNames2 = getStraddleOptionNamesByDeltaOptimised( baseDelta-deltaBias, 0);
			
			if (currentTrend.equals("CE")) {
				ceStraddleOptionName =  entryStraddleOptionNames1[0];
				peStraddleOptionName =  entryStraddleOptionNames2[1];
			} else {
				ceStraddleOptionName =  entryStraddleOptionNames2[0];
				peStraddleOptionName =  entryStraddleOptionNames1[1];
			}
			
			ceHedgeOptionName =  entryStraddleOptionNames1[2];
			peHedgeOptionName =  entryStraddleOptionNames1[3];
				
			float cePrice = getPriceFromTicks(ceStraddleOptionName);
			float pePrice = getPriceFromTicks(peStraddleOptionName);
			
			ceDbId = createAlgoSellOrder(ceStraddleOptionName, cePrice, noOfLots*lotSize);
			peDbId = createAlgoSellOrder(peStraddleOptionName, pePrice, noOfLots*lotSize);
			
			if (this.placeActualOrder) { 
				placeRealOrder( ceHedgeOptionName, noOfLots*lotSize, "BUY",  true, KiteUtil.USE_NORMAL_ORDER_FALSE);
				placeRealOrder( ceDbId, ceStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);			 
				placeRealOrder( peHedgeOptionName, noOfLots*lotSize, "BUY",  true, KiteUtil.USE_NORMAL_ORDER_FALSE);
				placeRealOrder( peDbId , peStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
			}
			
			fileLogTelegramWriter.write( "Forming starddle ceStraddleOptionName="+ceStraddleOptionName + "(@" + cePrice +") ceHedgeOptionName="+ceHedgeOptionName 
						+ " peStraddleOptionName="+peStraddleOptionName + "(@" + pePrice +") peHedgeOptionName="+peHedgeOptionName);
			
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
				
				currentTrend = getSellerDirectionByFuturesTrend();
				
				fileLogTelegramWriter.write( " instrumentLtp=" + this.instrumentLtp +" currentTrend="+currentTrend + " currentProfit="+currentProfitPerUnit+" maxLowestpointReachedPerUnit="+(maxLowestpointReached)+" maxTrailingProfit="+maxTrailingProfit);
				
				if ( (currentTrend.equals("CE") && ceOptionGreeks.getDelta() < -peOptionGreeks.getDelta())
						|| (currentTrend.equals("PE") && ceOptionGreeks.getDelta() > -peOptionGreeks.getDelta()) ) {
					
					// Exit existing function
					fileLogTelegramWriter.write( " Exiting running straddle="+ceStraddleOptionName +" and " + peStraddleOptionName);
					if (this.placeActualOrder) {
						placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
					}
					updateCurrentOrderStatus(ceStraddleOptionName, ceDbId, "LegClosed");
					updateCurrentOrderStatus(peStraddleOptionName, peDbId, "LegClosed");
					ceStraddleOptionName = "";
					peStraddleOptionName = "";
					
					if (this.noOfOrders < maxAllowedNoOfOrders) {
						
						entryStraddleOptionNames1 = getStraddleOptionNamesByDeltaOptimised( baseDelta+deltaBias, 0);
						entryStraddleOptionNames2 = getStraddleOptionNamesByDeltaOptimised( baseDelta-deltaBias, 0);
						
						if (currentTrend.equals("CE")) {
							ceStraddleOptionName =  entryStraddleOptionNames1[0];
							peStraddleOptionName =  entryStraddleOptionNames2[1];
						} else {
							ceStraddleOptionName =  entryStraddleOptionNames2[0];
							peStraddleOptionName =  entryStraddleOptionNames1[1];
						}
							
						cePrice = getPriceFromTicks(ceStraddleOptionName);
						pePrice = getPriceFromTicks(peStraddleOptionName);
						
						fileLogTelegramWriter.write( "Forming starddle ceStraddleOptionName="+ceStraddleOptionName + "(@" + cePrice +") ceHedgeOptionName="+ceHedgeOptionName 
								+ " peStraddleOptionName="+peStraddleOptionName + "(@" + pePrice +") peHedgeOptionName="+peHedgeOptionName);
						
						ceDbId = createAlgoSellOrder(ceStraddleOptionName, cePrice, noOfLots*lotSize);
						peDbId = createAlgoSellOrder(peStraddleOptionName, pePrice, noOfLots*lotSize);
						
						if (this.placeActualOrder) {
							placeRealOrder( ceDbId, ceStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							placeRealOrder( peDbId , peStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
					} else {
						prepareExit("Too many orders");
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
	
	private String getSellerDirectionByFuturesTrend() {

		String retVal = "";
		
		Connection conn = null;
		try {
			conn = HDataSource.getConnection();
			Statement stmt = conn.createStatement();
			
			float totalBullishPoint = 0f;
			float totalBearishPoint = 0f;
			
			int totalBullishCount = 0;
			int totalBearishCount = 0;
			
			Map<Long, String> mainInstrumentExchangeMap = new HashMap<Long, String>();
			
			mainInstrumentExchangeMap.put(2L, "NSE"); // "NIFTY"
			mainInstrumentExchangeMap.put(3L, "NSE"); // "BANKNIFTY"
			mainInstrumentExchangeMap.put(4L, "BSE"); // "SENSEX"
			
			Iterator<Long> iter =  mainInstrumentExchangeMap.keySet().iterator();
			while(iter.hasNext()) {
				Long mainInstrumentId = iter.next();
				String exchange = mainInstrumentExchangeMap.get(mainInstrumentId);
				
				String futurePrefix = getNextNFUTUREExpiryDatePrefix(mainInstrumentId, exchange);
				
				String fetchSql = "SELECT count(*) as total, COUNT(DISTINCT CASE WHEN total_buy_qty > total_sell_qty THEN id END) as bullishCount,"
						+ " COUNT(DISTINCT CASE WHEN total_buy_qty < total_sell_qty THEN id END) as bearishCount"
						+ " FROM nexcorio_tick_data"
						+ " WHERE quote_time <='" + postgresLongDateFormat.format(getCurrentTime()) + "'"
						+ " AND  quote_time > '" + postgresLongDateFormat.format(getCurrentTime(-5)) + "'"
						+ " AND trading_symbol='" + futurePrefix + "'";
				
				fileLogTelegramWriter.write("For " + mainInstrumentId + "  fetchSql="+fetchSql);
				
				ResultSet rs = stmt.executeQuery(fetchSql);
				
				int totalEntry = 0;
				int bullishEntry = 0;
				int bearishEntry = 0;
				
				while (rs.next()) {
					totalEntry = rs.getInt("total");
					bullishEntry = rs.getInt("bullishCount");
					bearishEntry = rs.getInt("bearishCount");
				}
				rs.close();
				
				float bullishPoint = (float)bullishEntry/(float)totalEntry;
				float bearishPoint = (float)bearishEntry/(float)totalEntry;
				
				fileLogTelegramWriter.write("Local bullishPoint="+bullishPoint+" bearishPoint="+bearishPoint);
				
				totalBullishPoint = totalBullishPoint + bullishPoint;
				totalBearishPoint = totalBearishPoint + bearishPoint;
				
				if (bullishPoint > 0.50f) totalBullishCount++;
				else totalBearishCount++;
			}
			stmt.close();
			
			fileLogTelegramWriter.write(" totalBullishPoint="+totalBullishPoint+" totalBearishPoint="+totalBearishPoint+" totalBullishCount="+totalBullishCount + " totalBearishCount="+totalBearishCount);
			
			if (totalBullishPoint > totalBearishPoint) {
				retVal = "PE";
			} else {
				retVal = "CE";
			}
			
		} catch(Exception ex) {
			log.error("Error"+ex.getMessage(),ex);
			ex.printStackTrace();
		}finally {
			try {
				if (conn!=null) conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}	
		return retVal;
	
	}
	
	public static void main(String[] args) {
		new G3BiasdStrangleTriangularisationFuturesTrendAlgoThread(23L, null);
	}

}
