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

public class G3AllOrNoneFuturesTrendAlgoThread extends G3BaseClass implements Runnable{

	private static final Logger log = LogManager.getLogger(G3AllOrNoneFuturesTrendAlgoThread.class);
	
	public float baseDelta = 0.5f;
	
	public G3AllOrNoneFuturesTrendAlgoThread(Long napAlgoId, String backTestDateStr) {
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
			
			String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(baseDelta, this.optimalHedgeDistance);
			
			String lastKnownTrend = "Unknown";
			
			String currentTrend = null;
			
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
				
				currentTrend = getSellerDirectionByFuturesTrend(); // Unknown, CE, PE
				
				if (!currentTrend.equals(lastKnownTrend)) {
					fileLogTelegramWriter.write( " Trend changed, repositioning");
					// Exit running position 
					if (!peStraddleOptionName.equals("")) { 
						fileLogTelegramWriter.write( " Exiting ="+peStraddleOptionName );
						// Exit PE
						if (this.placeActualOrder) {
							placeRealOrder( peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						peStraddleOptionName = "";
					}
					if (!ceStraddleOptionName.equals("")) { 
						fileLogTelegramWriter.write( " Exiting ="+ceStraddleOptionName );
						// Exit CE
						if (this.placeActualOrder) {
							placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						ceStraddleOptionName = "";
					}
				
					if (currentTrend.equals("CE") || currentTrend.equals("PE")) {
						if (this.noOfOrders<maxAllowedNoOfOrders) {
							entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(baseDelta, this.optimalHedgeDistance);
							
							if (currentTrend.equals("CE")) {
								ceStraddleOptionName =  entryStraddleOptionNames[0];
								float cePrice = getPriceFromTicks(ceStraddleOptionName);
								
								fileLogTelegramWriter.write( "Taking CE directional ceStraddleOptionName="+ceStraddleOptionName + "(@" + cePrice +") ceHedgeOptionName="+ceHedgeOptionName);
								ceDbId = createAlgoSellOrder(ceStraddleOptionName, cePrice, noOfLots*lotSize);
								if (this.placeActualOrder) {
									if (ceHedgeOptionName.equals("") ) {
										ceHedgeOptionName =  entryStraddleOptionNames[2];
										placeRealOrder( ceHedgeOptionName, noOfLots*lotSize, "BUY",  true, KiteUtil.USE_NORMAL_ORDER_FALSE);
									}
									placeRealOrder( ceDbId, ceStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
								}
							} else { // PE
								peStraddleOptionName =  entryStraddleOptionNames[1];
								float pePrice = getPriceFromTicks(peStraddleOptionName);
								
								fileLogTelegramWriter.write( "Taking PE directional peStraddleOptionName="+peStraddleOptionName + "(@" + pePrice +") peHedgeOptionName="+peHedgeOptionName);
								peDbId = createAlgoSellOrder(peStraddleOptionName, pePrice, noOfLots*lotSize);
								if (this.placeActualOrder) {
									if (peHedgeOptionName.equals("") ) {
										peHedgeOptionName =  entryStraddleOptionNames[3];
										placeRealOrder( peHedgeOptionName, noOfLots*lotSize, "BUY",  true, KiteUtil.USE_NORMAL_ORDER_FALSE);
									}
									placeRealOrder( peDbId , peStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
								}
							}
						} else {
							prepareExit("Too many order");
						}
					}
					lastKnownTrend = currentTrend;
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

		String retVal = "Unknown";
		
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
			
			if (totalBullishCount==3) {
				retVal = "PE";
			} else if (totalBearishCount==3) {
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
		new G3AllOrNoneFuturesTrendAlgoThread(23L, null);
	}

}
