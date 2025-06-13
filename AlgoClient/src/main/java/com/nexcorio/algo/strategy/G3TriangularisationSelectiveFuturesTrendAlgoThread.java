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

import com.nexcorio.algo.dto.MainInstruments;
import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.util.KiteUtil;
import com.nexcorio.algo.util.db.HDataSource;

public class G3TriangularisationSelectiveFuturesTrendAlgoThread extends G3BaseClass implements Runnable{

	private static final Logger log = LogManager.getLogger(G3TriangularisationSelectiveFuturesTrendAlgoThread.class);
	
	public float baseDelta = 0.5f;
	public String selectiveAssetsMainInstrumentIds = null; // Comma separated
	
	private Map<Long, MainInstruments> selectiveAssetsMainInstrumentsMap = new HashMap<Long, MainInstruments>();
	
	public G3TriangularisationSelectiveFuturesTrendAlgoThread(Long napAlgoId, String backTestDateStr) {
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
			
			// Prefill MainInstruments			
			String[] selectiveAssetsIds = selectiveAssetsMainInstrumentIds.split(",");
			for(int i=0;i<selectiveAssetsIds.length;i++) {
				Long aId = Long.parseLong(selectiveAssetsIds[i].trim());
				MainInstruments aInstrument = getMainInstrumentDtoById(aId);
				selectiveAssetsMainInstrumentsMap.put(aId, aInstrument);
			}
			
			long ceDbId = -1;
			long peDbId = -1;
			
			this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
			
			fileLogTelegramWriter.write( " this.instrumentLtp="+this.instrumentLtp);
			
			String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(baseDelta, this.optimalHedgeDistance);
			
			String lastKnownTrend = "Unknown";
			
			String currentTrend = null;
			do {
				currentTrend = getSellerDirectionByFuturesTrend(lastKnownTrend);
				if (currentTrend.equals("Unknown")) sleep(15);
			} while (currentTrend.equals(lastKnownTrend));
			
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
			}
			
			lastKnownTrend = currentTrend;
			
			float maxProfitReached = 0f;
			Date maxProfitReachedAt = getCurrentTime();
			float maxLowestpointReached = 0f;
			Date maxLowestpointReachedAt = getCurrentTime();
			float maxTrailingProfit = 0f;
			
			updateAlgoStatus("Running");
			
			do {
				sleep(15); // Every 10sec
				
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
				
				currentTrend = getSellerDirectionByFuturesTrend(lastKnownTrend); // StatusQuo, CE, PE
				
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
	
	private String getSellerDirectionByFuturesTrend( String lastKnownTrend) {

		String retVal = lastKnownTrend;
		
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			float totalBullishPoint = 0f;
			float totalBearishPoint = 0f;
			
			int totalBullishCount = 0;
			int totalBearishCount = 0;
			
		
			Iterator<Long> iter =  selectiveAssetsMainInstrumentsMap.keySet().iterator();
			while(iter.hasNext()) {
				Long mainInstrumentId = iter.next();
				String exchange = selectiveAssetsMainInstrumentsMap.get(mainInstrumentId).getExchange();
				
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
		new G3TriangularisationSelectiveFuturesTrendAlgoThread(23L, null);
	}

}
