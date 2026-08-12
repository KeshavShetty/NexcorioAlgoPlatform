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

public class G3DeltaProbablityAdjustedOIWorthSellerDirectionAlgoThread extends G3BaseClass implements Runnable{

	private static final Logger log = LogManager.getLogger(G3DeltaProbablityAdjustedOIWorthSellerDirectionAlgoThread.class);
	
	public float baseDelta = 0.5f;
	public boolean filterOptionWorth = false;
	
	public boolean useScaledDelta = false;
	
	public int topOis = 5;
	
	public G3DeltaProbablityAdjustedOIWorthSellerDirectionAlgoThread(Long napAlgoId, String backTestDateStr) {
		super(napAlgoId);
		initializeParameters(backTestDateStr);
		
		fileLogTelegramWriter.write(this.algoname);
		Thread t = new Thread(this, this.mainInstrument.getShortName()+this.algoname);
		t.setPriority(Thread.MAX_PRIORITY);
		t.start();
	}
	
	@Override
	public void run() {
		
		printFields(this);
		
		try {
			long ceDbId = -1;
			long peDbId = -1;
						
			this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
			
			fileLogTelegramWriter.write( " this.instrumentLtp="+this.instrumentLtp);
			
			String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised( baseDelta, this.hedgeDistance);
			String lastKnownOptiontrend = "CE";
			String optiontrend = getOptionTrendFromOIWorth(lastKnownOptiontrend);
			lastKnownOptiontrend = optiontrend;
			if (optiontrend.equals("CE")) {
				ceStraddleOptionName =  entryStraddleOptionNames[0];
				ceHedgeOptionName =  entryStraddleOptionNames[2];
				
				float cePrice = getPriceFromTicks(ceStraddleOptionName);
				
				String logString = "Taking CE directional ceStraddleOptionName="+ceStraddleOptionName + "(@" + cePrice +") ceHedgeOptionName="+ceHedgeOptionName; 
				log.info(logString);
				fileLogTelegramWriter.write( " "+logString);
				ceDbId = createAlgoSellOrder(ceStraddleOptionName, cePrice, noOfLots*lotSize);
				if (this.placeActualOrder) { // Place the straddle order with Kite
					placeRealOrder(ceHedgeOptionName, noOfLots*lotSize, "BUY",  true, KiteUtil.USE_NORMAL_ORDER_FALSE);
					placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
				}
			} else { // PE
				peStraddleOptionName =  entryStraddleOptionNames[1];
				peHedgeOptionName =  entryStraddleOptionNames[3];
				
				float pePrice = getPriceFromTicks(peStraddleOptionName);
				String logString = "Taking PE directional peStraddleOptionName="+peStraddleOptionName + "(@" + pePrice +") peHedgeOptionName="+peHedgeOptionName; 
				log.info(logString);
				fileLogTelegramWriter.write( " "+logString);
				peDbId = createAlgoSellOrder(peStraddleOptionName, pePrice, noOfLots*lotSize);
				if (this.placeActualOrder) { // Place the straddle order with Kite
					placeRealOrder( peHedgeOptionName, noOfLots*lotSize, "BUY",  true, KiteUtil.USE_NORMAL_ORDER_FALSE);
					placeRealOrder( peDbId , peStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
				}
			}
			
			float maxProfitReached = 0f;
			Date maxProfitReachedAt = getCurrentTime();
			float maxLowestpointReached = 0f;
			Date maxLowestpointReachedAt = getCurrentTime();
			float maxTrailingProfit = 0f;
			
			updateAlgoStatus("Running");
			
			do {
				sleep(60); // Every 1 minute
				
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
				
				optiontrend = getOptionTrendFromOIWorth(lastKnownOptiontrend); // StatusQuo, CE, PE
				
				fileLogTelegramWriter.write( " optiontrend="+optiontrend);
			
				if (!lastKnownOptiontrend.equals(optiontrend)) {
					entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised( baseDelta, this.hedgeDistance);
					
					if (optiontrend.equals("CE")) {
						// Exit PE
						// Enter CE
						if (!peStraddleOptionName.equals("")) { // Exit PE, taking Directional
							fileLogTelegramWriter.write( " Exiting ="+peStraddleOptionName );
							// Exit PE
							if (this.placeActualOrder) {
								placeRealOrder( peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
							peStraddleOptionName = "";
						}
						if (ceStraddleOptionName.equals("")) {
							if (this.noOfOrders<maxAllowedNoOfOrders) {
								ceStraddleOptionName =  entryStraddleOptionNames[0];
								float cePrice = getPriceFromTicks(ceStraddleOptionName);
								fileLogTelegramWriter.write( " Entering ="+ceStraddleOptionName +"(@"+cePrice+")");
								// Place order
								ceDbId = createAlgoSellOrder(ceStraddleOptionName, cePrice, noOfLots*lotSize);
								if (this.placeActualOrder) {
									if (ceHedgeOptionName.equals("")) {								
										ceHedgeOptionName =  entryStraddleOptionNames[2];
										placeRealOrder( ceHedgeOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
									}
									placeRealOrder( ceDbId, ceStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
								}
							} else {
								prepareExit("Too many orders");
							}
							
						}
					} else if (optiontrend.equals("PE")) {
						// Exit CE
						// Enter PE
						if (!ceStraddleOptionName.equals("")) { // Exit CE, taking Directional
							fileLogTelegramWriter.write( " Exiting ="+ceStraddleOptionName );
							// Exit CE
							if (this.placeActualOrder) {
								placeRealOrder( ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
							ceStraddleOptionName = "";
						}
						if (peStraddleOptionName.equals("")) {
							if (this.noOfOrders<maxAllowedNoOfOrders) {
								peStraddleOptionName =  entryStraddleOptionNames[1];
								float pePrice = getPriceFromTicks(peStraddleOptionName);
								fileLogTelegramWriter.write( "Entering ="+peStraddleOptionName +"(@"+pePrice+")");
								// Place order
								peDbId = createAlgoSellOrder(peStraddleOptionName, pePrice, noOfLots*lotSize);
								if (this.placeActualOrder) {
									if (peHedgeOptionName.equals("")) {
										peHedgeOptionName =  entryStraddleOptionNames[3];
										placeRealOrder( peHedgeOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
									}
									placeRealOrder( peDbId, peStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
								}
							} else {
								prepareExit("Too many orders");
							}
						}
					} 
				}
				lastKnownOptiontrend = optiontrend;
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
			fileLogTelegramWriter.write( " noOfOrders="+noOfOrders + " ROI=" + (currentProfitPerUnit*100f)/requiredMargin + "% (Max profit/lot reached to "+ (maxProfitReached) +"@" + maxProfitReachedAt+ "\n and Lowest reached to " + (maxLowestpointReached) + "@" + maxLowestpointReachedAt + ")");
			
		} catch (Exception e) {			
			updateAlgoStatus("Error");
			log.error("Error"+e.getMessage(), e);
			fileLogTelegramWriter.write("Error " + ExceptionUtils.getStackTrace(e));
		} finally {
			fileLogTelegramWriter.close();
		}
	}
	
	private String getOptionTrendFromOIWorth(String lastKnownOptiontrend) {
		String retVal = "StatusQuo";
		
		Connection conn = null;
		String top4Options ="";
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			String optionnamePrefix = getCurrentWeekExpiryOptionnamePrefix();
			
			float ceOIWorth = 0f;
			float peOIWorth = 0f;
			
			if (backtestDate == null) {
				String opOIFetch = "select trading_symbol, delta, oi as open_interest, oi*ltp/10000000 as worthInCr from nexcorio_option_snapshot where trading_symbol like '" + optionnamePrefix + "%' and record_date = '" + postgresShortDateFormat.format(getCurrentTime()) +"' "
						+ (filterOptionWorth==true?" and oi*ltp/10000000>10":"")  + " order by oi desc limit "+this.topOis;
				
				fileLogTelegramWriter.write("opOIFetch="+opOIFetch);
				ResultSet rs = stmt.executeQuery(opOIFetch);
				
				while (rs.next()) {
					String tradingSymbol = rs.getString("trading_symbol");
					float worthInCr = rs.getFloat("worthInCr");
					float delta = rs.getFloat("delta");
					top4Options = top4Options + tradingSymbol +" ";
					
					if (tradingSymbol.endsWith("CE")) {
						if (useScaledDelta) ceOIWorth = ceOIWorth + worthInCr*(1f-delta);				
						else ceOIWorth = ceOIWorth + worthInCr*delta;
					} else {
						if (useScaledDelta) peOIWorth = peOIWorth + worthInCr*(1f+delta);
						else peOIWorth = peOIWorth + worthInCr*-delta;
					}
				}
				rs.close();
			} else {
				
				// First try to fetch from Snapshot table
				String fetchSql = "select DISTINCT(trading_symbol) as trading_symbol from nexcorio_option_snapshot"
						+ " where trading_symbol like '" + mainInstrument.getShortName() + "%' "
						+ " and record_date = '" + postgresShortDateFormat.format(getCurrentTime()) + "'";
				fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
				
				List<String> optionnames = new ArrayList<>();			
				ResultSet rs = stmt.executeQuery(fetchSql);
				while (rs.next()) {
					optionnames.add(rs.getString("trading_symbol"));
				}
				rs.close();
				
				if (optionnames.size()==0) { // not found in snapshot		
					fetchSql = "select DISTINCT(trading_symbol) as trading_symbol from nexcorio_option_greeks"
							+ " where f_main_instrument = " + mainInstrument.getId() + " "
							+ " and quote_time > '" + postgresShortDateFormat.format(getCurrentTime()) + " 09:15:00'"
							+ " and quote_time < '" + postgresShortDateFormat.format(getCurrentTime()) + " 09:20:00'";
								
					rs = stmt.executeQuery(fetchSql);
					while (rs.next()) {
						optionnames.add(rs.getString("trading_symbol"));
					}
					rs.close();
					
					// Insert to snapshot
					for(String aSymbol: optionnames) {
						String insertSql = "INSERT INTO nexcorio_option_snapshot (id, trading_symbol, record_date)"
								+ " VALUES (nextval('nexcorio_option_snapshot_id_seq'),'" + aSymbol + "','" + postgresShortDateFormat.format(getCurrentTime()) + "')";
						stmt.executeUpdate(insertSql);
					}
				}
				List<OptionGreek> optionGreeks = getOptionGreeks(optionnames, 0);
//				for(String optionname:optionnames ) {
//					OptionGreek aGreek = getOptionGreeks(optionname);
//					if (aGreek!=null) {
//						optionGreeks.add(aGreek);
//					}
//				}
				Collections.sort(optionGreeks, new SortbyOiDesc());
				
				int recProcessed = 0;
				for(OptionGreek aGreek: optionGreeks) {
					if (aGreek.getOi()*aGreek.getLtp()/10000000>10) {
						recProcessed++;
						String tradingSymbol = aGreek.getTradingSymbol();
						float worthInCr = aGreek.getOi()*aGreek.getLtp()/10000000f;
						float delta = aGreek.getDelta();
						top4Options = top4Options + tradingSymbol +" ";
						
						if (tradingSymbol.endsWith("CE")) {
							if (useScaledDelta) ceOIWorth = ceOIWorth + worthInCr*(1f-delta);				
							else ceOIWorth = ceOIWorth + worthInCr*delta;
						} else {
							if (useScaledDelta) peOIWorth = peOIWorth + worthInCr*(1f+delta);
							else peOIWorth = peOIWorth + worthInCr*-delta;
						}
					}
					if (recProcessed>=this.topOis) break;
				}				
			}
			stmt.close();
			
			if (ceOIWorth-peOIWorth>10) {
				retVal = "CE";
			} else if (peOIWorth-ceOIWorth>10) {
				retVal = "PE";
			} else {
				retVal = lastKnownOptiontrend;
			}
			String logString = " ceOIWorth="+ceOIWorth+" peOIWorth="+peOIWorth +" retVal="+retVal+" top4Options="+top4Options;
			fileLogTelegramWriter.write( logString);
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
		
	}

	

}
