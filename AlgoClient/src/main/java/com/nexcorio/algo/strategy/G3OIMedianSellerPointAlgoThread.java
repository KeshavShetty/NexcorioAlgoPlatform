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

public class G3OIMedianSellerPointAlgoThread extends G3BaseClass implements Runnable{

	private static final Logger log = LogManager.getLogger(G3OIMedianSellerPointAlgoThread.class);
	
	public boolean filterOptionWorth = false;
	public int topOis = 5;
	
	public G3OIMedianSellerPointAlgoThread(Long napAlgoId, String backTestDateStr) {
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
			
			float maxProfitReached = 0f;
			Date maxProfitReachedAt = getCurrentTime();
			float maxLowestpointReached = 0f;
			Date maxLowestpointReachedAt = getCurrentTime();
			float maxTrailingProfit = 0f;
			
			updateAlgoStatus("Running");
			
			do {
				sleep(15); // Every 1 minute
				
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
				
				String[] entryStraddleOptionNames = getCenterStrikeFromOI(100);
				
				if (!ceStraddleOptionName.equals(entryStraddleOptionNames[0])) {
					if (!ceStraddleOptionName.equals("")) { // Exit CE, taking Directional
						fileLogTelegramWriter.write( " Exiting ="+ceStraddleOptionName );
						// Exit CE
						if (this.placeActualOrder) {
							placeRealOrder( ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						ceStraddleOptionName = "";
					}
					if(!entryStraddleOptionNames[0].equals("")) {
						if (this.noOfOrders<maxAllowedNoOfOrders) {
							ceStraddleOptionName =  entryStraddleOptionNames[0];
							float cePrice = getOptionGreeks(ceStraddleOptionName).getLtp();
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
				}
				if (!peStraddleOptionName.equals(entryStraddleOptionNames[1])) {
					if (!peStraddleOptionName.equals("")) { // Exit PE, taking Directional
						fileLogTelegramWriter.write( " Exiting ="+peStraddleOptionName );
						// Exit PE
						if (this.placeActualOrder) {
							placeRealOrder( peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						peStraddleOptionName = "";
					}
					if(!entryStraddleOptionNames[1].equals("")) {
						if (this.noOfOrders<maxAllowedNoOfOrders) {
							peStraddleOptionName =  entryStraddleOptionNames[1];
							float pePrice = getOptionGreeks(peStraddleOptionName).getLtp();
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
				
				checkExitSignals();
				
				if ( (runningCePrice+runningPePrice)>0 && (runningCePrice+runningPePrice)<5f ) {
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
	
	
	private String[] getCenterStrikeFromOI(int distance) {
		String[] retStr = null;
		Connection conn = null;
		String top4Options ="";
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			List<OptionGreek> optionGreeks = new ArrayList<OptionGreek>();
			
			if (this.backtestDate == null) { // Live
				optionGreeks = getSnapshotGreeksFromCache();
			} else { // First try to fetch from Snapshot table
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
				for(String optionname:optionnames ) {
					OptionGreek aGreek = getOptionGreeks(optionname);
					if (aGreek!=null) {
						optionGreeks.add(aGreek);
					}
				}
			}
			stmt.close();
			
			Collections.sort(optionGreeks, new SortbyOI());
			
			List<OptionGreek> ceGreeks = new ArrayList<OptionGreek>();
			List<OptionGreek> peGreeks = new ArrayList<OptionGreek>();
			
			int recProcessed = 0;
			for(OptionGreek aGreek: optionGreeks) {
				if (aGreek.getOi()*aGreek.getLtp()/10000000>10) {
					recProcessed++;
					
					String tradingSymbol = aGreek.getTradingSymbol();
					
					top4Options = top4Options + tradingSymbol +" ";
					if (tradingSymbol.endsWith("CE")) {
						ceGreeks.add(aGreek);
					} else {
						peGreeks.add(aGreek);
					}
				}
				if (recProcessed>=this.topOis) break;
			}
			
			int commonStrike = 0;
			for(OptionGreek aCeGreek: ceGreeks) {
				int ceStrike = getStrikePriceFromOptionName(aCeGreek.getTradingSymbol());
				for(OptionGreek aPeGreek: peGreeks) {
					int peStrike = getStrikePriceFromOptionName(aPeGreek.getTradingSymbol());
					if (ceStrike == peStrike) {
						commonStrike = ceStrike;
					}
				}
				if (commonStrike!=0) break;
			}
			
			String optionnamePrefix = getCurrentWeekExpiryOptionnamePrefix();
			
			String localCeStraddleOptionName =  optionnamePrefix + (commonStrike) + "CE";
			String localPeStraddleOptionName =  optionnamePrefix + (commonStrike) + "PE";
			
			
			if (commonStrike==0) {
				String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(0.5f, this.optimalHedgeDistance);
				if(ceGreeks.size() > peGreeks.size()) {
					localCeStraddleOptionName = entryStraddleOptionNames[0]; //ceStraddleOptionName.equals("")?entryStraddleOptionNames[0]:ceStraddleOptionName;
					localPeStraddleOptionName = "";
				} else {
					localCeStraddleOptionName = "";
					localPeStraddleOptionName = entryStraddleOptionNames[1]; // peStraddleOptionName.equals("")?entryStraddleOptionNames[1]:peStraddleOptionName;
				}
				
			} else {
				if (this.instrumentLtp > commonStrike ) {
					localCeStraddleOptionName = "";
				} else {
					localPeStraddleOptionName = "";
				}
			}
			fileLogTelegramWriter.write("commonStrike="+commonStrike + "top4Options="+top4Options);
			
			String localCeHedgeOptionName =  "";
			String localPeHedgeOptionName =  "";
			if (hedgeDistance>0) {
				int centerStrike = getOptionCenterStrike(optionnamePrefix);
				localCeHedgeOptionName =  optionnamePrefix + (centerStrike+hedgeDistance) + "CE";
				localPeHedgeOptionName =  optionnamePrefix + (centerStrike-hedgeDistance) + "PE";
			} 
			fileLogTelegramWriter.write("localCeStraddleOptionName="+localCeStraddleOptionName+" localPeStraddleOptionName="+localPeStraddleOptionName);
			retStr = new String[]{localCeStraddleOptionName, localPeStraddleOptionName, localCeHedgeOptionName, localPeHedgeOptionName};
			
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return retStr;	
	}
	private String getOptionTrendFromOIWorth(String lastKnownOptiontrend) {
		String retVal = "StatusQuo";
		
		Connection conn = null;
		String top4Options ="";
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			List<OptionGreek> optionGreeks = new ArrayList<OptionGreek>();
			
			if (this.backtestDate == null) { // Live
				optionGreeks = getSnapshotGreeksFromCache();
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
				for(String optionname:optionnames ) {
					OptionGreek aGreek = getOptionGreeks(optionname);
					if (aGreek!=null) {
						optionGreeks.add(aGreek);
					}
				}
			}
			stmt.close();
			
			Collections.sort(optionGreeks, new SortbyOI());
			
			float ceOIWorth = 0f;
			float peOIWorth = 0f;
			
			float ceOICount = 0;
			float peOICount = 0;
			
			int recProcessed = 0;
			for(OptionGreek aGreek: optionGreeks) {
				if (aGreek.getOi()*aGreek.getLtp()/10000000>10) {
					recProcessed++;
					
					String tradingSymbol = aGreek.getTradingSymbol();
					float worthInCr = aGreek.getOi()*aGreek.getLtp()/10000000f;
					float openInterest = aGreek.getOi();
					top4Options = top4Options + tradingSymbol +" ";
					if (tradingSymbol.endsWith("CE")) {
						ceOIWorth = ceOIWorth + worthInCr;
						ceOICount = ceOICount + openInterest;
					} else {
						peOIWorth = peOIWorth + worthInCr;
						peOICount = peOICount + openInterest;
					}
				}
				if (recProcessed>=this.topOis) break;
			}
			
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
