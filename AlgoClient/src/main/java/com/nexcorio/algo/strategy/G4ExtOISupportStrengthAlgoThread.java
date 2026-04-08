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

public class G4ExtOISupportStrengthAlgoThread extends G3BaseClass implements Runnable{

	private static final Logger log = LogManager.getLogger(G4ExtOISupportStrengthAlgoThread.class);
		
	public float baseDelta = 0.5f;	
	public int topOIs = 5;
	
	public G4ExtOISupportStrengthAlgoThread(Long napAlgoId, String backTestDateStr) {
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
						log.debug(logString);
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
						log.debug(logString);
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
			
			int ceCount = 0;
			int peCount = 0;
			
			float totalCeOI = 0;
			float totalPeOI = 0;
			
			float totalCeOIWorth = 0;
			float totalPeOIWorth = 0;
			
			int top5CeCount = 0;
			int top5PeCount = 0;
			
			int recCount = 0;
			List<Integer> ceStrikes = new ArrayList<Integer>();
			List<Integer> peStrikes = new ArrayList<Integer>();
			for(OptionGreek aGreek:optionGreeks ) {
				if (aGreek.getOi()*aGreek.getLtp()/10000000 > 10) {
					
					String tradingSymbol = aGreek.getTradingSymbol();
					int strikePrice = aGreek.getStrike();
					float openInterest = aGreek.getOi();
					float oiWorth = aGreek.getOi()*aGreek.getLtp()/10000000;
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
				if (recCount >= this.topOIs) break;
			}
			
			Collections.sort(ceStrikes);
			Collections.sort(peStrikes, Collections.reverseOrder());
			
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
			
			if (totalCeOIWorth > 2*totalPeOIWorth) { // too much CE
				retVal = "CE";
			} else if (totalPeOIWorth > 2*totalCeOIWorth) { // too much CE
				retVal = "PE";
			} else if (ceGap>peGap) {
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
		
		new G4ExtOISupportStrengthAlgoThread(525L, "2025-03-06 09:50:00" );
	
	}
}
