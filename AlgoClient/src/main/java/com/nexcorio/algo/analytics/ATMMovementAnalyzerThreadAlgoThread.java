package com.nexcorio.algo.analytics;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.kite.KiteCache;
import com.nexcorio.algo.util.FileLogTelegramWriter;
import com.nexcorio.algo.util.KiteUtil;
import com.nexcorio.algo.util.db.HDataSource;

public class ATMMovementAnalyzerThreadAlgoThread extends AnalyticsBaseClass implements Runnable {

	private static final Logger log = LogManager.getLogger(ATMMovementAnalyzerThreadAlgoThread.class);
	
	String futuresTradingSymbol;
	
	public ATMMovementAnalyzerThreadAlgoThread(String instrumentName, String backDateStr) {
		super();
		
		this.mainInstrument = KiteCache.getTradingSymbolMainInstrumentCache(instrumentName);
		
		this.algoname=this.mainInstrument.getShortName() + "ATMMovementAnalyzer";
		
		if (backDateStr!=null) {
			try {
				Calendar cal = Calendar.getInstance();
				cal.setTime(postgresLongDateFormat.parse(backDateStr));
				this.backtestDate = cal;
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		
		Thread t = new Thread(this, this.mainInstrument.getShortName()+this.algoname);
		t.setPriority(Thread.MAX_PRIORITY);
		t.start();
	}
	
	@Override
	public void run() {
		try {			
			fileLogTelegramWriter = new FileLogTelegramWriter(this.mainInstrument.getShortName(), this.algoname, this.backtestDate);
			
			while  ( getCurrentTime().before(KiteUtil.getDailyCustomTime(getCurrentTime(), 9, 15, 5 )) )  {
				log.debug("Too early for ATM data, going to sleep for 30 seconds");
				System.out.println("Too early for ATM data, going to sleep for 30 seconds");
				sleep(30);
			}
			System.out.println("Time has come to process ATM Data");
			futuresTradingSymbol = getNextNFUTUREExpiryDatePrefix(this.mainInstrument.getId(), this.mainInstrument.getExchange());
			do {
				
				
				//System.out.println("Going to sleep");
				sleep(5);				
				//System.out.println("Wakreup");
				fileLogTelegramWriter.write("====================================================================================================");
				this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
				fileLogTelegramWriter.write("instrumentLtp="+instrumentLtp);
				processATMMovement();
					
				if (timeout(15, 29, 0)) {
					prepareExit(" Exiting: Timeout");
				}
				//this.exitThread = true;
				
			} while(!this.exitThread);
			
			fileLogTelegramWriter.close();
		} catch (Exception e) {			
			log.error("Error"+e.getMessage(), e);
		}
	}
	
	private void processATMMovement() {
		try {
			StringBuffer logStr = new StringBuffer();
			Long beginTime = System.currentTimeMillis();
			Long startTime = System.currentTimeMillis();
			
			//Map<String, Integer> futuresMap = getFutureStandOff();
			Long elapsedTime1 = System.currentTimeMillis();
			//logStr.append(", Time taken for getFutureStandOff=" +(elapsedTime1-startTime));
			//startTime = elapsedTime1;
			
			float futuresLtp = getPriceFromTicks(futuresTradingSymbol);
			elapsedTime1 = System.currentTimeMillis();
			logStr.append(", Time taken for getPriceFromTicks=" +(elapsedTime1-startTime));
			startTime = elapsedTime1;
			
			Map<String, Float> aggregateGreeks = getAggregateGreeksDetails();
			elapsedTime1 = System.currentTimeMillis();
			logStr.append(", Time taken for getAggregateGreeksDetails=" +(elapsedTime1-startTime));
			startTime = elapsedTime1;
			
			Map<String, Float> selective5StrikeAggregateGreeks = getSelectiveGreeksDetails(5);
			Map<String, Float> selective10StrikeAggregateGreeks = getSelectiveGreeksDetails(10);
			Map<String, Float> selective20StrikeAggregateGreeks = getSelectiveGreeksDetails(20);
			elapsedTime1 = System.currentTimeMillis();
			logStr.append(", Time taken for getSelectiveAvgGamma=" +(elapsedTime1-startTime));
			startTime = elapsedTime1;
			
			Map<String, Float> deltaRangeGreeksDetails = getDeltaRangeGreeksDetails();
			elapsedTime1 = System.currentTimeMillis();
			logStr.append(", Time taken for getDeltaRangeGreeksDetails=" +(elapsedTime1-startTime));
			startTime = elapsedTime1;
			
			Map<String, OptionGreek> atmGreeksMap = processAndSaveRawStraddleData(0.5f,  
					aggregateGreeks.get("TotalCEOI"), aggregateGreeks.get("TotalPEOI"), 
					aggregateGreeks.get("TotalCEIV"), aggregateGreeks.get("TotalPEIV"),
					aggregateGreeks.get("TotalCEGamma"), aggregateGreeks.get("TotalPEGamma"), 
					aggregateGreeks.get("TotalCEVega"), aggregateGreeks.get("TotalPEVega"),
					aggregateGreeks.get("AvgCEGamma"), aggregateGreeks.get("AvgPEGamma"),
					selective5StrikeAggregateGreeks.get("AvgCEGamma"), selective5StrikeAggregateGreeks.get("AvgPEGamma"),
					selective5StrikeAggregateGreeks.get("AvgCEIv"), selective5StrikeAggregateGreeks.get("AvgPEIv"),
					
					selective10StrikeAggregateGreeks.get("AvgCEGamma"), selective10StrikeAggregateGreeks.get("AvgPEGamma"),
					selective10StrikeAggregateGreeks.get("AvgCEIv"), selective10StrikeAggregateGreeks.get("AvgPEIv"),
					
					selective20StrikeAggregateGreeks.get("AvgCEGamma"), selective20StrikeAggregateGreeks.get("AvgPEGamma"),
					selective20StrikeAggregateGreeks.get("AvgCEIv"), selective20StrikeAggregateGreeks.get("AvgPEIv"),
					
					futuresLtp, deltaRangeGreeksDetails
					
					);
			elapsedTime1 = System.currentTimeMillis();
			logStr.append(", Time taken for processAndSaveRawStraddleData=" +(elapsedTime1-startTime));
			
			Long endTime = System.currentTimeMillis();
			Long timeTaken = endTime-beginTime;
			if (timeTaken>200) {
				log.error("Delay in ATMMovementAnalyzer " + this.mainInstrument.getShortName() +" timeTaken="+timeTaken+logStr.toString());
			}
			
		} catch (Exception e) {
			log.error("Error"+e.getMessage(),e);
			e.printStackTrace();
		}
	}
	
	private Map<String, Float> getAggregateGreeksDetails() {
		Map<String, Float> retMap = new HashMap<>();
		
		if(this.backtestDate == null) { // live
			List<OptionGreek> greekList = getSnapshotGreeks();
			float totalCEOI = 0f;
			float totalCEIV = 0f; // It is actually avg IV
			float totalCEGamma = 0f;
			float avgCEGamma = 0f;
			float totalCEVega = 0f;
			int ceRecCount = 0;
			
			float totalPEOI = 0f;
			float totalPEIV = 0f;
			float totalPEGamma = 0f;
			float avgPEGamma = 0f;
			float totalPEVega = 0f;
			int peRecCount = 0;
			
			for(OptionGreek aGreek :greekList) {
				if (aGreek.getTradingSymbol().endsWith("CE")) {
					ceRecCount++;
					totalCEOI = totalCEOI + aGreek.getOi();
					totalCEIV = totalCEIV + aGreek.getIv();
					totalCEGamma = totalCEGamma + aGreek.getGamma();
					totalCEVega = totalCEVega + aGreek.getVega();
				}
				
				if (aGreek.getTradingSymbol().endsWith("PE")) {
					peRecCount++;
					totalPEOI = totalPEOI + aGreek.getOi();
					totalPEIV = totalPEIV + aGreek.getIv();
					totalPEGamma = totalPEGamma + aGreek.getGamma();
					totalPEVega = totalPEVega + aGreek.getVega();
				}
			}
			totalCEIV = ceRecCount!=0?totalCEIV/(float)ceRecCount:0f;
			totalPEIV = peRecCount!=0?totalPEIV/(float)peRecCount:0f;
			
			avgCEGamma = ceRecCount>0?totalCEGamma/(float)ceRecCount:0f; 
			avgPEGamma = peRecCount>0?totalPEGamma/(float)peRecCount:0f;
			
			retMap.put("TotalCEOI", totalCEOI);
			retMap.put("TotalCEIV", totalCEIV);
			retMap.put("TotalCEGamma", totalCEGamma);
			retMap.put("AvgCEGamma", avgCEGamma);
			retMap.put("TotalCEVega", totalCEVega);
			
			retMap.put("TotalPEOI", totalPEOI);
			retMap.put("TotalPEIV", totalPEIV);
			retMap.put("TotalPEGamma", totalPEGamma);
			retMap.put("AvgPEGamma", avgPEGamma);
			retMap.put("TotalPEVega", totalPEVega);
		} else { // Get from snapshot table
			Connection conn = null;
			try {			
				conn = HDataSource.getReadOnlyConnection();
				Statement stmt = conn.createStatement();
				
				String optionnamePrefix = getCurrentWeekExpiryOptionnamePrefix();
				
				String 
				fetchSql = "select sum(oi) as totalOI, avg(iv) as totalIV, sum(gamma) as totalGamma, avg(gamma) as avgGamma, sum(vega) as totalVega from nexcorio_option_snapshot where record_date = '" + postgresShortDateFormat.format(getCurrentTime())+ "' and trading_symbol LIKE '" + optionnamePrefix + "%CE'";
				fileLogTelegramWriter.write(fetchSql);
				
				ResultSet rs = stmt.executeQuery(fetchSql);
				
				float totalCEOI = 0f;
				float totalCEIV = 0f;
				float totalCEGamma = 0f;
				float avgCEGamma = 0f;
				float totalCEVega = 0f;
				while (rs.next()) {
					totalCEOI = rs.getFloat("totalOI");
					totalCEIV = rs.getFloat("totalIV");
					totalCEGamma = rs.getFloat("totalGamma");
					avgCEGamma = rs.getFloat("avgGamma");
					totalCEVega = rs.getFloat("totalVega");
				}
				rs.close();
				retMap.put("TotalCEOI", totalCEOI);
				
				retMap.put("TotalCEIV", totalCEIV);
				retMap.put("TotalCEGamma", totalCEGamma);
				retMap.put("AvgCEGamma", avgCEGamma);
				retMap.put("TotalCEVega", totalCEVega);
				
				
				fetchSql = "select sum(oi) as totalOI, avg(iv) as totalIV, sum(gamma) as totalGamma, avg(gamma) as avgGamma, sum(vega) as totalVega from nexcorio_option_snapshot where record_date = '" + postgresShortDateFormat.format(getCurrentTime())+ "' and trading_symbol LIKE '" + optionnamePrefix + "%PE'";
				fileLogTelegramWriter.write(fetchSql);
				
				rs = stmt.executeQuery(fetchSql);
				
				float totalPEOI = 0f;
				float totalPEIV = 0f;
				float totalPEGamma = 0f;
				float avgPEGamma = 0f;
				float totalPEVega = 0f;
				while (rs.next()) {
					totalPEOI = rs.getFloat("totalOI");
					totalPEIV = rs.getFloat("totalIV");
					totalPEGamma = rs.getFloat("totalGamma");
					avgPEGamma = rs.getFloat("avgGamma");
					totalPEVega = rs.getFloat("totalVega");
				}
				rs.close();
				retMap.put("TotalPEOI", totalPEOI);
				
				retMap.put("TotalPEIV", totalPEIV);
				retMap.put("TotalPEGamma", totalPEGamma);
				retMap.put("AvgPEGamma", avgPEGamma);
				retMap.put("TotalPEVega", totalPEVega);
				
				stmt.close();
			} catch (Exception e) {
				e.printStackTrace();
				log.error("Error"+e.getMessage(),e);
			} finally {
				try {
					conn.close();
				} catch (SQLException e) {
					log.error(e);
				}
			}
		}
		return retMap;
	}
	
	private Map<String, Float> getSelectiveGreeksDetails(int noOflegs) {
		Map<String, Float> retMap = new HashMap<>(); 
		
		if (this.backtestDate == null) { // Live
			List<OptionGreek> greekList = getSnapshotGreeks();
			
			float avgCEGamma = 0f;
			float avgCEIv = 0f;
			int ceRecCount = 0;
			
			float avgPEGamma = 0f;
			float avgPEIv = 0f;
			int peRecCount = 0;
			
			int upperBound = (int) (this.instrumentLtp + this.mainInstrument.getGapBetweenStrikes()*noOflegs);
			int lowerBound = (int) (this.instrumentLtp - this.mainInstrument.getGapBetweenStrikes()*noOflegs);
			
			for(OptionGreek aGreek :greekList) {
				int strike = getStrikePriceFromOptionName(aGreek.getTradingSymbol());
				if (aGreek.getTradingSymbol().endsWith("CE") && strike >=lowerBound && strike <= upperBound ) {
					avgCEGamma = avgCEGamma + aGreek.getGamma();
					avgCEIv = avgCEIv + aGreek.getIv();
					ceRecCount++;
				}
				if (aGreek.getTradingSymbol().endsWith("PE") && strike >=lowerBound && strike <= upperBound ) {
					avgPEGamma = avgPEGamma + aGreek.getGamma();
					avgPEIv = avgPEIv + aGreek.getIv();
					peRecCount++;
				}
			}
			avgCEGamma = ceRecCount>0?avgCEGamma/(float)ceRecCount:0f;
			avgPEGamma = peRecCount>0?avgPEGamma/(float)peRecCount:0f;
			
			avgCEIv = ceRecCount>0?avgCEIv/(float)ceRecCount:0f;
			avgPEIv = peRecCount>0?avgPEIv/(float)peRecCount:0f;
			
			retMap.put("AvgCEGamma", avgCEGamma);
			retMap.put("AvgCEIv", avgCEIv);
			
			retMap.put("AvgPEGamma", avgPEGamma);
			retMap.put("AvgPEIv", avgPEIv);
			
		} else {
			Connection conn = null;
			try {			
				conn = HDataSource.getReadOnlyConnection();
				Statement stmt = conn.createStatement();
				
				String optionnamePrefix = getCurrentWeekExpiryOptionnamePrefix();
				
				int upperBound = (int) (this.instrumentLtp + this.mainInstrument.getGapBetweenStrikes()*noOflegs);
				int lowerBound = (int) (this.instrumentLtp - this.mainInstrument.getGapBetweenStrikes()*noOflegs);
				
				String 
				fetchSql = "select avg(gamma) as avgGamma, avg(iv) as avgIv from nexcorio_option_snapshot where record_date = '" + postgresShortDateFormat.format(getCurrentTime())+ "' and trading_symbol LIKE '" + optionnamePrefix + "%CE'"
						+ " AND strike <= " + upperBound
						+ " AND strike >= " + lowerBound;
				fileLogTelegramWriter.write(fetchSql);
				
				ResultSet rs = stmt.executeQuery(fetchSql);
				
				float avgCEGamma = 0f;
				float avgCEIv = 0f;
				while (rs.next()) {
					avgCEGamma = rs.getFloat("avgGamma");
					avgCEIv = rs.getFloat("avgIv");
				}
				rs.close();
				
				retMap.put("AvgCEGamma", avgCEGamma);
				retMap.put("AvgCEIv", avgCEIv);
				
				fetchSql = "select avg(gamma) as avgGamma, avg(iv) as avgIv  from nexcorio_option_snapshot where record_date = '" + postgresShortDateFormat.format(getCurrentTime())+ "' and trading_symbol LIKE '" + optionnamePrefix + "%PE'"
						+ " AND strike <= " + upperBound
						+ " AND strike >= " + lowerBound;
				
				fileLogTelegramWriter.write(fetchSql);
				
				rs = stmt.executeQuery(fetchSql);
				
				float avgPEGamma = 0f;
				float avgPEIv = 0f;
				while (rs.next()) {
					avgPEGamma = rs.getFloat("avgGamma");
					avgPEIv = rs.getFloat("avgIv");
				}
				rs.close();
				
				retMap.put("AvgPEGamma", avgPEGamma);
				retMap.put("AvgPEIv", avgPEIv);
				
				stmt.close();
			} catch (Exception e) {
				e.printStackTrace();
				log.error("Error"+e.getMessage(),e);
			} finally {
				try {
					conn.close();
				} catch (SQLException e) {
					log.error(e);
				}
			}
		}
		return retMap;
	}
	
	private Map<String, Float> getDeltaRangeGreeksDetails() {
		Map<String, Float> retMap = new HashMap<>(); 
		
			Connection conn = null;
			try {			
				conn = HDataSource.getReadOnlyConnection();
				Statement stmt = conn.createStatement();
				
				String optionnamePrefix = getCurrentWeekExpiryOptionnamePrefix();
				
				float lowerDelta = 0.1f;
				float upperDelta = 0.9f;
				
				String 
				fetchSql = "select ltp, oi, iv, delta, vega, gamma, volume1min from nexcorio_option_snapshot where record_date = '" + postgresShortDateFormat.format(getCurrentTime())+ "'"
						+ " AND trading_symbol LIKE '" + optionnamePrefix + "%CE'"
						+ " AND delta >= " + lowerDelta
						+ " AND delta <= " + upperDelta
						+ " ORDER BY iv";
				fileLogTelegramWriter.write(fetchSql);
				
				ResultSet rs = stmt.executeQuery(fetchSql);
				
				float lastIvRead = 0f;
				int recCount = 0;
				int fullcount = 0;
				float deltaRangeCEAvgLtp = 0f;
				float deltaRangeCEAvgIv = 0f;
				float deltaRangeCEFullAvgIv = 0f;
				float deltaRangeCEAvgDelta = 0f;
				float deltaRangeCEAvgGamma = 0f;
				float deltaRangeCEFullGamma = 0f;
				float deltaRangeCEAvgVega = 0f;
				float deltaRangeCEWorth = 0f;
				float deltaRangeCEOI = 0f;
				float deltaRangeCEDeltaOI = 0f;
				float deltaRangeCEFullDeltaOI = 0f;
				float deltaRangeCEGammaOI = 0f;
				float deltaRangeCEvolume1min = 0f;
				StringBuffer logBuffer = new StringBuffer();
				float dr49CEAvgIV = 0f;
				int dr49Count = 0;
				float ceDeltaOIWorth = 0f;
				
				float dr16CEAvgIV = 0f;
				int dr16Count = 0;
				while (rs.next()) {	
					float curIv = rs.getFloat("iv");
					float delta = Math.abs(rs.getFloat("delta"));
					float ltp = rs.getFloat("ltp");
					float oi = rs.getFloat("oi");
					float gamma = rs.getFloat("gamma");
					float volume1min = rs.getFloat("volume1min");
					
					if (lastIvRead<0.1f || curIv < lastIvRead +5f) {
						//System.out.println("Include "+curIv);
						logBuffer.append( " {" + curIv+ " D " + delta +" Worth " + (oi*ltp/10000000f) +"} ");
						deltaRangeCEAvgLtp = deltaRangeCEAvgLtp + ltp;
						deltaRangeCEAvgIv = deltaRangeCEAvgIv + curIv;
						ceDeltaOIWorth = ceDeltaOIWorth + oi*delta;	
						deltaRangeCEAvgDelta = deltaRangeCEAvgDelta + delta;
						deltaRangeCEAvgGamma = deltaRangeCEAvgGamma + gamma;
						deltaRangeCEAvgVega = deltaRangeCEAvgVega + rs.getFloat("vega");
						deltaRangeCEWorth = deltaRangeCEWorth + oi*ltp;
						deltaRangeCEOI = deltaRangeCEOI + oi;
						deltaRangeCEDeltaOI = deltaRangeCEDeltaOI + oi*delta;
						deltaRangeCEGammaOI = deltaRangeCEGammaOI + oi*gamma;
						lastIvRead = curIv; 
						recCount++;
					} else {
						logBuffer.append( " [" + curIv+" D " + delta +" Worth " + (oi*ltp/10000000f) +"] ");
					}
					fullcount++;
					deltaRangeCEvolume1min = deltaRangeCEvolume1min + volume1min;
					deltaRangeCEFullAvgIv = deltaRangeCEFullAvgIv + curIv;
					deltaRangeCEFullGamma = deltaRangeCEFullGamma + gamma;
					deltaRangeCEFullDeltaOI = deltaRangeCEFullDeltaOI + rs.getFloat("oi")* Math.abs(rs.getFloat("delta"));
					
					if (delta >= 0.4f && delta <= 0.9f) {
						dr49CEAvgIV = dr49CEAvgIV + curIv;
						dr49Count++;
					}
					if (delta >= 0.1f && delta <= 0.6f) {
						dr16CEAvgIV = dr16CEAvgIV + curIv;
						dr16Count++;
					}
				}
				rs.close();
				//System.out.println("CE recCount "+recCount);
				fileLogTelegramWriter.write("CE IVs " + logBuffer.toString());
				
				int countCETotal = fullcount;
				int countCEOutlier = fullcount - recCount;
				
				float deltaRangeHybridCEAvgIv = 0f;
				float deltaRangeHybridCEAvgGamma = 0f;
				if ((float)recCount/(float)fullcount < 0.65f) {				
					deltaRangeHybridCEAvgIv = deltaRangeCEFullAvgIv/(float)fullcount;
					deltaRangeHybridCEAvgGamma = deltaRangeCEFullGamma/(float)fullcount;
				} else {
					deltaRangeHybridCEAvgIv = deltaRangeCEAvgIv/(float)recCount;
					deltaRangeHybridCEAvgGamma = deltaRangeCEAvgGamma/(float)recCount;
				}
				
				deltaRangeCEAvgLtp = deltaRangeCEAvgLtp/(float)recCount;
				deltaRangeCEAvgIv  = deltaRangeCEAvgIv/(float)recCount;
				deltaRangeCEAvgDelta =  deltaRangeCEAvgDelta/(float)recCount;
				deltaRangeCEAvgGamma = deltaRangeCEAvgGamma/(float)recCount;
				deltaRangeCEAvgVega = deltaRangeCEAvgVega/(float)recCount;
				deltaRangeCEWorth = deltaRangeCEWorth/10000000f; // in Crores
				
				deltaRangeCEFullAvgIv = deltaRangeCEFullAvgIv/(float)fullcount;
				
				retMap.put("deltaRangeCEAvgLtp", deltaRangeCEAvgLtp);
				retMap.put("deltaRangeCEAvgIv", deltaRangeCEAvgIv);
				retMap.put("deltaRangeCEAvgDelta", deltaRangeCEAvgDelta);
				retMap.put("deltaRangeCEAvgGamma", deltaRangeCEAvgGamma);
				retMap.put("deltaRangeCEAvgVega", deltaRangeCEAvgVega);
				retMap.put("deltaRangeCEWorth", deltaRangeCEWorth);
				retMap.put("deltaRangeCEOI", deltaRangeCEOI/10000000f);
				retMap.put("deltaRangeCEDeltaOI", deltaRangeCEDeltaOI/10000000f);
				retMap.put("deltaRangeCEFullDeltaOI", deltaRangeCEFullDeltaOI/10000000f);
				retMap.put("deltaRangeCEGammaOI", deltaRangeCEGammaOI);
				retMap.put("deltaRangeCEFullAvgIv", deltaRangeCEFullAvgIv);
				retMap.put("deltaRangeHybridCEAvgIv",deltaRangeHybridCEAvgIv);
				retMap.put("deltaRangeCEvolume1min", deltaRangeCEvolume1min);
				retMap.put("deltaRangeHybridCEAvgGamma",deltaRangeHybridCEAvgGamma);
				
				retMap.put("deltaRangeCEOutlierRatio", (float)fullcount/(float)recCount);
				
				retMap.put("dr49CEAvgIV",dr49CEAvgIV!=0?dr49CEAvgIV/(float)dr49Count:0);
				retMap.put("dr16CEAvgIV",dr16CEAvgIV!=0?dr16CEAvgIV/(float)dr16Count:0);
				
				retMap.put("countCETotal",(float) countCETotal);
				retMap.put("countCEOutlier",(float) countCEOutlier);
				retMap.put("ceDeltaOIWorth",ceDeltaOIWorth);
				
				
				lowerDelta = -0.9f;
				upperDelta = -0.1f;
				
				fetchSql = "select ltp, oi, iv, delta, vega, gamma, volume1min from nexcorio_option_snapshot where record_date = '" + postgresShortDateFormat.format(getCurrentTime())+ "'"
						+ " AND trading_symbol LIKE '" + optionnamePrefix + "%PE'"
						+ " AND delta >= " + lowerDelta
						+ " AND delta <= " + upperDelta
						+ " ORDER BY iv";
				
				fileLogTelegramWriter.write(fetchSql);
				
				rs = stmt.executeQuery(fetchSql);
				
				lastIvRead = 0f;
				recCount = 0;
				fullcount = 0;
				float deltaRangePEAvgLtp = 0f;
				float deltaRangePEAvgIv = 0f;
				
				float deltaRangePEFullAvgIv = 0f;
				float deltaRangePEFullGamma = 0f;
				float deltaRangePEAvgDelta = 0f;
				float deltaRangePEAvgGamma = 0f;
				float deltaRangePEAvgVega = 0f;
				float deltaRangePEWorth = 0f;
				float deltaRangePEOI = 0f;
				
				float deltaRangePEDeltaOI = 0f;
				float deltaRangePEGammaOI = 0f;
				float deltaRangePEFullDeltaOI = 0f;
				float deltaRangePEvolume1min = 0f;
				StringBuffer logBuffer2 = new StringBuffer();
				
				float dr49PEAvgIV = 0f;
				dr49Count = 0;
				float dr16PEAvgIV = 0f;
				dr16Count = 0;
				float peDeltaOIWorth = 0f;
				while (rs.next()) {	
					float curIv = rs.getFloat("iv");
					float delta = Math.abs(rs.getFloat("delta"));
					float ltp = rs.getFloat("ltp");
					float oi = rs.getFloat("oi");				
					float gamma = rs.getFloat("gamma");
					float volume1min = rs.getFloat("volume1min");
					if (lastIvRead<0.1f || curIv < lastIvRead +5f) {
						//System.out.println("Include "+curIv);
						logBuffer2.append( " {" + curIv+" D " + delta +" Worth " + (oi*ltp/10000000f)+"} ");
						
						deltaRangePEAvgLtp = deltaRangePEAvgLtp + ltp;
						deltaRangePEAvgIv = deltaRangePEAvgIv + curIv;
						peDeltaOIWorth = peDeltaOIWorth + oi*delta;
						deltaRangePEAvgDelta = deltaRangePEAvgDelta + delta;
						deltaRangePEAvgGamma = deltaRangePEAvgGamma + gamma;
						deltaRangePEAvgVega = deltaRangePEAvgVega + rs.getFloat("vega");
						deltaRangePEWorth = deltaRangePEWorth + oi*ltp;
						deltaRangePEOI = deltaRangePEOI + oi;
						deltaRangePEDeltaOI = deltaRangePEDeltaOI + oi*delta;
						deltaRangePEGammaOI = deltaRangePEGammaOI + oi*gamma;
						lastIvRead = curIv; 
						recCount++;
					} else {
						logBuffer.append( " [" + curIv+" D " + delta +" Worth " + (oi*ltp/10000000f) +"] ");
					}
					fullcount++;
					deltaRangePEvolume1min = deltaRangePEvolume1min + volume1min;
					deltaRangePEFullAvgIv = deltaRangePEFullAvgIv + curIv;
					deltaRangePEFullGamma = deltaRangePEFullGamma + gamma;
					deltaRangePEFullDeltaOI = deltaRangePEFullDeltaOI + rs.getFloat("oi")* Math.abs(rs.getFloat("delta"));
					if (delta >= 0.4f && delta <= 0.9f) {
						dr49PEAvgIV = dr49PEAvgIV + curIv;
						dr49Count++;
					}
					if (delta >= 0.1f && delta <= 0.6f) {
						dr16PEAvgIV = dr16PEAvgIV + curIv;
						dr16Count++;
					}
				}
				rs.close();
				//System.out.println("PE recCount "+recCount);
				fileLogTelegramWriter.write("PE IVs " + logBuffer2.toString());
				
				int countPETotal = fullcount;
				int countPEOutlier = fullcount - recCount;
				
				float deltaRangeHybridPEAvgIv = 0f;
				float deltaRangeHybridPEAvgGamma = 0f;
				if ((float)recCount/(float)fullcount < 0.65f) {
					deltaRangeHybridPEAvgIv = deltaRangePEFullAvgIv/(float)fullcount;
					deltaRangeHybridPEAvgGamma = deltaRangePEFullGamma/(float)fullcount;
				} else {
					deltaRangeHybridPEAvgIv = deltaRangePEAvgIv/(float)recCount;
					deltaRangeHybridPEAvgGamma = deltaRangePEAvgGamma/(float)recCount;
				}
				
				deltaRangePEAvgLtp = deltaRangePEAvgLtp/(float)recCount;
				deltaRangePEAvgIv  = deltaRangePEAvgIv/(float)recCount;
				deltaRangePEAvgDelta =  deltaRangePEAvgDelta/(float)recCount;
				deltaRangePEAvgGamma = deltaRangePEAvgGamma/(float)recCount;
				deltaRangePEAvgVega = deltaRangePEAvgVega/(float)recCount;
				deltaRangePEWorth = deltaRangePEWorth/10000000f; // in Crores
				deltaRangePEFullAvgIv = deltaRangePEFullAvgIv/(float)fullcount;
				
				retMap.put("deltaRangePEAvgLtp", deltaRangePEAvgLtp);
				retMap.put("deltaRangePEAvgIv", deltaRangePEAvgIv);
				retMap.put("deltaRangePEAvgDelta", deltaRangePEAvgDelta);
				retMap.put("deltaRangePEAvgGamma", deltaRangePEAvgGamma);
				retMap.put("deltaRangePEAvgVega", deltaRangePEAvgVega);
				retMap.put("deltaRangePEWorth", deltaRangePEWorth);
				retMap.put("deltaRangePEOI", deltaRangePEOI/10000000f);
				retMap.put("deltaRangePEDeltaOI", deltaRangePEDeltaOI/10000000f);
				retMap.put("deltaRangePEFullDeltaOI", deltaRangePEFullDeltaOI/10000000f);
				retMap.put("deltaRangePEGammaOI", deltaRangePEGammaOI);
				retMap.put("deltaRangePEFullAvgIv", deltaRangePEFullAvgIv);
				retMap.put("deltaRangeHybridPEAvgIv", deltaRangeHybridPEAvgIv);
				retMap.put("deltaRangePEvolume1min", deltaRangePEvolume1min);
				retMap.put("deltaRangeHybridPEAvgGamma",deltaRangeHybridPEAvgGamma);
				retMap.put("deltaRangePEOutlierRatio", (float)fullcount/(float)recCount);
				retMap.put("dr49PEAvgIV",dr49PEAvgIV!=0?dr49PEAvgIV/(float)dr49Count:0);
				retMap.put("dr16PEAvgIV",dr16PEAvgIV!=0?dr16PEAvgIV/(float)dr16Count:0);
				
				retMap.put("countPETotal",(float) countPETotal);
				retMap.put("countPEOutlier",(float) countPEOutlier);
				
				retMap.put("peDeltaOIWorth", peDeltaOIWorth);
				
				stmt.close();
			} catch (Exception e) {
				e.printStackTrace();
				log.error("Error"+e.getMessage(),e);
			} finally {
				try {
					conn.close();
				} catch (SQLException e) {
					log.error(e);
				}
			}
		
		return retMap;
	}
	
	private Map<String, OptionGreek> processAndSaveRawStraddleData(float baseDelta, float totalCEOI, float totalPEOI, float totalCEIV, float totalPEIV,
			float totalCEGamma, float totalPEGamma,
			float totalCEVega, float totalPEVega,
			float avgCeGamma, float avgPeGamma,
			float selectiveStrikeAvgCeGamma, float selectiveStrikeAvgPeGamma,
			float selectiveStrikeAvgCeIv, float selectiveStrikeAvgPeIv,
			
			float selective10StrikeAvgCeGamma, float selective10StrikeAvgPeGamma,
			float selective10StrikeAvgCeIv, float selective10StrikeAvgPeIv,
			
			float selective20StrikeAvgCeGamma, float selective20StrikeAvgPeGamma,
			float selective20StrikeAvgCeIv, float selective20StrikeAvgPeIv,
			
			float futuresLtp,
			Map<String, Float> deltaRangeGreeksDetails) {
		Map<String, OptionGreek> retMap = null;
		
		Connection conn = null;
		try {			
			conn = HDataSource.getConnection();
			Statement stmt = conn.createStatement();
			
			String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(baseDelta, 0); // Hedge distance 0
			String ceOptionName = entryStraddleOptionNames[0];
			String peOptionName = entryStraddleOptionNames[1];
			
			OptionGreek ceOptionGreek = getOptionGreeks(ceOptionName);
			OptionGreek peOptionGreek = getOptionGreeks(peOptionName);
			
			if (this.mainInstrument.getInstrumentType().equals("INDEX") && !ceOptionName.substring(0,ceOptionName.length()-2).equals(peOptionName.substring(0,peOptionName.length()-2))) {
				String optionPrefix = ceOptionName.substring(0, ceOptionName.length()-7);
				int optionStrike = Integer.parseInt(ceOptionName.substring(ceOptionName.length()-7,ceOptionName.length()-2));
				
				ceOptionName = optionPrefix + optionStrike + "CE";
				peOptionName = optionPrefix + optionStrike + "PE";
				peOptionGreek = getOptionGreeks(peOptionName);
			}
			
			if (ceOptionGreek!=null && peOptionGreek!=null) {
				String insertSql = "INSERT INTO nexcorio_option_atm_movement_data (id, f_main_instrument, instrumentltp, record_time, ceOptionname, peOptionname"
						+ ", ceDelta"
						+ ", peDelta"
						+ ", ceGamma"
						+ ", peGamma"
						+ ", ceVega"
						+ ", peVega"
						+ ", ceTheta"
						+ ", peTheta"
						+ ", ceIV"
						+ ", peIV"
						+ ", ceLtp"
						+ ", peLtp"
						
						+ ", ceOi"
						+ ", peOi"
						+ ", totalCEOI"
						+ ", totalPEOI"
						+ ", totalCEIV"
						+ ", totalPEIV"					
						+ ", totalCEGamma"
						+ ", totalPEGamma"
						+ ", totalCEVega"
						+ ", totalPEVega"
						
						+ ", avgCEGamma"
						+ ", avgPEGamma"

						+ ", selectiveStrike_AvgCeGamma"
						+ ", selectiveStrike_AvgPeGamma"						
						+ ", selectiveStrike_AvgCeIv"
						+ ", selectiveStrike_AvgPeIv"
						
						+ ", selective10Strike_AvgCeGamma"
						+ ", selective10Strike_AvgPeGamma"						
						+ ", selective10Strike_AvgCeIv"
						+ ", selective10Strike_AvgPeIv"
						
						+ ", selective20Strike_AvgCeGamma"
						+ ", selective20Strike_AvgPeGamma"						
						+ ", selective20Strike_AvgCeIv"
						+ ", selective20Strike_AvgPeIv"

						+ ", futures_Ltp"
						
						+ ", deltaRangeCEAvgLtp" 
						+ ", deltaRangeCEAvgIv" 
						+ ", deltaRangeCEAvgDelta" 
						+ ", deltaRangeCEAvgGamma" 
						+ ", deltaRangeCEAvgVega"
						+ ", deltaRangePEAvgLtp"
						+ ", deltaRangePEAvgIv"
						+ ", deltaRangePEAvgDelta" 
						+ ", deltaRangePEAvgGamma" 
						+ ", deltaRangePEAvgVega"
						+ ", deltaRangeCEWorth"
						+ ", deltaRangePEWorth"
						+ ", deltaRangeCEOI"
						+ ", deltaRangePEOI"
						+ ", deltaRangeCEDeltaOI"
						+ ", deltaRangePEDeltaOI"
						
						+ ", deltaRangeCEFullDeltaOI"
						+ ", deltaRangePEFullDeltaOI"
						
						+ ", deltaRangeCEGammaOI"
						+ ", deltaRangePEGammaOI"
						
						+ ", deltaRangeCEFullAvgIv"
						+ ", deltaRangePEFullAvgIv"
						
						+ ", deltaRangeHybridCEAvgIv"
						+ ", deltaRangeHybridPEAvgIv"
						
						+ ", deltaRangeCEvolume1min"
						+ ", deltaRangePEvolume1min"
						
						+ ", deltaRangeHybridCEAvgGamma"
						+ ", deltaRangeHybridPEAvgGamma"
						+ ", deltaRangeCEOutlierRatio"
						+ ", deltaRangePEOutlierRatio"
						
						+ ", dr4_9CEAvgIv"
						+ ", dr4_9PEAvgIv"
						
						+ ", countCETotal"
						+ ", countCEOutlier"
						
						+ ", countPETotal"
						+ ", countPEOutlier"

						+ ", dr1_6CEAvgIv"
						+ ", dr1_6PEAvgIv"
						
						+ ", ceDeltaOIWorth"
						+ ", peDeltaOIWorth"

						+ ")" 
						+ " VALUES (nextval('nexcorio_option_atm_movement_data_id_seq')," + this.mainInstrument.getId()+ "," + this.instrumentLtp 
						+ ",'" + postgresLongDateFormat.format(getCurrentTime()) + "'"
						+ ",'" + ceOptionName + "'"
						+ ",'" + peOptionName + "'"
						+ " ," + ceOptionGreek.getDelta() 
						+ " ," + peOptionGreek.getDelta()
						
						+ " ," + ceOptionGreek.getGamma() 
						+ " ," + peOptionGreek.getGamma()
						
						+ " ," + ceOptionGreek.getVega() 
						+ " ," + peOptionGreek.getVega()
						
						+ " ," + ceOptionGreek.getTheta() 
						+ " ," + peOptionGreek.getTheta()
						
						+ " ," + ceOptionGreek.getIv() 
						+ " ," + peOptionGreek.getIv()
						
						+ " ," + ceOptionGreek.getLtp() 
						+ " ," + peOptionGreek.getLtp()
						+ " ," + ceOptionGreek.getOi()
						+ " ," + peOptionGreek.getOi()
						+ " ," + totalCEOI
						+ " ," + totalPEOI
						+ " ," + totalCEIV
						+ " ," + totalPEIV
						+ " ," + totalCEGamma
						+ " ," + totalPEGamma
						+ " ," + totalCEVega
						+ " ," + totalPEVega
						+ " ," + avgCeGamma
						+ " ," + avgPeGamma
						+ " ," + selectiveStrikeAvgCeGamma 
						+ " ," + selectiveStrikeAvgPeGamma						
						+ " ," + selectiveStrikeAvgCeIv
						+ " ," + selectiveStrikeAvgPeIv
						
						+ " ," + selective10StrikeAvgCeGamma 
						+ " ," + selective10StrikeAvgPeGamma						
						+ " ," + selective10StrikeAvgCeIv
						+ " ," + selective10StrikeAvgPeIv
						
						+ " ," + selective20StrikeAvgCeGamma 
						+ " ," + selective20StrikeAvgPeGamma						
						+ " ," + selective20StrikeAvgCeIv
						+ " ," + selective20StrikeAvgPeIv
						
						+ " ," + futuresLtp
						
						+ " ," + deltaRangeGreeksDetails.get("deltaRangeCEAvgLtp") 
						+ " ," + deltaRangeGreeksDetails.get("deltaRangeCEAvgIv")
						+ " ," + deltaRangeGreeksDetails.get("deltaRangeCEAvgDelta") 
						+ " ," + deltaRangeGreeksDetails.get("deltaRangeCEAvgGamma") 
						+ " ," + deltaRangeGreeksDetails.get("deltaRangeCEAvgVega")
						+ " ," + deltaRangeGreeksDetails.get("deltaRangePEAvgLtp")
						+ " ," + deltaRangeGreeksDetails.get("deltaRangePEAvgIv")
						+ " ," + deltaRangeGreeksDetails.get("deltaRangePEAvgDelta")
						+ " ," + deltaRangeGreeksDetails.get("deltaRangePEAvgGamma") 
						+ " ," + deltaRangeGreeksDetails.get("deltaRangePEAvgVega")
						
						+ " ," + deltaRangeGreeksDetails.get("deltaRangeCEWorth")
						+ " ," + deltaRangeGreeksDetails.get("deltaRangePEWorth")
						
						+ " ," + deltaRangeGreeksDetails.get("deltaRangeCEOI")
						+ " ," + deltaRangeGreeksDetails.get("deltaRangePEOI")
						
						+ " ," + deltaRangeGreeksDetails.get("deltaRangeCEDeltaOI")
						+ " ," + deltaRangeGreeksDetails.get("deltaRangePEDeltaOI")
						
						+ " ," + deltaRangeGreeksDetails.get("deltaRangeCEFullDeltaOI")
						+ " ," + deltaRangeGreeksDetails.get("deltaRangePEFullDeltaOI")
						
						+ " ," + deltaRangeGreeksDetails.get("deltaRangeCEGammaOI")
						+ " ," + deltaRangeGreeksDetails.get("deltaRangePEGammaOI")
						
						+ " ," + deltaRangeGreeksDetails.get("deltaRangeCEFullAvgIv")
						+ " ," + deltaRangeGreeksDetails.get("deltaRangePEFullAvgIv")
						
						+ " ," + deltaRangeGreeksDetails.get("deltaRangeHybridCEAvgIv")
						+ " ," + deltaRangeGreeksDetails.get("deltaRangeHybridPEAvgIv")
						
						+ " ," + deltaRangeGreeksDetails.get("deltaRangeCEvolume1min")
						+ " ," + deltaRangeGreeksDetails.get("deltaRangePEvolume1min")
						
						+ " ," + deltaRangeGreeksDetails.get("deltaRangeHybridCEAvgGamma")
						+ " ," + deltaRangeGreeksDetails.get("deltaRangeHybridPEAvgGamma")
						
						+ " ," + deltaRangeGreeksDetails.get("deltaRangeCEOutlierRatio")
						+ " ," + deltaRangeGreeksDetails.get("deltaRangePEOutlierRatio")
						
						+ " ," + deltaRangeGreeksDetails.get("dr49CEAvgIV")
						+ " ," + deltaRangeGreeksDetails.get("dr49PEAvgIV")
						
						+ " ," + deltaRangeGreeksDetails.get("countCETotal").intValue()
						+ " ," + deltaRangeGreeksDetails.get("countCEOutlier").intValue()
				
						+ " ," + deltaRangeGreeksDetails.get("countPETotal").intValue()
						+ " ," + deltaRangeGreeksDetails.get("countPEOutlier").intValue()
						
						+ " ," + deltaRangeGreeksDetails.get("dr16CEAvgIV")
						+ " ," + deltaRangeGreeksDetails.get("dr16PEAvgIV")
						
						+ " ," + deltaRangeGreeksDetails.get("ceDeltaOIWorth")
						+ " ," + deltaRangeGreeksDetails.get("peDeltaOIWorth")
						
						+ ")";
				fileLogTelegramWriter.write(insertSql);
				stmt.execute(insertSql);
				
				retMap = new HashMap<>();
				retMap.put("CE", ceOptionGreek);
				retMap.put("PE", peOptionGreek);
			}
			stmt.close();
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error"+e.getMessage(),e);
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				log.error(e);
			}
		}
		return retMap;
	}
	
	private Map<String, Integer> getFutureStandOff() {
		Map<String, Integer> retMap = new HashMap<String, Integer>();
		
		Connection conn = null;
		try {			
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
						
			String fetchSql = "SELECT count(*) as total, COUNT(DISTINCT CASE WHEN total_buy_qty > total_sell_qty THEN id END) as bullishCount,"
					+ " COUNT(DISTINCT CASE WHEN total_buy_qty < total_sell_qty THEN id END) as bearishCount"
					+ " FROM nexcorio_tick_data"
					+ " WHERE f_main_instrument=" + this.mainInstrument.getId()
					+ " AND quote_time <='" + postgresLongDateFormat.format(getCurrentTime()) + "'"
					+ " AND  quote_time > '" + postgresLongDateFormat.format(getCurrentTime(-5)) + "'"
					+ " AND trading_symbol='" + futuresTradingSymbol + "'";
			
			fileLogTelegramWriter.write( "  fetchSql="+fetchSql);
			
			ResultSet rs = stmt.executeQuery(fetchSql);
			
			int totalEntry = 0;
			int bullishEntry = 0;
			
			while (rs.next()) {
				totalEntry = rs.getInt("total");
				bullishEntry = rs.getInt("bullishCount");
			}
			rs.close();
			
			retMap.put("Total", totalEntry);
			retMap.put("Bullish", bullishEntry);
			
			stmt.close();
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error"+e.getMessage(),e);
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				log.error(e);
			}
		}
		return retMap; 
	}
	
	/**
	 * Refer: https://www.investopedia.com/terms/p/putcallparity.asp
	 * 
	 * @param mainInstrumentId
	 * @param strikePrice
	 * @param instrumentLtp
	 * @return
	 */
	private static float getPutCallParity(Long mainInstrumentId, int strikePrice, float instrumentLtp) {
		float retVal = 0f;
		Connection conn = null;
		try {			
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			// ?????? What is the future value of weekly expiry
			
			stmt.close();
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error"+e.getMessage(),e);
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				log.error(e);
			}
		}
		return retVal;
	}
	
	private List<OptionGreek> getSnapshotGreeks() {
		List<OptionGreek> retList = new ArrayList<OptionGreek>();
		ConcurrentMap<String, OptionGreek> caffeineObjects =KiteCache.optionGreekCache.asMap();
		
		Iterator<String> iter = caffeineObjects.keySet().iterator();
		while(iter.hasNext()) {
			String keyStr = iter.next();
			
			if (keyStr.startsWith(this.mainInstrument.getShortName())) {
				retList.add(caffeineObjects.get(keyStr));
			}
		}
		return retList;
		
	}
	
	public static void main(String[] args) {
		new ATMMovementAnalyzerThreadAlgoThread("NIFTY", "2025-07-30 09:17:00");
		
		
	}

}
