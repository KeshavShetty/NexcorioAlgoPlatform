package com.nexcorio.algo.analytics;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.kite.KiteCache;
import com.nexcorio.algo.util.FileLogTelegramWriter;
import com.nexcorio.algo.util.KiteUtil;
import com.nexcorio.algo.util.db.HDataSource;

/**
 * Top N Gamma exposure included
 */
class SortbyIV implements Comparator<OptionGreek> 
{ 
    // Comparator 
    public int compare(OptionGreek a, OptionGreek b) 
    { 
    	if (a.getIv() < b.getIv()) return -1;
    	else if (a.getIv() > b.getIv()) return 1;
    	else return 0;
    } 
}

class SortbyOiDesc implements Comparator<OptionGreek> 
{ 
    // Comparator 
    public int compare(OptionGreek a, OptionGreek b) 
    { 
    	if (a.getOi() > b.getOi()) return -1;
    	else if (a.getOi() < b.getOi()) return 1;
    	else return 0;
    } 
}

// For cumulative Iv diff
class SortbyStrike implements Comparator<OptionGreek> 
{ 
    // Comparator 
    public int compare(OptionGreek a, OptionGreek b) 
    { 
    	if (a.getStrike() < b.getStrike()) return -1;
    	else if (a.getStrike() > b.getStrike()) return 1;
    	else return 0;
    } 
}

public class ATMMovementAnalyzerThreadAlgoThread extends AnalyticsBaseClass implements Runnable {

	private static final Logger log = LogManager.getLogger(ATMMovementAnalyzerThreadAlgoThread.class);
	
	String futuresTradingSymbol;
	
	float prev_last_traded_price = -1f;
	long prev_volume_traded_today = 0l;
	long lastProcessedId = 0l;
	float outstandingVolume = 0f;
	
	private List<OptionGreek> prevCeOptionGreeks = new ArrayList<OptionGreek>();
	private List<OptionGreek> prevPeOptionGreeks = new ArrayList<OptionGreek>();
	
	private float accumulatedChangein5secCeIV = 0f;
	private float accumulatedChangein5secPeIV = 0f;
	
	private float drResAccumulatedChangein5secCeIV = 0f;
	private float drResAccumulatedChangein5secPeIV = 0f;
	
	private float dr49AccumulatedChangein5secCeGamma = 0f;
	private float dr49AccumulatedChangein5secPeGamma = 0f;
	
	private float otm250x750AccmlCeTheta = 0f;
	private float otm250x750AccmlPeTheta = 0f;
	
	private float dr49AccumulatedChangein5secCeDelta = 0f;
	private float dr49AccumulatedChangein5secPeDelta = 0f;
	
	private float dr49AccumulatedChangein5secCeVega = 0f;
	private float dr49AccumulatedChangein5secPeVega = 0f;
	
	private float dr49AccumulatedChangein5secCeTheta = 0f;
	private float dr49AccumulatedChangein5secPeTheta = 0f;
	
	private float dr49AccumulatedChangein5secCeIv = 0f;
	private float dr49AccumulatedChangein5secPeIv = 0f;
	
	private float dr49AccumulatedChangein5secCeLtp = 0f;
	private float dr49AccumulatedChangein5secPeLtp = 0f;
	
	private float dr16AccumulatedChangein5secCeGamma = 0f;
	private float dr16AccumulatedChangein5secPeGamma = 0f;
	
	private float dr16AccumulatedChangein5secCeDelta = 0f;
	private float dr16AccumulatedChangein5secPeDelta = 0f;
	
	private float dr16AccumulatedChangein5secCeVega = 0f;
	private float dr16AccumulatedChangein5secPeVega = 0f;
	
	private float dr16AccumulatedChangein5secCeTheta = 0f;
	private float dr16AccumulatedChangein5secPeTheta = 0f;
	
	private float dr16AccumulatedChangein5secCeIv = 0f;
	private float dr16AccumulatedChangein5secPeIv = 0f;
	
	private float dr16AccumulatedChangein5secCeLtp = 0f;
	private float dr16AccumulatedChangein5secPeLtp = 0f;
	
	private float drWhlStrkAccumulatedChangein5secCeGamma = 0f;
	private float drWhlStrkAccumulatedChangein5secPeGamma = 0f;
	
	private float drWhlStrkAccumulatedChangein5secCeDelta = 0f;
	private float drWhlStrkAccumulatedChangein5secPeDelta = 0f;
	
	private float drWhlStrkAccumulatedChangein5secCeVega = 0f;
	private float drWhlStrkAccumulatedChangein5secPeVega = 0f;
	
	private float drWhlStrkAccumulatedChangein5secCeTheta = 0f;
	private float drWhlStrkAccumulatedChangein5secPeTheta = 0f;
	
	private float drWhlStrkAccumulatedChangein5secCeIv = 0f;
	private float drWhlStrkAccumulatedChangein5secPeIv = 0f;
	
	private float drWhlStrkAccumulatedChangein5secCeLtp = 0f;
	private float drWhlStrkAccumulatedChangein5secPeLtp = 0f;
	
	private float pt200AccumulatedChangein5secCeTheta = 0f;
	private float pt200AccumulatedChangein5secPeTheta = 0f;
	
	private float altAbove5WhlStrkAccmltCETheta = 0f;
	private float altAbove5WhlStrkAccmltPETheta = 0f;
	
	private float dr19fixedSizeAccmlCETheta = 0f;
	private float dr19fixedSizeAccmlPETheta = 0f;
	
	private float otm250_750AccmlCeTheta = 0f;
	private float otm250_750AccmlPeTheta = 0f;
	
	private float otm200_400AccmlCeTheta = 0f;
	private float otm200_400AccmlPeTheta = 0f;
	
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
			List<OptionGreek> greekList = getSnapshotGreeksFromCache();
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
			List<OptionGreek> greekList = getSnapshotGreeksFromCache();
			
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
				
				List<OptionGreek> ceOptionGreeks = new ArrayList<OptionGreek>();
				List<OptionGreek> peOptionGreeks = new ArrayList<OptionGreek>();
				
				if (this.backtestDate == null) { // Live
					for(OptionGreek aGreek: getSnapshotGreeksFromCache()) {
						if (aGreek.getTradingSymbol().endsWith("CE")) {
							ceOptionGreeks.add(aGreek);
						} else { // PE
							peOptionGreeks.add(aGreek);
						}
					}
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
							if (optionname.endsWith("CE")) {
								ceOptionGreeks.add(aGreek);
							} else {
								peOptionGreeks.add(aGreek);
							}
						}
					}
				}				
				Collections.sort(ceOptionGreeks, new SortbyIV());
				Collections.sort(peOptionGreeks, new SortbyIV());
				
				Map<String, OptionGreek> prevCeOptionGreeksMap = prevCeOptionGreeks.stream().collect(Collectors.toMap(OptionGreek::getTradingSymbol, item -> item));
				Map<String, OptionGreek> prevPeOptionGreeksMap = prevPeOptionGreeks.stream().collect(Collectors.toMap(OptionGreek::getTradingSymbol, item -> item));
				
				// 1-9 Delta Fixed size 
				List<OptionGreek> selectedCEGreeks = new ArrayList<OptionGreek>();
				float lastIvRead = 0f;
				
				for(OptionGreek aGreek: ceOptionGreeks) {
					float delta = Math.abs(aGreek.getDelta());
					
					if (delta >= 0.1f && delta <= 0.9f) {				
						float curIv = aGreek.getIv();				
						if (lastIvRead<0.1f || curIv < lastIvRead + 5f) {
							selectedCEGreeks.add(0,aGreek);
							//lastIvRead = curIv;
						}
					}
				}
				List<OptionGreek> selectedPEGreeks = new ArrayList<OptionGreek>();
				lastIvRead = 0f;
				for(OptionGreek aGreek: peOptionGreeks) {
					float delta = Math.abs(aGreek.getDelta());
					
					if (delta >= 0.1f && delta <= 0.9f) {				
						float curIv = aGreek.getIv();				
						if (lastIvRead<0.1f || curIv < lastIvRead + 5f) {
							selectedPEGreeks.add(0,aGreek);
							//lastIvRead = curIv;
						}
					}
				}
				while(selectedCEGreeks.size() != selectedPEGreeks.size()) {
					if (selectedCEGreeks.size() > selectedPEGreeks.size() ) {
						selectedCEGreeks.remove(0);
					} else {
						selectedPEGreeks.remove(0);
					}
				}
				retMap.put("dr19fixedSizeCEAvgIV", (float) selectedCEGreeks.stream().mapToDouble(d -> d.getIv()).average().orElse(0.0));
				retMap.put("dr19fixedSizePEAvgIV", (float) selectedPEGreeks.stream().mapToDouble(d -> d.getIv()).average().orElse(0.0));
				
				
				float changeInCETheta = 0f;
				float changeInPETheta = 0f;
				
				for(OptionGreek aGreek : selectedCEGreeks) {
					changeInCETheta = changeInCETheta +  (prevCeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null?aGreek.getTheta()-prevCeOptionGreeksMap.get(aGreek.getTradingSymbol()).getTheta():0f);
				}
				for(OptionGreek aGreek : selectedPEGreeks) {
					changeInPETheta = changeInPETheta +  (prevPeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null?aGreek.getTheta()-prevPeOptionGreeksMap.get(aGreek.getTradingSymbol()).getTheta():0f);
				}
				dr19fixedSizeAccmlCETheta = dr19fixedSizeAccmlCETheta + changeInCETheta;
				dr19fixedSizeAccmlPETheta = dr19fixedSizeAccmlPETheta + changeInPETheta;
				
				retMap.put("dr19fixedSizeAccmlCETheta", dr19fixedSizeAccmlCETheta);
				retMap.put("dr19fixedSizeAccmlPETheta", dr19fixedSizeAccmlPETheta);
				
				// ITM till 0.9 delta  Avg IV of same number of strikes
				List<OptionGreek> drITMWhlStrkCEIvs = new ArrayList<OptionGreek>();
				List<OptionGreek> drITMWhlStrkPEIvs = new ArrayList<OptionGreek>();
				
				List<OptionGreek> above5WhlStrkCEIvs = new ArrayList<OptionGreek>();
				List<OptionGreek> above5WhlStrkPEIvs = new ArrayList<OptionGreek>();
				
				for(OptionGreek aGreek: ceOptionGreeks) {
					float delta = Math.abs(aGreek.getDelta());
					
					if (aGreek.getStrike()%100==0) {
						drITMWhlStrkCEIvs.add(aGreek);
						if (delta > 0.5f) above5WhlStrkCEIvs.add(aGreek);
					}
				}
				for(OptionGreek aGreek: peOptionGreeks) {
					float delta = Math.abs(aGreek.getDelta());
					
					if (aGreek.getStrike()%100==0) {
						drITMWhlStrkPEIvs.add(aGreek);
						if (delta > 0.5f) above5WhlStrkPEIvs.add(aGreek);
					}
				}
				int optimalLength =  drITMWhlStrkCEIvs.size() < drITMWhlStrkPEIvs.size() ?	drITMWhlStrkCEIvs.size() : drITMWhlStrkPEIvs.size();	
				
				int above5OptimalLength =  above5WhlStrkCEIvs.size() < above5WhlStrkPEIvs.size() ?	above5WhlStrkCEIvs.size() : above5WhlStrkPEIvs.size();
				
				float drITMWhlStrkCEAvgIv = 0f;
				float drITMWhlStrkPEAvgIv = 0f;
				
				for(int i=0;i<optimalLength;i++) {
					drITMWhlStrkCEAvgIv = drITMWhlStrkCEAvgIv + drITMWhlStrkCEIvs.get(i).getIv();
					drITMWhlStrkPEAvgIv = drITMWhlStrkPEAvgIv + drITMWhlStrkPEIvs.get(i).getIv();
				}
				
				float above5WhlStrkCEAvgIv = 0f;
				float above5WhlStrkPEAvgIv = 0f;
				for(int i=0;i<above5OptimalLength;i++) {
					above5WhlStrkCEAvgIv = above5WhlStrkCEAvgIv + above5WhlStrkCEIvs.get(i).getIv();
					above5WhlStrkPEAvgIv = above5WhlStrkPEAvgIv + above5WhlStrkPEIvs.get(i).getIv();		
				}
				
				drITMWhlStrkCEAvgIv = drITMWhlStrkCEAvgIv/optimalLength;
				drITMWhlStrkPEAvgIv = drITMWhlStrkPEAvgIv/optimalLength;
				
				above5WhlStrkCEAvgIv = above5WhlStrkCEAvgIv/above5OptimalLength;
				above5WhlStrkPEAvgIv = above5WhlStrkPEAvgIv/above5OptimalLength;
				
				retMap.put("drITMWhlStrkSameSizeCEAvgIv", drITMWhlStrkCEAvgIv);
				retMap.put("drITMWhlStrkSameSizePEAvgIv", drITMWhlStrkPEAvgIv);
				
				retMap.put("above5WhlStrkCEAvgIv", above5WhlStrkCEAvgIv);
				retMap.put("above5WhlStrkPEAvgIv", above5WhlStrkPEAvgIv);
				
				
				float altAbove5WhlStrkCEAvgTimevalue = 0f;
				float altAbove5WhlStrkPEAvgTimevalue = 0f;
				
				int lowerLength = above5WhlStrkCEIvs.size() < above5WhlStrkPEIvs.size()?above5WhlStrkCEIvs.size():above5WhlStrkPEIvs.size();

				for(int i=0;i<lowerLength;i++) {
					altAbove5WhlStrkCEAvgTimevalue = altAbove5WhlStrkCEAvgTimevalue + above5WhlStrkCEIvs.get(i).getTimevalue();
					altAbove5WhlStrkPEAvgTimevalue = altAbove5WhlStrkPEAvgTimevalue + above5WhlStrkPEIvs.get(i).getTimevalue();
				}
				if (lowerLength>0) {
					altAbove5WhlStrkCEAvgTimevalue = altAbove5WhlStrkCEAvgTimevalue/lowerLength;
					altAbove5WhlStrkPEAvgTimevalue = altAbove5WhlStrkPEAvgTimevalue/lowerLength;
				}
				retMap.put("altAbove5WhlStrkCEAvgTimevalue", altAbove5WhlStrkCEAvgTimevalue);
				retMap.put("altAbove5WhlStrkPEAvgTimevalue", altAbove5WhlStrkPEAvgTimevalue);
				
				float altAbove5WhlStrkCEAvgIv = 0f;
				float altAbove5WhlStrkPEAvgIv = 0f;
				
				while(above5WhlStrkCEIvs.size() != above5WhlStrkPEIvs.size()) {
					if (above5WhlStrkCEIvs.size() > above5WhlStrkPEIvs.size() ) {
						above5WhlStrkCEIvs.remove(0);
					} else {
						above5WhlStrkPEIvs.remove(0);
					}
				}
				
				float altAbove5WhlStrk5secCETheta = 0f;
				float altAbove5WhlStrk5secPETheta = 0f;
				for(int i=0;i<above5WhlStrkPEIvs.size();i++) {
					altAbove5WhlStrkCEAvgIv = altAbove5WhlStrkCEAvgIv + above5WhlStrkCEIvs.get(i).getIv();
					altAbove5WhlStrkPEAvgIv = altAbove5WhlStrkPEAvgIv + above5WhlStrkPEIvs.get(i).getIv();
					
					for(OptionGreek prevGreek: prevCeOptionGreeks) {
						if (above5WhlStrkCEIvs.get(i).getTradingSymbol().equals(prevGreek.getTradingSymbol())) {
							altAbove5WhlStrk5secCETheta = altAbove5WhlStrk5secCETheta + (Math.abs(above5WhlStrkCEIvs.get(i).getTheta()) - Math.abs(prevGreek.getTheta()));
						}
					}
					for(OptionGreek prevGreek: prevPeOptionGreeks) {
						if (above5WhlStrkPEIvs.get(i).getTradingSymbol().equals(prevGreek.getTradingSymbol())) {
							altAbove5WhlStrk5secPETheta = altAbove5WhlStrk5secPETheta + (Math.abs(above5WhlStrkPEIvs.get(i).getTheta()) - Math.abs(prevGreek.getTheta()));
						}
					}
				}
				altAbove5WhlStrkCEAvgIv = altAbove5WhlStrkCEAvgIv/(float)above5WhlStrkPEIvs.size();
				altAbove5WhlStrkPEAvgIv = altAbove5WhlStrkPEAvgIv/(float)above5WhlStrkPEIvs.size();
				retMap.put("altAbove5WhlStrkCEAvgIv", altAbove5WhlStrkCEAvgIv);
				retMap.put("altAbove5WhlStrkPEAvgIv", altAbove5WhlStrkPEAvgIv);
				
				altAbove5WhlStrkAccmltCETheta = altAbove5WhlStrkAccmltCETheta + altAbove5WhlStrk5secCETheta;
				altAbove5WhlStrkAccmltPETheta = altAbove5WhlStrkAccmltPETheta + altAbove5WhlStrk5secPETheta;
				
				retMap.put("altAbove5WhlStrkAccmltCETheta", altAbove5WhlStrkAccmltCETheta);
				retMap.put("altAbove5WhlStrkAccmltPETheta", altAbove5WhlStrkAccmltPETheta);
				
				List<OptionGreek> range350CEGreeks = new ArrayList<OptionGreek>();
				List<OptionGreek> range350PEGreeks = new ArrayList<OptionGreek>();
				
				List<OptionGreek> range700CEGreeks = new ArrayList<OptionGreek>();
				List<OptionGreek> range700PEGreeks = new ArrayList<OptionGreek>();
				
				float otm250_750Changein5secCeTheta = 0f;
				float otm250_750Changein5secPeTheta = 0f;
				
				float otm200_400Changein5secCeTheta = 0f;
				float otm200_400Changein5secPeTheta = 0f;
				
				for(OptionGreek aGreek: ceOptionGreeks) {
					if (aGreek.getStrike() - instrumentLtp < 350 && aGreek.getStrike() - instrumentLtp > -350) {
						range350CEGreeks.add(aGreek);
					}
					if (aGreek.getStrike() - instrumentLtp > 350 && aGreek.getStrike() - instrumentLtp < 700) {
						range700CEGreeks.add(aGreek);
					}
					if (aGreek.getStrike() > this.instrumentLtp + 250 && aGreek.getStrike() < this.instrumentLtp + 750) {
						if (prevCeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null) otm250_750Changein5secCeTheta = otm250_750Changein5secCeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevCeOptionGreeksMap.get(aGreek.getTradingSymbol()).getTheta()));
					}
					if (aGreek.getStrike() - this.instrumentLtp > 200 && aGreek.getStrike() - this.instrumentLtp < 400) {
						if (prevCeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null) {
							otm200_400Changein5secCeTheta = otm200_400Changein5secCeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevCeOptionGreeksMap.get(aGreek.getTradingSymbol()).getTheta()));
						}
					}
				}
				for(OptionGreek aGreek: peOptionGreeks) {
					if (aGreek.getStrike() - instrumentLtp < 350 && aGreek.getStrike() - instrumentLtp > -350) {
						range350PEGreeks.add(aGreek);
					}
					if (aGreek.getStrike() - instrumentLtp < -350 && aGreek.getStrike() - instrumentLtp > -700) {
						range700PEGreeks.add(aGreek);
					}
					if (aGreek.getStrike() < this.instrumentLtp - 250 && aGreek.getStrike() > this.instrumentLtp - 750 ) {
						if (prevPeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null) otm250_750Changein5secPeTheta = otm250_750Changein5secPeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevPeOptionGreeksMap.get(aGreek.getTradingSymbol()).getTheta()));
					}
					if (aGreek.getStrike() - this.instrumentLtp < -200 && aGreek.getStrike() - this.instrumentLtp > -400) {
						if (prevPeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null) {
							otm200_400Changein5secPeTheta = otm200_400Changein5secPeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevPeOptionGreeksMap.get(aGreek.getTradingSymbol()).getTheta()));
						}
					}
				}
				otm250_750AccmlCeTheta = otm250_750AccmlCeTheta + otm250_750Changein5secCeTheta;
				otm250_750AccmlPeTheta = otm250_750AccmlPeTheta + otm250_750Changein5secPeTheta;
				
				otm200_400AccmlCeTheta = otm200_400AccmlCeTheta + otm200_400Changein5secCeTheta;
				otm200_400AccmlPeTheta = otm200_400AccmlPeTheta + otm200_400Changein5secPeTheta;
				
				retMap.put("otm250_750AccmlCeTheta", otm250_750AccmlCeTheta);
				retMap.put("otm250_750AccmlPeTheta", otm250_750AccmlPeTheta);
				
				retMap.put("otm200_400AccmlCeTheta", otm200_400AccmlCeTheta);
				retMap.put("otm200_400AccmlPeTheta", otm200_400AccmlPeTheta);
				
				retMap.put("range350CEAvgGamma", 100f* (float) range350CEGreeks.stream().mapToDouble(d -> d.getGamma()).average().orElse(0.0));
				retMap.put("range350PEAvgGamma", 100f* (float) range350PEGreeks.stream().mapToDouble(d -> d.getGamma()).average().orElse(0.0));
				
				retMap.put("range350CEAvgIv", (float) range350CEGreeks.stream().mapToDouble(d -> d.getIv()).average().orElse(0.0));
				retMap.put("range350PEAvgIv", (float) range350PEGreeks.stream().mapToDouble(d -> d.getIv()).average().orElse(0.0));
				
				retMap.put("range350CEAvgVega", (float) range350CEGreeks.stream().mapToDouble(d -> d.getVega()).average().orElse(0.0));
				retMap.put("range350PEAvgVega", (float) range350PEGreeks.stream().mapToDouble(d -> d.getVega()).average().orElse(0.0));
				
				retMap.put("range350CEAvgTheta", (float) range350CEGreeks.stream().mapToDouble(d -> d.getTheta()).average().orElse(0.0)/365f);
				retMap.put("range350PEAvgTheta", (float) range350PEGreeks.stream().mapToDouble(d -> d.getTheta()).average().orElse(0.0)/365f);
				
				
				retMap.put("range700CEAvgGamma", 100f* (float) range700CEGreeks.stream().mapToDouble(d -> d.getGamma()).average().orElse(0.0));
				retMap.put("range700PEAvgGamma", 100f* (float) range700PEGreeks.stream().mapToDouble(d -> d.getGamma()).average().orElse(0.0));
				
				retMap.put("range700CEAvgIv", (float) range700CEGreeks.stream().mapToDouble(d -> d.getIv()).average().orElse(0.0));
				retMap.put("range700PEAvgIv", (float) range700PEGreeks.stream().mapToDouble(d -> d.getIv()).average().orElse(0.0));
				
				retMap.put("range700CEAvgVega", (float) range700CEGreeks.stream().mapToDouble(d -> d.getVega()).average().orElse(0.0));
				retMap.put("range700PEAvgVega", (float) range700PEGreeks.stream().mapToDouble(d -> d.getVega()).average().orElse(0.0));
				
				retMap.put("range700CEAvgTheta", (float) range700CEGreeks.stream().mapToDouble(d -> d.getTheta()).average().orElse(0.0)/365f);
				retMap.put("range700PEAvgTheta", (float) range700PEGreeks.stream().mapToDouble(d -> d.getTheta()).average().orElse(0.0)/365f);
				
				// otm250x750 Accml Theta & itm1000x500 Avg IV
				
				float otm250x750Changein5secCeTheta = 0f;
				float otm250x750Changein5secPeTheta = 0f;
				
				List<OptionGreek> upperOtm300x600CEGreeks = new ArrayList<OptionGreek>();
				List<OptionGreek> upperOtm300x600PEGreeks = new ArrayList<OptionGreek>();
				
				List<OptionGreek> lowerOtm0x300CEGreeks = new ArrayList<OptionGreek>();
				List<OptionGreek> lowerOtm0x300PEGreeks = new ArrayList<OptionGreek>();
				
				List<OptionGreek> fullOtm0x600CEGreeks = new ArrayList<OptionGreek>();
				List<OptionGreek> fullOtm0x600PEGreeks = new ArrayList<OptionGreek>();
				
				List<OptionGreek> upperOtm150x300CEGreeks = new ArrayList<OptionGreek>();
				List<OptionGreek> upperOtm150x300PEGreeks = new ArrayList<OptionGreek>();
				
				List<OptionGreek> itm1000x500CeIvs = new ArrayList<OptionGreek>();
				List<OptionGreek> itm1000x500PeIvs = new ArrayList<OptionGreek>();
				
				List<OptionGreek> otm0x400CEGreeks = new ArrayList<OptionGreek>();
				List<OptionGreek> otm400x800CEGreeks = new ArrayList<OptionGreek>();
				
				List<OptionGreek> otm0x400PEGreeks = new ArrayList<OptionGreek>();
				List<OptionGreek> otm400x800PEGreeks = new ArrayList<OptionGreek>();
				
				List<OptionGreek> fullOtm50x400CEGreeks = new ArrayList<OptionGreek>();
				List<OptionGreek> fullOtm50x400PEGreeks = new ArrayList<OptionGreek>();
				
				List<OptionGreek> fullOtm0x500CEGreeks = new ArrayList<OptionGreek>();
				List<OptionGreek> fullOtm0x500PEGreeks = new ArrayList<OptionGreek>();
				
				for(OptionGreek aGreek: ceOptionGreeks) {
					if (aGreek.getStrike()%100==0) {
						if (aGreek.getStrike() > this.instrumentLtp + 250 && aGreek.getStrike() < this.instrumentLtp + 750) {
							if (prevCeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null) otm250x750Changein5secCeTheta = otm250x750Changein5secCeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevCeOptionGreeksMap.get(aGreek.getTradingSymbol()).getTheta()));
							
						}
						if (aGreek.getStrike() > this.instrumentLtp - 1000 && aGreek.getStrike() < this.instrumentLtp - 500 ) {					
							if (prevCeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null) itm1000x500CeIvs.add(aGreek);
						}
					}
					if (aGreek.getStrike() - this.instrumentLtp > 0 && aGreek.getStrike() - this.instrumentLtp < 300) {
						lowerOtm0x300CEGreeks.add(aGreek);
						fullOtm0x600CEGreeks.add(aGreek);
					}
					if (aGreek.getStrike() - this.instrumentLtp > 300 && aGreek.getStrike() - this.instrumentLtp < 600) {
						upperOtm300x600CEGreeks.add(aGreek);
						fullOtm0x600CEGreeks.add(aGreek);
					}
					if (aGreek.getStrike() - this.instrumentLtp > 500 && aGreek.getStrike() - this.instrumentLtp < 750) {
						upperOtm150x300CEGreeks.add(aGreek);
					}
					
					if (aGreek.getStrike() - this.instrumentLtp > 0 && aGreek.getStrike() - this.instrumentLtp < 400 ) {
						otm0x400CEGreeks.add(aGreek);
					}
					if (aGreek.getStrike() - this.instrumentLtp > 400 && aGreek.getStrike() - this.instrumentLtp < 800 ) {
						otm400x800CEGreeks.add(aGreek);
					}
					if (aGreek.getStrike() - this.instrumentLtp > 0 && aGreek.getStrike() - this.instrumentLtp < 500) {
						fullOtm0x500CEGreeks.add(aGreek);
					}
					if (aGreek.getStrike() - this.instrumentLtp > -50 && aGreek.getStrike() - this.instrumentLtp < 400) {
						fullOtm50x400CEGreeks.add(aGreek);
					}
				}
				for(OptionGreek aGreek: peOptionGreeks) {
					if (aGreek.getStrike()%100==0) {
						if (aGreek.getStrike() < this.instrumentLtp - 250 && aGreek.getStrike() > this.instrumentLtp - 750 ) {
							if (prevPeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null) otm250x750Changein5secPeTheta = otm250x750Changein5secPeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevPeOptionGreeksMap.get(aGreek.getTradingSymbol()).getTheta()));
						}
						if (aGreek.getStrike() < this.instrumentLtp + 1000 && aGreek.getStrike() > this.instrumentLtp + 500 ) {
							if (prevPeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null) itm1000x500PeIvs.add(aGreek);
						}
					}
					if (aGreek.getStrike() - this.instrumentLtp < 0 && aGreek.getStrike() - this.instrumentLtp > -300) {
						lowerOtm0x300PEGreeks.add(aGreek);
						fullOtm0x600PEGreeks.add(aGreek);
					}
					if (aGreek.getStrike() - this.instrumentLtp < -300 && aGreek.getStrike() - this.instrumentLtp > -600) {
						upperOtm300x600PEGreeks.add(aGreek);
						fullOtm0x600PEGreeks.add(aGreek);
					}
					if (aGreek.getStrike() - this.instrumentLtp < -500 && aGreek.getStrike() - this.instrumentLtp > -750) {
						upperOtm150x300PEGreeks.add(aGreek);
					}
					if (aGreek.getStrike() - this.instrumentLtp < 0 && aGreek.getStrike() - this.instrumentLtp > -400 ) {
						otm0x400PEGreeks.add(aGreek);
					}
					if (aGreek.getStrike() - this.instrumentLtp < -400 && aGreek.getStrike() - this.instrumentLtp > -800 ) {
						otm400x800PEGreeks.add(aGreek);
					}
					if (aGreek.getStrike() - this.instrumentLtp < 0 && aGreek.getStrike() - this.instrumentLtp > -500) {
						fullOtm0x500PEGreeks.add(aGreek);
					}
					if (aGreek.getStrike() - this.instrumentLtp < 50 && aGreek.getStrike() - this.instrumentLtp > -400) {
						fullOtm50x400PEGreeks.add(aGreek);
					}
				}
				
				otm250x750AccmlCeTheta = otm250x750AccmlCeTheta + otm250x750Changein5secCeTheta;
				otm250x750AccmlPeTheta = otm250x750AccmlPeTheta + otm250x750Changein5secPeTheta;
				
				retMap.put("otm250x750AccmlCeTheta", otm250x750AccmlCeTheta);
				retMap.put("otm250x750AccmlPeTheta", otm250x750AccmlPeTheta);
				
				retMap.put("itm1000x500AvgCeIv", (float) itm1000x500CeIvs.stream().mapToDouble(d -> d.getIv()).average().orElse(0.0));
				retMap.put("itm1000x500AvgPeIv", (float) itm1000x500PeIvs.stream().mapToDouble(d -> d.getIv()).average().orElse(0.0));
				
				retMap.put("fullOtm0x600CEGreeks", (float) fullOtm0x600CEGreeks.stream().mapToDouble(d -> d.getOi()).sum());
				retMap.put("fullOtm0x600PEGreeks", (float) fullOtm0x600PEGreeks.stream().mapToDouble(d -> d.getOi()).sum());
				
				retMap.put("lowerOtm0x300CEGreeks", (float) lowerOtm0x300CEGreeks.stream().mapToDouble(d -> d.getOi()).sum());
				retMap.put("lowerOtm0x300PEGreeks", (float) lowerOtm0x300PEGreeks.stream().mapToDouble(d -> d.getOi()).sum());
				
				retMap.put("upperOtm300x600CEGreeks", (float) upperOtm300x600CEGreeks.stream().mapToDouble(d -> d.getOi()).sum());
				retMap.put("upperOtm300x600PEGreeks", (float) upperOtm300x600PEGreeks.stream().mapToDouble(d -> d.getOi()).sum());
				
				retMap.put("upperOtm150x300CEGreeks", (float) upperOtm150x300CEGreeks.stream().mapToDouble(d -> d.getOi()).sum());
				retMap.put("upperOtm150x300PEGreeks", (float) upperOtm150x300PEGreeks.stream().mapToDouble(d -> d.getOi()).sum());
				
				retMap.put("otm0x400CEGreeks", (float) otm0x400CEGreeks.stream().mapToDouble(d -> d.getOi()).sum());
				retMap.put("otm400x800CEGreeks", (float) otm400x800CEGreeks.stream().mapToDouble(d -> d.getOi()).sum());
				
				retMap.put("otm0x400PEGreeks", (float) otm0x400PEGreeks.stream().mapToDouble(d -> d.getOi()).sum());
				retMap.put("otm400x800PEGreeks", (float) otm400x800PEGreeks.stream().mapToDouble(d -> d.getOi()).sum());
				
				retMap.put("fullOtm0x500CEGreeks", (float) fullOtm0x500CEGreeks.stream().mapToDouble(d -> d.getOi()).sum());
				retMap.put("fullOtm0x500PEGreeks", (float) fullOtm0x500PEGreeks.stream().mapToDouble(d -> d.getOi()).sum());

				retMap.put("fullOtm50x400CEGreeks", (float) fullOtm50x400CEGreeks.stream().mapToDouble(d -> d.getOi()).sum());
				retMap.put("fullOtm50x400PEGreeks", (float) fullOtm50x400PEGreeks.stream().mapToDouble(d -> d.getOi()).sum());
				
				List<OptionGreek> lowerStrikeCEGreeks = new ArrayList<OptionGreek>();
				List<OptionGreek> lowerStrikePEGreeks = new ArrayList<OptionGreek>();
				
				for(OptionGreek aGreek: ceOptionGreeks) {
					float delta = Math.abs(aGreek.getDelta());
					if (delta > 0.5f && delta < 0.8f && this.instrumentLtp - aGreek.getStrike() < 350) {
						lowerStrikeCEGreeks.add(aGreek);
						for(OptionGreek peGreek: peOptionGreeks) {
							if (peGreek.getStrike() == aGreek.getStrike()) {
								lowerStrikePEGreeks.add(peGreek);
							}
						}
					}
				}
				
				retMap.put("lowerStrikeCEAvgIv", (float) lowerStrikeCEGreeks.stream().mapToDouble(d -> d.getIv()).average().orElse(0.0));
				retMap.put("lowerStrikePEAvgIv", (float) lowerStrikePEGreeks.stream().mapToDouble(d -> d.getIv()).average().orElse(0.0));
				
				float lowerDelta = 0.1f;
				float upperDelta = 0.9f;
				
				lastIvRead = 0f;
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
				float dr49CEAvgIV = 0f;
				int dr49Count = 0;
				float ceDeltaOIWorth = 0f;
				
				float dr16CEAvgIV = 0f;
				int dr16Count = 0;
				
				List<Float> dr16CEIvList = new ArrayList<Float>();
				List<Float> dr49CEIvList = new ArrayList<Float>();
				List<Float> dr46CEIvList = new ArrayList<Float>();
				List<Float> dr4PlusCEIvList = new ArrayList<Float>();
				List<Float> fullCEIvList = new ArrayList<Float>();
				List<Float> outlierCEIvList = new ArrayList<Float>();
				List<Float> dr19WholeStrikeCEIvList = new ArrayList<Float>();
				float totalChangeInCEIV = 0f;
				for(OptionGreek aGreek: ceOptionGreeks) {
					float delta = Math.abs(aGreek.getDelta());
					fullCEIvList.add(aGreek.getIv());
					
					if (delta >= 0.1f && delta <= 0.6f) dr16CEIvList.add(aGreek.getIv());
					if (delta >= 0.4f && delta <= 0.9f) dr49CEIvList.add(aGreek.getIv());
					if (delta >= 0.4f && delta <= 0.6f) dr46CEIvList.add(aGreek.getIv());
					if (delta >= 0.4f ) dr4PlusCEIvList.add(aGreek.getIv());
					
					if (delta >= lowerDelta && delta <= upperDelta) {
						
						float curIv = aGreek.getIv();
						float ltp = aGreek.getLtp();
						float oi = aGreek.getOi();
						float gamma = aGreek.getGamma();
						
						totalChangeInCEIV =  totalChangeInCEIV + aGreek.getChangeInIv();
						
						if (lastIvRead<0.1f || curIv < lastIvRead + 5f) {
							deltaRangeCEAvgLtp = deltaRangeCEAvgLtp + ltp;
							deltaRangeCEAvgIv = deltaRangeCEAvgIv + curIv;
							ceDeltaOIWorth = ceDeltaOIWorth + oi*delta;	
							deltaRangeCEAvgDelta = deltaRangeCEAvgDelta + delta;
							deltaRangeCEAvgGamma = deltaRangeCEAvgGamma + gamma;
							deltaRangeCEAvgVega = deltaRangeCEAvgVega + aGreek.getVega();
							deltaRangeCEWorth = deltaRangeCEWorth + oi*ltp;
							deltaRangeCEOI = deltaRangeCEOI + oi;
							deltaRangeCEDeltaOI = deltaRangeCEDeltaOI + oi*delta;
							deltaRangeCEGammaOI = deltaRangeCEGammaOI + oi*gamma;
							lastIvRead = curIv; 
							recCount++;
						} else {
							outlierCEIvList.add(curIv);
						}
						fullcount++;
						deltaRangeCEFullAvgIv = deltaRangeCEFullAvgIv + curIv;
						deltaRangeCEFullGamma = deltaRangeCEFullGamma + gamma;
						deltaRangeCEFullDeltaOI = deltaRangeCEFullDeltaOI + oi* delta;
						
						if (delta >= 0.4f && delta <= 0.9f) {
							dr49CEAvgIV = dr49CEAvgIV + curIv;
							dr49Count++;
						}
						if (delta >= 0.1f && delta <= 0.6f) {
							dr16CEAvgIV = dr16CEAvgIV + curIv;
							dr16Count++;
						}
						if (getStrikePriceFromOptionName(aGreek.getTradingSymbol())%100 == 0) { // Strike ends with 00 (full number, no 50s) 
							dr19WholeStrikeCEIvList.add(curIv);
						}
					}
				}
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
				retMap.put("deltaRangeHybridCEAvgGamma",deltaRangeHybridCEAvgGamma);
				retMap.put("deltaRangeCEOutlierRatio", (float)fullcount/(float)recCount);
				retMap.put("dr49CEAvgIV",dr49CEAvgIV!=0?dr49CEAvgIV/(float)dr49Count:0);
				retMap.put("dr16CEAvgIV",dr16CEAvgIV!=0?dr16CEAvgIV/(float)dr16Count:0);
				retMap.put("countCETotal",(float) countCETotal);
				retMap.put("countCEOutlier",(float) countCEOutlier);
				retMap.put("ceDeltaOIWorth",ceDeltaOIWorth);
				
				retMap.put("fullRangeCETotalIV",(float) fullCEIvList.stream().mapToDouble(d -> d).sum());
				
				retMap.put("dr16CETotalIV",(float) dr16CEIvList.stream().mapToDouble(d -> d).sum());
				retMap.put("dr49CETotalIV",(float) dr49CEIvList.stream().mapToDouble(d -> d).sum());
				retMap.put("dr46CETotalIV",(float) dr46CEIvList.stream().mapToDouble(d -> d).sum());
				retMap.put("dr4PlusCETotalIV",(float) dr4PlusCEIvList.stream().mapToDouble(d -> d).sum());
				
				retMap.put("outlierCEMinIV", outlierCEIvList.size()>0?outlierCEIvList.get(0):0f);
				retMap.put("outlierCEMaxIV", outlierCEIvList.size()>0?outlierCEIvList.get(outlierCEIvList.size()-1):0f);
				retMap.put("outlierCETotalIV",(float) outlierCEIvList.stream().mapToDouble(d -> d).sum());
				retMap.put("outlierCEAvgIV",(float) outlierCEIvList.stream().mapToDouble(d -> d).average().orElse(0.0));
				retMap.put("outlierCEMedianIV", (float) outlierCEIvList.stream().mapToDouble(d -> d).sorted().skip((outlierCEIvList.size()-1)/2).limit(2-outlierCEIvList.size()%2).average().orElse(0.0) );
				
				retMap.put("dr19WholeStrikeCEAvgIV",(float) dr19WholeStrikeCEIvList.stream().mapToDouble(d -> d).average().orElse(0.0));
				retMap.put("totalChangeInCEIV", totalChangeInCEIV);
				// Now PE
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
				
				float dr49PEAvgIV = 0f;
				dr49Count = 0;
				float dr16PEAvgIV = 0f;
				dr16Count = 0;
				float peDeltaOIWorth = 0f;
				
				List<Float> dr16PEIvList = new ArrayList<Float>();
				List<Float> dr49PEIvList = new ArrayList<Float>();
				List<Float> dr46PEIvList = new ArrayList<Float>();
				List<Float> dr4PlusPEIvList = new ArrayList<Float>();
				List<Float> fullPEIvList = new ArrayList<Float>();
				List<Float> outlierPEIvList = new ArrayList<Float>();
				List<Float> dr19WholeStrikePEIvList = new ArrayList<Float>();
				float totalChangeInPEIV = 0f;
				for(OptionGreek aGreek: peOptionGreeks) {
					float delta = Math.abs(aGreek.getDelta());
					fullPEIvList.add(aGreek.getIv());
					
					if (delta >= 0.1f && delta <= 0.6f) dr16PEIvList.add(aGreek.getIv());
					if (delta >= 0.4f && delta <= 0.9f) dr49PEIvList.add(aGreek.getIv());
					if (delta >= 0.4f && delta <= 0.6f) dr46PEIvList.add(aGreek.getIv());
					if (delta >= 0.4f ) dr4PlusPEIvList.add(aGreek.getIv());
					
					if (delta >= lowerDelta && delta <= upperDelta) {
						float curIv = aGreek.getIv();						
						float ltp = aGreek.getLtp();
						float oi = aGreek.getOi();				
						float gamma = aGreek.getGamma();
						
						totalChangeInPEIV = totalChangeInPEIV + aGreek.getChangeInIv();
						
						if (lastIvRead<0.1f || curIv < lastIvRead + 5f) {
							deltaRangePEAvgLtp = deltaRangePEAvgLtp + ltp;
							deltaRangePEAvgIv = deltaRangePEAvgIv + curIv;
							peDeltaOIWorth = peDeltaOIWorth + oi*delta;
							deltaRangePEAvgDelta = deltaRangePEAvgDelta + delta;
							deltaRangePEAvgGamma = deltaRangePEAvgGamma + gamma;
							deltaRangePEAvgVega = deltaRangePEAvgVega + aGreek.getVega();
							deltaRangePEWorth = deltaRangePEWorth + oi*ltp;
							deltaRangePEOI = deltaRangePEOI + oi;
							deltaRangePEDeltaOI = deltaRangePEDeltaOI + oi*delta;
							deltaRangePEGammaOI = deltaRangePEGammaOI + oi*gamma;
							lastIvRead = curIv; 
							recCount++;
						} else {
							outlierPEIvList.add(curIv);
						}
						fullcount++;
						deltaRangePEFullAvgIv = deltaRangePEFullAvgIv + curIv;
						deltaRangePEFullGamma = deltaRangePEFullGamma + gamma;
						deltaRangePEFullDeltaOI = deltaRangePEFullDeltaOI + oi* delta;
						if (delta >= 0.4f && delta <= 0.9f) {
							dr49PEAvgIV = dr49PEAvgIV + curIv;
							dr49Count++;
						}
						if (delta >= 0.1f && delta <= 0.6f) {
							dr16PEAvgIV = dr16PEAvgIV + curIv;
							dr16Count++;
						}
						if (getStrikePriceFromOptionName(aGreek.getTradingSymbol())%100 == 0) { // Strike ends with 00 (full number, no 50s) 
							dr19WholeStrikePEIvList.add(curIv);
						}
					}
				}
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
				
				retMap.put("fullRangePETotalIV",(float) fullPEIvList.stream().mapToDouble(d -> d).sum());
				retMap.put("dr16PETotalIV",(float) dr16PEIvList.stream().mapToDouble(d -> d).sum());
				retMap.put("dr49PETotalIV",(float) dr49PEIvList.stream().mapToDouble(d -> d).sum());
				retMap.put("dr46PETotalIV",(float) dr46PEIvList.stream().mapToDouble(d -> d).sum());
				retMap.put("dr4PlusPETotalIV",(float) dr4PlusPEIvList.stream().mapToDouble(d -> d).sum());
				
				retMap.put("outlierPEMinIV", outlierPEIvList.size()>0?outlierPEIvList.get(0):0f);
				retMap.put("outlierPEMaxIV", outlierPEIvList.size()>0?outlierPEIvList.get(outlierPEIvList.size()-1):0f);
				retMap.put("outlierPETotalIV",(float) outlierPEIvList.stream().mapToDouble(d -> d).sum());
				retMap.put("outlierPEAvgIV",(float) outlierPEIvList.stream().mapToDouble(d -> d).average().orElse(0.0));
				retMap.put("outlierPEMedianIV", (float) outlierPEIvList.stream().mapToDouble(d -> d).sorted().skip((outlierPEIvList.size()-1)/2).limit(2-outlierPEIvList.size()%2).average().orElse(0.0) );
				retMap.put("dr19WholeStrikePEAvgIV",(float) dr19WholeStrikePEIvList.stream().mapToDouble(d -> d).average().orElse(0.0));
				retMap.put("totalChangeInPEIV", totalChangeInPEIV);
				
				// Gamma exposure
				Map<Integer, Float> gammaPerStrike = new HashMap<Integer, Float>();
				for(OptionGreek aGreek: ceOptionGreeks) {
					float gammaExposure = aGreek.getOi()*aGreek.getGamma();
					int strike = getStrike(aGreek.getTradingSymbol());
					gammaExposure = gammaExposure + (gammaPerStrike.get(strike)!=null?gammaPerStrike.get(strike):0f);
					gammaPerStrike.put(strike, gammaExposure);
				}
				for(OptionGreek aGreek: peOptionGreeks) {
					float gammaExposure = aGreek.getOi()*aGreek.getGamma();
					int strike = getStrike(aGreek.getTradingSymbol());
					gammaExposure = gammaExposure - (gammaPerStrike.get(strike)!=null?gammaPerStrike.get(strike):0f);
					gammaPerStrike.put(strike, gammaExposure);
				}
				// Convert HashMap entries to a List
		        List<Map.Entry<Integer, Float>> entryList = new ArrayList<>(gammaPerStrike.entrySet());

		        // Sort the List by value in ascending order
		        Collections.sort(entryList, (entry1, entry2) -> entry1.getValue().compareTo(entry2.getValue()));

		        // Create a LinkedHashMap to store the sorted entries
		        LinkedHashMap<Integer, Float> sortedMap = new LinkedHashMap<>();
		        for (Map.Entry<Integer, Float> entry : entryList) {
		            sortedMap.put(entry.getKey(), entry.getValue());
		        }
		        
		        float minGammaExposure = Float.MAX_VALUE;
		        float maxGammaExposure = Float.MIN_VALUE;
		        float netGammaExposure = 0f;
				Iterator<Integer> iter = sortedMap.keySet().iterator();
				while (iter.hasNext()) {
					int strike = iter.next();
					if (gammaPerStrike.get(strike) < -1000 || gammaPerStrike.get(strike) > 1000) {
						if (gammaPerStrike.get(strike) < minGammaExposure) minGammaExposure = gammaPerStrike.get(strike);
						if (gammaPerStrike.get(strike) > maxGammaExposure) maxGammaExposure = gammaPerStrike.get(strike);
						//fileLogTelegramWriter.write(" For Strike " + strike + " gamma exposure " + gammaPerStrike.get(strike));
					}
					netGammaExposure = netGammaExposure + gammaPerStrike.get(strike);
				}
				retMap.put("minGammaExposure", Math.abs(minGammaExposure));
				retMap.put("maxGammaExposure",maxGammaExposure);
				retMap.put("netGammaExposure",netGammaExposure);
				
				// Gamma exposure with strike distance
				Map<Integer, Float> gammaPerStrikeDistance = new HashMap<Integer, Float>();
				for(OptionGreek aGreek: ceOptionGreeks) {
					int strike = getStrike(aGreek.getTradingSymbol());
					float gammaExposure = aGreek.getOi()*aGreek.getGamma()*((strike-this.instrumentLtp)/2f);
					gammaExposure = gammaExposure + (gammaPerStrikeDistance.get(strike)!=null?gammaPerStrikeDistance.get(strike):0f);
					gammaPerStrikeDistance.put(strike, gammaExposure);
				}
				
				for(OptionGreek aGreek: peOptionGreeks) {
					int strike = getStrike(aGreek.getTradingSymbol());
					float gammaExposure = aGreek.getOi()*aGreek.getGamma()*((this.instrumentLtp-strike)/2f);
					gammaExposure = gammaExposure - (gammaPerStrikeDistance.get(strike)!=null?gammaPerStrikeDistance.get(strike):0f);
					gammaPerStrikeDistance.put(strike, gammaExposure);
				}
				
				// Convert HashMap entries to a List
		        List<Map.Entry<Integer, Float>> entryList2 = new ArrayList<>(gammaPerStrikeDistance.entrySet());

		        // Sort the List by value in ascending order
		        Collections.sort(entryList2, (entry1, entry2) -> entry1.getValue().compareTo(entry2.getValue()));

		        // Create a LinkedHashMap to store the sorted entries
		        LinkedHashMap<Integer, Float> sortedMap2 = new LinkedHashMap<>();
		        for (Map.Entry<Integer, Float> entry : entryList2) {
		            sortedMap2.put(entry.getKey(), entry.getValue());
		        }
		        
		        float minGammaExposureStrikeDistance = Float.MAX_VALUE;
		        float maxGammaExposureStrikeDistance = Float.MIN_VALUE;
		        float netGammaExposureStrikeDistance = 0f;
		        
				iter = sortedMap2.keySet().iterator();
				while (iter.hasNext()) {
					int strike = iter.next();
					
					if (gammaPerStrikeDistance.get(strike) < minGammaExposureStrikeDistance) {
						minGammaExposureStrikeDistance = gammaPerStrikeDistance.get(strike);
					}
					if (gammaPerStrikeDistance.get(strike) > maxGammaExposureStrikeDistance) {
						maxGammaExposureStrikeDistance = gammaPerStrikeDistance.get(strike);
					}
					fileLogTelegramWriter.write(" For Strike " + strike + " strike distance gamma exposure " + gammaPerStrikeDistance.get(strike));
				
					netGammaExposureStrikeDistance = netGammaExposureStrikeDistance + gammaPerStrikeDistance.get(strike);
				}
				retMap.put("minGammaExposureWithStrike", Math.abs(minGammaExposureStrikeDistance)/1000f);
				retMap.put("maxGammaExposureWithStrike",maxGammaExposureStrikeDistance/1000f);
				retMap.put("netGammaExposureWithStrike",netGammaExposureStrikeDistance);
				
				
				float changein5secCeIV = 0;
				float changein5secPeIV = 0;
				
				float drResChangein5secCeIV = 0;
				float drResChangein5secPeIV = 0;
				
				float dr49Changein5secCeGamma = 0;
				float dr49Changein5secPeGamma = 0;
				float dr49Changein5secCeTheta = 0f; 
				float dr49Changein5secCeDelta = 0f; 
				float dr49Changein5secCeVega  = 0f;
				float dr49Changein5secCeIv  = 0f;
				float dr49Changein5secCeLtp = 0f;
				float dr49Changein5secPeTheta = 0f; 
				float dr49Changein5secPeDelta = 0f; 
				float dr49Changein5secPeVega  = 0f;
				float dr49Changein5secPeIv  = 0f;
				float dr49Changein5secPeLtp = 0f;
				
				float dr16Changein5secCeGamma = 0;
				float dr16Changein5secPeGamma = 0;
				float dr16Changein5secCeTheta = 0f; 
				float dr16Changein5secCeDelta = 0f; 
				float dr16Changein5secCeVega  = 0f;
				float dr16Changein5secCeIv  = 0f;
				float dr16Changein5secCeLtp = 0f;
				float dr16Changein5secPeTheta = 0f; 
				float dr16Changein5secPeDelta = 0f; 
				float dr16Changein5secPeVega  = 0f;
				float dr16Changein5secPeIv  = 0f;
				float dr16Changein5secPeLtp = 0f;
				
				float drWhlStrkChangein5secCeGamma = 0;
				float drWhlStrkChangein5secPeGamma = 0;
				float drWhlStrkChangein5secCeTheta = 0f; 
				float drWhlStrkChangein5secCeDelta = 0f; 
				float drWhlStrkChangein5secCeVega  = 0f;
				float drWhlStrkChangein5secCeIv  = 0f;
				float drWhlStrkChangein5secCeLtp = 0f;
				float drWhlStrkChangein5secPeTheta = 0f; 
				float drWhlStrkChangein5secPeDelta = 0f; 
				float drWhlStrkChangein5secPeVega  = 0f;
				float drWhlStrkChangein5secPeIv  = 0f;
				float drWhlStrkChangein5secPeLtp = 0f;
				
				float pt200Changein5secCeTheta = 0l;
				float pt200Changein5secPeTheta = 0l;
				
				for(OptionGreek aGreek: ceOptionGreeks) {
					for(OptionGreek prevGreek: prevCeOptionGreeks) {
						if (aGreek.getTradingSymbol().equals(prevGreek.getTradingSymbol())) {
							changein5secCeIV = changein5secCeIV + (aGreek.getIv() - prevGreek.getIv());
						}
					}
					if (Math.abs(aGreek.getDelta()) >= 0.25f && Math.abs(aGreek.getDelta()) <= 0.75f) { 
						for(OptionGreek prevGreek: prevCeOptionGreeks) {
							if (aGreek.getTradingSymbol().equals(prevGreek.getTradingSymbol())) {
								drResChangein5secCeIV = drResChangein5secCeIV + (aGreek.getIv() - prevGreek.getIv());
							}
						}
					}
					if (Math.abs(aGreek.getDelta()) >= 0.4f && Math.abs(aGreek.getDelta()) <= 0.9f) { 
						for(OptionGreek prevGreek: prevCeOptionGreeks) {
							if (aGreek.getTradingSymbol().equals(prevGreek.getTradingSymbol())) {
								dr49Changein5secCeGamma = dr49Changein5secCeGamma + (aGreek.getGamma() - prevGreek.getGamma());
								dr49Changein5secCeTheta = dr49Changein5secCeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevGreek.getTheta()));
								dr49Changein5secCeDelta = dr49Changein5secCeDelta + (Math.abs(aGreek.getDelta()) - Math.abs(prevGreek.getDelta()));
								dr49Changein5secCeVega = dr49Changein5secCeVega + (aGreek.getVega() - prevGreek.getVega());
								dr49Changein5secCeIv = dr49Changein5secCeIv + (aGreek.getIv() - prevGreek.getIv());
								dr49Changein5secCeLtp = dr49Changein5secCeLtp + (aGreek.getLtp() - prevGreek.getLtp());
							}
						}
					}
					if (Math.abs(aGreek.getDelta()) >= 0.1f && Math.abs(aGreek.getDelta()) <= 0.6f) { 
						for(OptionGreek prevGreek: prevCeOptionGreeks) {
							if (aGreek.getTradingSymbol().equals(prevGreek.getTradingSymbol())) {
								dr16Changein5secCeGamma = dr16Changein5secCeGamma + (aGreek.getGamma() - prevGreek.getGamma());
								dr16Changein5secCeTheta = dr16Changein5secCeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevGreek.getTheta()));
								dr16Changein5secCeDelta = dr16Changein5secCeDelta + (Math.abs(aGreek.getDelta()) - Math.abs(prevGreek.getDelta()));
								dr16Changein5secCeVega = dr16Changein5secCeVega + (aGreek.getVega() - prevGreek.getVega());
								dr16Changein5secCeIv = dr16Changein5secCeIv + (aGreek.getIv() - prevGreek.getIv());
								dr16Changein5secCeLtp = dr16Changein5secCeLtp + (aGreek.getLtp() - prevGreek.getLtp());
							}
						}
					}
					
					if (aGreek.getStrike() > this.instrumentLtp + 500 && aGreek.getStrike() < this.instrumentLtp + 750 ) {
						
						for(OptionGreek prevGreek: prevCeOptionGreeks) {
							if (aGreek.getTradingSymbol().equals(prevGreek.getTradingSymbol())) {
								drWhlStrkChangein5secCeDelta = drWhlStrkChangein5secCeDelta + (Math.abs(aGreek.getDelta()) - Math.abs(prevGreek.getDelta()));
								drWhlStrkChangein5secCeGamma = drWhlStrkChangein5secCeGamma + (aGreek.getGamma() - prevGreek.getGamma());
								drWhlStrkChangein5secCeVega = drWhlStrkChangein5secCeVega + (aGreek.getVega() - prevGreek.getVega());
								drWhlStrkChangein5secCeTheta = drWhlStrkChangein5secCeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevGreek.getTheta()));
								drWhlStrkChangein5secCeIv = drWhlStrkChangein5secCeIv + (aGreek.getIv() - prevGreek.getIv());
								drWhlStrkChangein5secCeLtp  = drWhlStrkChangein5secCeLtp + (aGreek.getLtp() - prevGreek.getLtp());
								break;
							}
						}
					}
					
					if (aGreek.getStrike() < this.instrumentLtp + 200 && aGreek.getStrike() > this.instrumentLtp - 200 ) {						
						for(OptionGreek prevGreek: prevCeOptionGreeks) {
							if (aGreek.getTradingSymbol().equals(prevGreek.getTradingSymbol())) {
								pt200Changein5secCeTheta = pt200Changein5secCeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevGreek.getTheta()));
							}
						}
					}
				}
				for(OptionGreek aGreek: peOptionGreeks) {
					for(OptionGreek prevGreek: prevPeOptionGreeks) {
						if (aGreek.getTradingSymbol().equals(prevGreek.getTradingSymbol())) {
							changein5secPeIV = changein5secPeIV + (aGreek.getIv() - prevGreek.getIv());
						}
					}
					if (Math.abs(aGreek.getDelta()) >= 0.25f && Math.abs(aGreek.getDelta()) <= 0.75f) { 
						for(OptionGreek prevGreek: prevPeOptionGreeks) {
							if (aGreek.getTradingSymbol().equals(prevGreek.getTradingSymbol())) {
								drResChangein5secPeIV = drResChangein5secPeIV + (aGreek.getIv() - prevGreek.getIv());
							}
						}
					}
					if (Math.abs(aGreek.getDelta()) >= 0.4f && Math.abs(aGreek.getDelta()) <= 0.9f) { 
						for(OptionGreek prevGreek: prevPeOptionGreeks) {
							if (aGreek.getTradingSymbol().equals(prevGreek.getTradingSymbol())) {
								dr49Changein5secPeGamma = dr49Changein5secPeGamma + (aGreek.getGamma() - prevGreek.getGamma());
								
								dr49Changein5secPeTheta = dr49Changein5secPeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevGreek.getTheta()));
								dr49Changein5secPeDelta = dr49Changein5secPeDelta + (Math.abs(aGreek.getDelta()) - Math.abs(prevGreek.getDelta()));
								dr49Changein5secPeVega = dr49Changein5secPeVega + (aGreek.getVega() - prevGreek.getVega());
								dr49Changein5secPeIv = dr49Changein5secPeIv + (aGreek.getIv() - prevGreek.getIv());
								dr49Changein5secPeLtp = dr49Changein5secPeLtp + (aGreek.getLtp() - prevGreek.getLtp());
							}
						}
					}
					if (Math.abs(aGreek.getDelta()) >= 0.1f && Math.abs(aGreek.getDelta()) <= 0.6f) { 
						for(OptionGreek prevGreek: prevPeOptionGreeks) {
							if (aGreek.getTradingSymbol().equals(prevGreek.getTradingSymbol())) {
								dr16Changein5secPeGamma = dr16Changein5secPeGamma + (aGreek.getGamma() - prevGreek.getGamma());
								
								dr16Changein5secPeTheta = dr16Changein5secPeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevGreek.getTheta()));
								dr16Changein5secPeDelta = dr16Changein5secPeDelta + (Math.abs(aGreek.getDelta()) - Math.abs(prevGreek.getDelta()));
								dr16Changein5secPeVega = dr16Changein5secPeVega + (aGreek.getVega() - prevGreek.getVega());
								dr16Changein5secPeIv = dr16Changein5secPeIv + (aGreek.getIv() - prevGreek.getIv());
								dr16Changein5secPeLtp = dr16Changein5secPeLtp + (aGreek.getLtp() - prevGreek.getLtp());
							}
						}
					}
					if (aGreek.getStrike() < this.instrumentLtp -500 && aGreek.getStrike() > this.instrumentLtp -750) {
						for(OptionGreek prevGreek: prevPeOptionGreeks) {
							if (aGreek.getTradingSymbol().equals(prevGreek.getTradingSymbol())) {
								drWhlStrkChangein5secPeDelta = drWhlStrkChangein5secPeDelta + (Math.abs(aGreek.getDelta()) - Math.abs(prevGreek.getDelta()));
								drWhlStrkChangein5secPeGamma = drWhlStrkChangein5secPeGamma + (aGreek.getGamma() - prevGreek.getGamma());
								drWhlStrkChangein5secPeVega = drWhlStrkChangein5secPeVega + (aGreek.getVega() - prevGreek.getVega());
								drWhlStrkChangein5secPeTheta = drWhlStrkChangein5secPeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevGreek.getTheta()));
								drWhlStrkChangein5secPeIv = drWhlStrkChangein5secPeIv + (aGreek.getIv() - prevGreek.getIv());
								drWhlStrkChangein5secPeLtp  = drWhlStrkChangein5secPeLtp + (aGreek.getLtp() - prevGreek.getLtp());
								break;
							}
						}
					}
					
					if (aGreek.getStrike() < this.instrumentLtp + 200 && aGreek.getStrike() > this.instrumentLtp - 200 ) {						
						for(OptionGreek prevGreek: prevPeOptionGreeks) {
							if (aGreek.getTradingSymbol().equals(prevGreek.getTradingSymbol())) {
								pt200Changein5secPeTheta = pt200Changein5secPeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevGreek.getTheta()));
							}
						}
					}
				}
				prevCeOptionGreeks = ceOptionGreeks;
				prevPeOptionGreeks = peOptionGreeks;
				
				accumulatedChangein5secCeIV = accumulatedChangein5secCeIV + changein5secCeIV;
				accumulatedChangein5secPeIV = accumulatedChangein5secPeIV + changein5secPeIV;
				
				drResAccumulatedChangein5secCeIV = drResAccumulatedChangein5secCeIV + drResChangein5secCeIV;
				drResAccumulatedChangein5secPeIV = drResAccumulatedChangein5secPeIV + drResChangein5secPeIV;
				
				dr49AccumulatedChangein5secCeGamma = dr49AccumulatedChangein5secCeGamma + dr49Changein5secCeGamma;
				dr49AccumulatedChangein5secPeGamma = dr49AccumulatedChangein5secPeGamma + dr49Changein5secPeGamma;
				
				dr49AccumulatedChangein5secCeTheta = dr49AccumulatedChangein5secCeTheta + dr49Changein5secCeTheta;
				dr49AccumulatedChangein5secPeTheta = dr49AccumulatedChangein5secPeTheta + dr49Changein5secPeTheta;
				
				dr49AccumulatedChangein5secCeDelta = dr49AccumulatedChangein5secCeDelta + dr49Changein5secCeDelta;
				dr49AccumulatedChangein5secPeDelta = dr49AccumulatedChangein5secPeDelta + dr49Changein5secPeDelta;
				
				dr49AccumulatedChangein5secCeVega = dr49AccumulatedChangein5secCeVega + dr49Changein5secCeVega;
				dr49AccumulatedChangein5secPeVega = dr49AccumulatedChangein5secPeVega  + dr49Changein5secPeVega;
				
				dr49AccumulatedChangein5secCeIv = dr49AccumulatedChangein5secCeIv + dr49Changein5secCeIv;
				dr49AccumulatedChangein5secPeIv = dr49AccumulatedChangein5secPeIv + dr49Changein5secPeIv;
				
				dr49AccumulatedChangein5secCeLtp = dr49AccumulatedChangein5secCeLtp + dr49Changein5secCeLtp;
				dr49AccumulatedChangein5secPeLtp = dr49AccumulatedChangein5secPeLtp + dr49Changein5secPeLtp;
				
				
				dr16AccumulatedChangein5secCeGamma = dr16AccumulatedChangein5secCeGamma + dr16Changein5secCeGamma;
				dr16AccumulatedChangein5secPeGamma = dr16AccumulatedChangein5secPeGamma + dr16Changein5secPeGamma;
				
				dr16AccumulatedChangein5secCeTheta = dr16AccumulatedChangein5secCeTheta + dr16Changein5secCeTheta;
				dr16AccumulatedChangein5secPeTheta = dr16AccumulatedChangein5secPeTheta + dr16Changein5secPeTheta;
				
				dr16AccumulatedChangein5secCeDelta = dr16AccumulatedChangein5secCeDelta + dr16Changein5secCeDelta;
				dr16AccumulatedChangein5secPeDelta = dr16AccumulatedChangein5secPeDelta + dr16Changein5secPeDelta;
				
				dr16AccumulatedChangein5secCeVega = dr16AccumulatedChangein5secCeVega + dr16Changein5secCeVega;
				dr16AccumulatedChangein5secPeVega = dr16AccumulatedChangein5secPeVega  + dr16Changein5secPeVega;
				
				dr16AccumulatedChangein5secCeIv = dr16AccumulatedChangein5secCeIv + dr16Changein5secCeIv;
				dr16AccumulatedChangein5secPeIv = dr16AccumulatedChangein5secPeIv + dr16Changein5secPeIv;
				
				dr16AccumulatedChangein5secCeLtp = dr16AccumulatedChangein5secCeLtp + dr16Changein5secCeLtp;
				dr16AccumulatedChangein5secPeLtp = dr16AccumulatedChangein5secPeLtp + dr16Changein5secPeLtp;
				
				drWhlStrkAccumulatedChangein5secCeGamma = drWhlStrkAccumulatedChangein5secCeGamma + drWhlStrkChangein5secCeGamma;
				drWhlStrkAccumulatedChangein5secPeGamma = drWhlStrkAccumulatedChangein5secPeGamma + drWhlStrkChangein5secPeGamma;
				
				drWhlStrkAccumulatedChangein5secCeTheta = drWhlStrkAccumulatedChangein5secCeTheta + drWhlStrkChangein5secCeTheta;
				drWhlStrkAccumulatedChangein5secPeTheta = drWhlStrkAccumulatedChangein5secPeTheta + drWhlStrkChangein5secPeTheta;
				
				drWhlStrkAccumulatedChangein5secCeDelta = drWhlStrkAccumulatedChangein5secCeDelta + drWhlStrkChangein5secCeDelta;
				drWhlStrkAccumulatedChangein5secPeDelta = drWhlStrkAccumulatedChangein5secPeDelta + drWhlStrkChangein5secPeDelta;
				
				drWhlStrkAccumulatedChangein5secCeVega = drWhlStrkAccumulatedChangein5secCeVega + drWhlStrkChangein5secCeVega;
				drWhlStrkAccumulatedChangein5secPeVega = drWhlStrkAccumulatedChangein5secPeVega  + drWhlStrkChangein5secPeVega;
				
				drWhlStrkAccumulatedChangein5secCeIv = drWhlStrkAccumulatedChangein5secCeIv + drWhlStrkChangein5secCeIv;
				drWhlStrkAccumulatedChangein5secPeIv = drWhlStrkAccumulatedChangein5secPeIv + drWhlStrkChangein5secPeIv;
				
				drWhlStrkAccumulatedChangein5secCeLtp = drWhlStrkAccumulatedChangein5secCeLtp + drWhlStrkChangein5secCeLtp;
				drWhlStrkAccumulatedChangein5secPeLtp = drWhlStrkAccumulatedChangein5secPeLtp + drWhlStrkChangein5secPeLtp;
				
				pt200AccumulatedChangein5secCeTheta = pt200AccumulatedChangein5secCeTheta + pt200Changein5secCeTheta;
				pt200AccumulatedChangein5secPeTheta = pt200AccumulatedChangein5secPeTheta + pt200Changein5secPeTheta;
								
				retMap.put("changein5secCeIV",changein5secCeIV);
				retMap.put("changein5secPeIV",changein5secPeIV);
				
				retMap.put("accumulatedChangein5secCeIV",accumulatedChangein5secCeIV);
				retMap.put("accumulatedChangein5secPeIV",accumulatedChangein5secPeIV);
				
				retMap.put("drResAccumulatedChangein5secCeIV",drResAccumulatedChangein5secCeIV);
				retMap.put("drResAccumulatedChangein5secPeIV",drResAccumulatedChangein5secPeIV);
				
				
				retMap.put("dr49AccumulatedChangein5secCeGamma",dr49AccumulatedChangein5secCeGamma);
				retMap.put("dr49AccumulatedChangein5secPeGamma",dr49AccumulatedChangein5secPeGamma);				
				retMap.put("dr49AccumulatedChangein5secCeDelta",dr49AccumulatedChangein5secCeDelta);
				retMap.put("dr49AccumulatedChangein5secPeDelta",dr49AccumulatedChangein5secPeDelta);				
				retMap.put("dr49AccumulatedChangein5secCeVega",dr49AccumulatedChangein5secCeVega);
				retMap.put("dr49AccumulatedChangein5secPeVega",dr49AccumulatedChangein5secPeVega);				
				retMap.put("dr49AccumulatedChangein5secCeTheta",dr49AccumulatedChangein5secCeTheta);
				retMap.put("dr49AccumulatedChangein5secPeTheta",dr49AccumulatedChangein5secPeTheta);				
				retMap.put("dr49AccumulatedChangein5secCeIv",dr49AccumulatedChangein5secCeIv);
				retMap.put("dr49AccumulatedChangein5secPeIv",dr49AccumulatedChangein5secPeIv);				
				retMap.put("dr49AccumulatedChangein5secCeLtp",dr49AccumulatedChangein5secCeLtp);
				retMap.put("dr49AccumulatedChangein5secPeLtp",dr49AccumulatedChangein5secPeLtp);
				
				retMap.put("dr16AccumulatedChangein5secCeGamma",dr16AccumulatedChangein5secCeGamma);
				retMap.put("dr16AccumulatedChangein5secPeGamma",dr16AccumulatedChangein5secPeGamma);				
				retMap.put("dr16AccumulatedChangein5secCeDelta",dr16AccumulatedChangein5secCeDelta);
				retMap.put("dr16AccumulatedChangein5secPeDelta",dr16AccumulatedChangein5secPeDelta);				
				retMap.put("dr16AccumulatedChangein5secCeVega",dr16AccumulatedChangein5secCeVega);
				retMap.put("dr16AccumulatedChangein5secPeVega",dr16AccumulatedChangein5secPeVega);				
				retMap.put("dr16AccumulatedChangein5secCeTheta",dr16AccumulatedChangein5secCeTheta);
				retMap.put("dr16AccumulatedChangein5secPeTheta",dr16AccumulatedChangein5secPeTheta);				
				retMap.put("dr16AccumulatedChangein5secCeIv",dr16AccumulatedChangein5secCeIv);
				retMap.put("dr16AccumulatedChangein5secPeIv",dr16AccumulatedChangein5secPeIv);				
				retMap.put("dr16AccumulatedChangein5secCeLtp",dr16AccumulatedChangein5secCeLtp);
				retMap.put("dr16AccumulatedChangein5secPeLtp",dr16AccumulatedChangein5secPeLtp);
				
				retMap.put("drWhlStrkAccumulatedChangein5secCeGamma",drWhlStrkAccumulatedChangein5secCeGamma);
				retMap.put("drWhlStrkAccumulatedChangein5secPeGamma",drWhlStrkAccumulatedChangein5secPeGamma);				
				retMap.put("drWhlStrkAccumulatedChangein5secCeDelta",drWhlStrkAccumulatedChangein5secCeDelta);
				retMap.put("drWhlStrkAccumulatedChangein5secPeDelta",drWhlStrkAccumulatedChangein5secPeDelta);				
				retMap.put("drWhlStrkAccumulatedChangein5secCeVega",drWhlStrkAccumulatedChangein5secCeVega);
				retMap.put("drWhlStrkAccumulatedChangein5secPeVega",drWhlStrkAccumulatedChangein5secPeVega);				
				retMap.put("drWhlStrkAccumulatedChangein5secCeTheta",drWhlStrkAccumulatedChangein5secCeTheta);
				retMap.put("drWhlStrkAccumulatedChangein5secPeTheta",drWhlStrkAccumulatedChangein5secPeTheta);				
				retMap.put("drWhlStrkAccumulatedChangein5secCeIv",drWhlStrkAccumulatedChangein5secCeIv);
				retMap.put("drWhlStrkAccumulatedChangein5secPeIv",drWhlStrkAccumulatedChangein5secPeIv);				
				retMap.put("drWhlStrkAccumulatedChangein5secCeLtp",drWhlStrkAccumulatedChangein5secCeLtp);
				retMap.put("drWhlStrkAccumulatedChangein5secPeLtp",drWhlStrkAccumulatedChangein5secPeLtp);
				
				retMap.put("pt200AccumulatedChangein5secCeTheta", pt200AccumulatedChangein5secCeTheta);
				retMap.put("pt200AccumulatedChangein5secPeTheta", pt200AccumulatedChangein5secPeTheta);
				
				// Top 5 strike Gamma Exposure
				List<OptionGreek> optionGreeks = new ArrayList<>();
				optionGreeks.addAll(ceOptionGreeks);
				optionGreeks.addAll(peOptionGreeks);
				
				Collections.sort(optionGreeks, new SortbyOiDesc());				
				Set<Integer> top5Strikes = new HashSet<Integer>();
				int recProcessed = 0;
				float ceTotal = 0f;
				float peTotal = 0f;
				for(OptionGreek aGreek: optionGreeks) {
					if (aGreek.getOi()*aGreek.getLtp()/10000000>10) {
						recProcessed++;
						top5Strikes.add(getStrike(aGreek.getTradingSymbol()));
						
						if(aGreek.getTradingSymbol().endsWith("CE")) {
							ceTotal = ceTotal + aGreek.getOi()*aGreek.getLtp()/10000000;
						} else {
							peTotal = peTotal + aGreek.getOi()*aGreek.getLtp()/10000000;
						}
						
					}
					if (recProcessed>=5) break;
				}
				
				float minGammaExposureTopN = Float.MAX_VALUE;
		        float maxGammaExposureTopN = Float.MIN_VALUE;
		        float netGammaExposureTopN = 0f;
		        
				iter = sortedMap2.keySet().iterator();
				while (iter.hasNext()) {
					int strike = iter.next();
					if (top5Strikes.contains(strike)) {
						if (gammaPerStrikeDistance.get(strike) < minGammaExposureTopN) {
							minGammaExposureTopN = gammaPerStrikeDistance.get(strike);
						}
						if (gammaPerStrikeDistance.get(strike) > maxGammaExposureTopN) {
							maxGammaExposureTopN = gammaPerStrikeDistance.get(strike);
						}
						fileLogTelegramWriter.write(" For Strike " + strike + " strike distance gamma exposure " + gammaPerStrikeDistance.get(strike));
						netGammaExposureTopN = netGammaExposureTopN + gammaPerStrikeDistance.get(strike);
					}	
				}
				retMap.put("minGammaExposureTopN", Math.abs(minGammaExposureTopN)/1000f);
				retMap.put("maxGammaExposureTopN",maxGammaExposureTopN/1000f);
				retMap.put("netGammaExposureTopN",netGammaExposureTopN/1000f);
				retMap.put("top5OiDiff",peTotal-ceTotal);
				
				
				// Cumulative IV diff between sequqntial strikes 
				Collections.sort(ceOptionGreeks, new SortbyStrike());
				Collections.sort(peOptionGreeks, new SortbyStrike());
				Collections.reverse(peOptionGreeks);
				
				lowerDelta = 0.1f;
				upperDelta = 0.5f;
				
				float prevIv = 0f;
				float cumulativeCEAvgIVDiff = 0f;
				recCount = 0;
				for(OptionGreek aGreek: ceOptionGreeks) {
					float delta = Math.abs(aGreek.getDelta());
					if (delta >= lowerDelta && delta <= upperDelta) {
						if (prevIv > 0.01f) {
							
							float ivDiff = aGreek.getIv()-prevIv;
							if (ivDiff > -1.5f && ivDiff < 1.5f) {
								recCount++;
								//cumulativeCEAvgIVDiff = cumulativeCEAvgIVDiff + ivDiff;
								cumulativeCEAvgIVDiff = cumulativeCEAvgIVDiff + aGreek.getIv();
								fileLogTelegramWriter.write("CE Strike "+ aGreek.getStrike()+" Iv Diff " + ivDiff + " delta="+delta + " Iv="+aGreek.getIv());
							}
						}
						prevIv = aGreek.getIv();
					}
				}
				if (recCount==0) recCount=1;
				retMap.put("cumulativeCEAvgIVDiff", cumulativeCEAvgIVDiff/(float)recCount);
				fileLogTelegramWriter.write("cumulativeCEAvgIVDiff="+cumulativeCEAvgIVDiff+" recCount="+recCount);
				prevIv = 0f;
				recCount = 0;
				float cumulativePEAvgIVDiff = 0f;
				for(OptionGreek aGreek: peOptionGreeks) {
					float delta = Math.abs(aGreek.getDelta());
					if (delta >= lowerDelta && delta <= upperDelta) {
						if (prevIv > 0.01f) {
							float ivDiff = aGreek.getIv()-prevIv;
							if (ivDiff > -1.5f && ivDiff < 1.5f) {
								recCount++;
								cumulativePEAvgIVDiff = cumulativePEAvgIVDiff + aGreek.getIv();
								fileLogTelegramWriter.write("PE Strike "+ aGreek.getStrike()+" Iv Diff " + ivDiff + " delta="+delta+ " Iv="+aGreek.getIv());
							}
						}
						prevIv = aGreek.getIv();
					}
				}
				if (recCount==0) recCount=1;
				retMap.put("cumulativePEAvgIVDiff", cumulativePEAvgIVDiff/(float)recCount);
				fileLogTelegramWriter.write("cumulativePEAvgIVDiff="+cumulativePEAvgIVDiff+" recCount="+recCount);
				
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
	
	private int getStrike(String optionName) {
		return KiteUtil.getStrike(optionName);
	}
	
	private OptionGreek[] getExactATMQuandrangle(float baseDelta) {
		OptionGreek[] returnGreeks = null;
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			List<OptionGreek> ceOptionGreeks = new ArrayList<OptionGreek>();
			List<OptionGreek> peOptionGreeks = new ArrayList<OptionGreek>();
			
			if (this.backtestDate == null) { // Live
				for(OptionGreek aGreek: getSnapshotGreeksFromCache()) {
					if (aGreek.getTradingSymbol().endsWith("CE")) {
						ceOptionGreeks.add(aGreek);
					} else { // PE
						peOptionGreeks.add(aGreek);
					}
				}
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
						if (optionname.endsWith("CE")) {
							ceOptionGreeks.add(aGreek);
						} else {
							peOptionGreeks.add(aGreek);
						}
					}
				}
				stmt.close();	
			}
		
			float minDelta = 1f;
			OptionGreek lowerCEOptionGreek = null;
			for(OptionGreek aGreek: ceOptionGreeks) {
				if (baseDelta - Math.abs(aGreek.getDelta()) >= 0 ) {
					float deltaDiff = baseDelta - Math.abs(aGreek.getDelta());
					if (deltaDiff < minDelta) {
						minDelta = deltaDiff;
						lowerCEOptionGreek = aGreek;
					}
				}				
			}
			minDelta = 1f;
			OptionGreek upperCEOptionGreek = null;
			for(OptionGreek aGreek: ceOptionGreeks) {
				if (Math.abs(aGreek.getDelta())-baseDelta >= 0 ) {
					float deltaDiff = Math.abs(aGreek.getDelta()) - baseDelta;
					if (deltaDiff < minDelta) {
						minDelta = deltaDiff;
						upperCEOptionGreek = aGreek;
					}
				}				
			}
			minDelta = 1f;
			OptionGreek lowerPEOptionGreek = null;
			for(OptionGreek aGreek: peOptionGreeks) {
				if (baseDelta - Math.abs(aGreek.getDelta()) >= 0 ) {
					float deltaDiff = baseDelta - Math.abs(aGreek.getDelta());
					if (deltaDiff < minDelta) {
						minDelta = deltaDiff;
						lowerPEOptionGreek = aGreek;
					}
				}				
			}
			minDelta = 1f;
			OptionGreek upperPEOptionGreek = null;
			for(OptionGreek aGreek: peOptionGreeks) {
				if (Math.abs(aGreek.getDelta())-baseDelta >= 0 ) {
					float deltaDiff = Math.abs(aGreek.getDelta()) - baseDelta;
					if (deltaDiff < minDelta) {
						minDelta = deltaDiff;
						upperPEOptionGreek = aGreek;
					}
				}				
			}
			print(lowerCEOptionGreek);
			print(upperCEOptionGreek);
			print(lowerPEOptionGreek);
			print(upperPEOptionGreek);
			
			float adjustedCEATMLtp = getScaledValue(Math.abs(lowerCEOptionGreek.getDelta()), Math.abs(upperCEOptionGreek.getDelta()), lowerCEOptionGreek.getLtp(), upperCEOptionGreek.getLtp(), 0.5f);
			float adjustedCEATMIV  = getScaledValue(Math.abs(lowerCEOptionGreek.getDelta()), Math.abs(upperCEOptionGreek.getDelta()), lowerCEOptionGreek.getIv(),  upperCEOptionGreek.getIv(),  0.5f);
			float adjustedCEATMGamma  = getScaledValue(Math.abs(lowerCEOptionGreek.getDelta()), Math.abs(upperCEOptionGreek.getDelta()), lowerCEOptionGreek.getGamma(),  upperCEOptionGreek.getGamma(),  0.5f);
			float adjustedCEATMVega = getScaledValue(Math.abs(lowerCEOptionGreek.getDelta()), Math.abs(upperCEOptionGreek.getDelta()), lowerCEOptionGreek.getVega(),  upperCEOptionGreek.getVega(),  0.5f);
			float adjustedCEATMTheta = getScaledValue(Math.abs(lowerCEOptionGreek.getDelta()), Math.abs(upperCEOptionGreek.getDelta()), lowerCEOptionGreek.getTheta(),  upperCEOptionGreek.getTheta(),  0.5f);
			
			float adjustedPEATMLtp = getScaledValue(Math.abs(lowerPEOptionGreek.getDelta()), Math.abs(upperPEOptionGreek.getDelta()), lowerPEOptionGreek.getLtp(), upperPEOptionGreek.getLtp(), 0.5f);
			float adjustedPEATMIV  = getScaledValue(Math.abs(lowerPEOptionGreek.getDelta()), Math.abs(upperPEOptionGreek.getDelta()), lowerPEOptionGreek.getIv(),  upperPEOptionGreek.getIv(),  0.5f);
			float adjustedPEATMGamma  = getScaledValue(Math.abs(lowerPEOptionGreek.getDelta()), Math.abs(upperPEOptionGreek.getDelta()), lowerPEOptionGreek.getGamma(),  upperPEOptionGreek.getGamma(),  0.5f);
			float adjustedPEATMVega = getScaledValue(Math.abs(lowerPEOptionGreek.getDelta()), Math.abs(upperPEOptionGreek.getDelta()), lowerPEOptionGreek.getVega(),  upperPEOptionGreek.getVega(),  0.5f);
			float adjustedPEATMTheta = getScaledValue(Math.abs(lowerPEOptionGreek.getDelta()), Math.abs(upperPEOptionGreek.getDelta()), lowerPEOptionGreek.getTheta(),  upperPEOptionGreek.getTheta(),  0.5f);
			
			OptionGreek adjustedCEReturnGreek = new OptionGreek("DummyCE", adjustedCEATMIV, 0.5f,adjustedCEATMVega, adjustedCEATMTheta, adjustedCEATMGamma, adjustedCEATMLtp);
			OptionGreek adjustedPEReturnGreek = new OptionGreek("DummyPE", adjustedPEATMIV, 0.5f,adjustedPEATMVega, adjustedPEATMTheta, adjustedPEATMGamma, adjustedPEATMLtp);
			
			fileLogTelegramWriter.write("adjustedCEATMLtp="+adjustedCEATMLtp+" adjustedCEATMIV="+adjustedCEATMIV+" adjustedCEATMGamma="+adjustedCEATMGamma+" adjustedCEATMVega="+adjustedCEATMVega+" adjustedCEATMTheta="+adjustedCEATMTheta); 
			fileLogTelegramWriter.write("adjustedPEATMLtp="+adjustedPEATMLtp+" adjustedPEATMIV="+adjustedPEATMIV+" adjustedPEATMGamma="+adjustedPEATMGamma+" adjustedPEATMVega="+adjustedPEATMVega+" adjustedPEATMTheta="+adjustedPEATMTheta);
			
			returnGreeks = new OptionGreek[]{adjustedCEReturnGreek, adjustedPEReturnGreek};
			
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
		return returnGreeks;
	}
	
	protected void print(OptionGreek optionGreekDto) {
		if (optionGreekDto!=null) {
			fileLogTelegramWriter.write( "[" + optionGreekDto.getTradingSymbol()+"@" + optionGreekDto.getLtp() + "] IV=" + optionGreekDto.getIv()+" Delta="+optionGreekDto.getDelta()+" Gamma="+optionGreekDto.getGamma()+" Vega="+optionGreekDto.getVega()+" Theta="+optionGreekDto.getTheta());
		}
	}
	
	private float getScaledValue(float lowerDelta, float upperDelta, float lowerLtp, float upperLtp, float targetValue) {
		
		float retVal =  lowerLtp + (targetValue - lowerDelta)*(upperLtp-lowerLtp)/(upperDelta-lowerDelta);
		
		return retVal;
	}
	
	private float getFutureOutstanding () {
	
		if  ( getCurrentTime().before(KiteUtil.getDailyCustomTime(getCurrentTime(), 9, 18, 1 )) ) {
			return 0f;
		}
		
		
		float retVal = 0f; 
		
		Connection conn = null;
		Statement stmt = null;
		
		try {
			conn = HDataSource.getReadOnlyConnection();
			stmt = conn.createStatement();
			
			String fetchSql = "SELECT ID, quote_time, last_traded_price, volume_traded_today FROM nexcorio_tick_data"
				+ " WHERE trading_symbol = '" + futuresTradingSymbol + "'"
				+ " AND quote_time >= '" + postgresShortDateFormat.format(getCurrentTime()) + " 09:20:01'"
				+ " AND quote_time <= '" + postgresLongDateFormat.format(getCurrentTime()) + " '"
				+ " AND id > " + lastProcessedId
				+ " ORDER BY quote_time, ID ";
		
			ResultSet rs = stmt.executeQuery(fetchSql);
			
			while(rs.next()) {
				long curId = rs.getLong("ID");
				if (curId > lastProcessedId) lastProcessedId = curId;
				
				float currentLtp = rs.getFloat("last_traded_price");
				long currentVolumeTradedToday = (long) rs.getFloat("volume_traded_today");
				Date quoteTime = rs.getTimestamp("quote_time");
				if (prev_last_traded_price>0f) { // already initialisefd
					if (currentVolumeTradedToday!=prev_volume_traded_today
							&& currentLtp != prev_last_traded_price) { // new data found
						long volume = currentVolumeTradedToday - prev_volume_traded_today;
						if (currentLtp < prev_last_traded_price ) outstandingVolume = outstandingVolume - volume;
						else if (currentLtp > prev_last_traded_price )  outstandingVolume = outstandingVolume + volume;
						//System.out.println("currentLtp="+currentLtp+" prevLtp="+prev_last_traded_price+ " currentVolumeTradedToday="+currentVolumeTradedToday+"  prev_volume_traded_today="+prev_volume_traded_today+ " volume="+volume);
						
						
						prev_last_traded_price = currentLtp;
						prev_volume_traded_today = currentVolumeTradedToday;
					}
				} else {
					prev_last_traded_price = currentLtp;
					prev_volume_traded_today = currentVolumeTradedToday;
				}
			}
			rs.close();
			stmt.close();
			retVal = outstandingVolume;
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (conn!=null) conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		return retVal;
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
			// Calculate adjusted Adjust Ltp, IV and greeks
			OptionGreek[] adjustedATMGreeks = getExactATMQuandrangle(baseDelta);
			OptionGreek adjustedATMCEGreek = adjustedATMGreeks[0];
			OptionGreek adjustedATMPEGreek = adjustedATMGreeks[1];
			
			float mlXgbPrediction = (float) (0.1364f * deltaRangeGreeksDetails.get("dr19fixedSizeAccmlCETheta") 
					+ 0.1321 * deltaRangeGreeksDetails.get("drWhlStrkAccumulatedChangein5secCeTheta")
					+ 0.1308 * deltaRangeGreeksDetails.get("dr19fixedSizeCEAvgIV")
					+ 0.1284 * deltaRangeGreeksDetails.get("dr19fixedSizePEAvgIV")
					+ 0.1281 * deltaRangeGreeksDetails.get("dr19fixedSizeAccmlPETheta")
					+ 0.1199 * deltaRangeGreeksDetails.get("drWhlStrkAccumulatedChangein5secPeTheta")
					+ 0.1146 * deltaRangeGreeksDetails.get("altAbove5WhlStrkCEAvgIv")
					+ 0.1097 * deltaRangeGreeksDetails.get("altAbove5WhlStrkPEAvgIv")
					+ 12.3456);
			
			float futureOutStanding = getFutureOutstanding();
			
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
						+ ", fullrangecetotaliv, fullrangepetotaliv, dr16CETotalIV, dr16PETotalIV, dr49CETotalIV, dr49PETotalIV, dr46CETotalIV, dr46PETotalIV, dr4PlusCETotalIV, dr4PlusPETotalIV"
						+ ", outlierCEMinIV, outlierPEMinIV, outlierCEMaxIV, outlierPEMaxIV, outlierCETotalIV, outlierPETotalIV, outlierCEAvgIV, outlierPEAvgIV, outlierCEMedianIV, outlierPEMedianIV"
						
						+ ", adjustedCEATMLtp, adjustedPEATMLtp"
						+ ", adjustedCEATMIV, adjustedPEATMIV"
						+ ", adjustedCEATMGamma, adjustedPEATMGamma"
						+ ", adjustedCEATMVega, adjustedPEATMVega"
						+ ", adjustedCEATMTheta, adjustedPEATMTheta"
						
						+ ", dr19WholeStrikeCEAvgIV, dr19WholeStrikePEAvgIV"
						+ ", totalChangeInCEIV, totalChangeInPEIV"
						+ ", minGammaExposure, maxGammaExposure, netGammaExposure"
						+ ", changein5secCeIV, changein5secPeIV"
						+ ", accumulatedChangein5secCeIV, accumulatedChangein5secPeIV"
						+ ", minGammaExposureWithStrike, maxGammaExposureWithStrike, netGammaExposureWithStrike"
						+ ", minGammaExposureTopN, maxGammaExposureTopN, netGammaExposureTopN"
						+ ", cumulativeCEAvgIVDiff, cumulativePEAvgIVDiff"
						+ ", drResAccumulatedChangein5secCeIV, drResAccumulatedChangein5secPeIV"
						
						
						+ ", dr49AccumulatedChangein5secCeGamma"
						+ ", dr49AccumulatedChangein5secPeGamma"				
						+ ", dr49AccumulatedChangein5secCeDelta"
						+ ", dr49AccumulatedChangein5secPeDelta"				
						+ ", dr49AccumulatedChangein5secCeVega"
						+ ", dr49AccumulatedChangein5secPeVega"				
						+ ", dr49AccumulatedChangein5secCeTheta"
						+ ", dr49AccumulatedChangein5secPeTheta"				
						+ ", dr49AccumulatedChangein5secCeIv"
						+ ", dr49AccumulatedChangein5secPeIv"				
						+ ", dr49AccumulatedChangein5secCeLtp"
						+ ", dr49AccumulatedChangein5secPeLtp"
						
						+ ", dr16AccumulatedChangein5secCeGamma"
						+ ", dr16AccumulatedChangein5secPeGamma"				
						+ ", dr16AccumulatedChangein5secCeDelta"
						+ ", dr16AccumulatedChangein5secPeDelta"				
						+ ", dr16AccumulatedChangein5secCeVega"
						+ ", dr16AccumulatedChangein5secPeVega"				
						+ ", dr16AccumulatedChangein5secCeTheta"
						+ ", dr16AccumulatedChangein5secPeTheta"				
						+ ", dr16AccumulatedChangein5secCeIv"
						+ ", dr16AccumulatedChangein5secPeIv"				
						+ ", dr16AccumulatedChangein5secCeLtp"
						+ ", dr16AccumulatedChangein5secPeLtp"

						+ ", drWhlStrkAccumulatedChangein5secCeGamma"
						+ ", drWhlStrkAccumulatedChangein5secPeGamma"				
						+ ", drWhlStrkAccumulatedChangein5secCeDelta"
						+ ", drWhlStrkAccumulatedChangein5secPeDelta"				
						+ ", drWhlStrkAccumulatedChangein5secCeVega"
						+ ", drWhlStrkAccumulatedChangein5secPeVega"				
						+ ", drWhlStrkAccumulatedChangein5secCeTheta"
						+ ", drWhlStrkAccumulatedChangein5secPeTheta"				
						+ ", drWhlStrkAccumulatedChangein5secCeIv"
						+ ", drWhlStrkAccumulatedChangein5secPeIv"				
						+ ", drWhlStrkAccumulatedChangein5secCeLtp"
						+ ", drWhlStrkAccumulatedChangein5secPeLtp"
						
						+ ", pt200AccumulatedChangein5secCeTheta"
						+ ", pt200AccumulatedChangein5secPeTheta"
						
						+ ", drITMWhlStrkSameSizeCEAvgIv"
						+ ", drITMWhlStrkSameSizePEAvgIv"
						
						+ ", above5WhlStrkCEAvgIv, above5WhlStrkPEAvgIv"
						
						+ ", altAbove5WhlStrkCEAvgIv, altAbove5WhlStrkPEAvgIv"
						
						+ ", altAbove5WhlStrkAccmltCETheta, altAbove5WhlStrkAccmltPETheta"
						
						+ ", otm250x750AccmlCeTheta, otm250x750AccmlPeTheta"
						+ ", itm1000x500AvgCeIv, itm1000x500AvgPeIv"
						
						+ ", dr19fixedSizeCEAvgIV, dr19fixedSizePEAvgIV"
						+ ", dr19fixedSizeAccmlCETheta, dr19fixedSizeAccmlPETheta"
						+ ", ml_xgb_prediction"
						
						+ ", future_Outstanding_Volume"
						
						+ ", range350CEAvgGamma, range350PEAvgGamma, range350CEAvgTheta, range350PEAvgTheta, range350CEAvgVega, range350PEAvgVega, range350CEAvgIv, range350PEAvgIv"
						+ ", range700CEAvgGamma, range700PEAvgGamma, range700CEAvgTheta, range700PEAvgTheta, range700CEAvgVega, range700PEAvgVega, range700CEAvgIv, range700PEAvgIv"
						
						+ ", top5OiDiff"
						
						+ ", otm250_750AccmlCeTheta, otm250_750AccmlPeTheta"
						
						+ ", lowerStrikeCEAvgIv, lowerStrikePEAvgIv"
						
						+ ", otm200_400AccmlCeTheta, otm200_400AccmlPeTheta"
						
						+ ", altAbove5WhlStrkCEAvgTimevalue, altAbove5WhlStrkPEAvgTimevalue"
						
						+", fullOtm0x600CEGreeks, fullOtm0x600PEGreeks"
						+", lowerOtm0x300CEGreeks, lowerOtm0x300PEGreeks"
						+", upperOtm300x600CEGreeks, upperOtm300x600PEGreeks"
						+", upperOtm150x300CEGreeks, upperOtm150x300PEGreeks"
						
						+", otm0x400CEGreeks, otm400x800CEGreeks, otm0x400PEGreeks, otm400x800PEGreeks"
						+", fullOtm0x500CEGreeks, fullOtm0x500PEGreeks, fullOtm50x400CEGreeks, fullOtm50x400PEGreeks"
						
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
						
						+ " ," + deltaRangeGreeksDetails.get("fullRangeCETotalIV")
						+ " ," + deltaRangeGreeksDetails.get("fullRangePETotalIV")
						
						+ " ," + deltaRangeGreeksDetails.get("dr16CETotalIV")
						+ " ," + deltaRangeGreeksDetails.get("dr16PETotalIV")
						
						+ " ," + deltaRangeGreeksDetails.get("dr49CETotalIV")
						+ " ," + deltaRangeGreeksDetails.get("dr49PETotalIV")
						
						+ " ," + deltaRangeGreeksDetails.get("dr46CETotalIV")
						+ " ," + deltaRangeGreeksDetails.get("dr46PETotalIV")
						
						+ " ," + deltaRangeGreeksDetails.get("dr4PlusCETotalIV")
						+ " ," + deltaRangeGreeksDetails.get("dr4PlusPETotalIV")
						
						+ " ," + deltaRangeGreeksDetails.get("outlierCEMinIV")
						+ " ," + deltaRangeGreeksDetails.get("outlierPEMinIV")
						
						+ " ," + deltaRangeGreeksDetails.get("outlierCEMaxIV")
						+ " ," + deltaRangeGreeksDetails.get("outlierPEMaxIV")
						
						+ " ," + deltaRangeGreeksDetails.get("outlierCETotalIV")
						+ " ," + deltaRangeGreeksDetails.get("outlierPETotalIV")
						
						+ " ," + deltaRangeGreeksDetails.get("outlierCEAvgIV")
						+ " ," + deltaRangeGreeksDetails.get("outlierPEAvgIV")
						
						+ " ," + deltaRangeGreeksDetails.get("outlierCEMedianIV")
						+ " ," + deltaRangeGreeksDetails.get("outlierPEMedianIV")
						
						+ "," + adjustedATMCEGreek.getLtp() + "," + adjustedATMPEGreek.getLtp()
						+ "," + adjustedATMCEGreek.getIv() + "," + adjustedATMPEGreek.getIv()
						+ "," + adjustedATMCEGreek.getGamma() + "," + adjustedATMPEGreek.getGamma()
						+ "," + adjustedATMCEGreek.getVega() + "," + adjustedATMPEGreek.getVega()
						+ "," + adjustedATMCEGreek.getTheta() + "," + adjustedATMPEGreek.getTheta()
						
						+ " ," + deltaRangeGreeksDetails.get("dr19WholeStrikeCEAvgIV")
						+ " ," + deltaRangeGreeksDetails.get("dr19WholeStrikePEAvgIV")
						+ " ," + deltaRangeGreeksDetails.get("totalChangeInCEIV") + " ," + deltaRangeGreeksDetails.get("totalChangeInPEIV")
						+ " ," + deltaRangeGreeksDetails.get("minGammaExposure") + " ," + deltaRangeGreeksDetails.get("maxGammaExposure") + " ," + deltaRangeGreeksDetails.get("netGammaExposure")
						+ " ," + deltaRangeGreeksDetails.get("changein5secCeIV") + "," + deltaRangeGreeksDetails.get("changein5secPeIV")
						+ " ," + deltaRangeGreeksDetails.get("accumulatedChangein5secCeIV") + "," + deltaRangeGreeksDetails.get("accumulatedChangein5secPeIV") 
						+ " ," + deltaRangeGreeksDetails.get("minGammaExposureWithStrike") + " ," + deltaRangeGreeksDetails.get("maxGammaExposureWithStrike") + " ," + deltaRangeGreeksDetails.get("netGammaExposureWithStrike")
						+ " ," + deltaRangeGreeksDetails.get("minGammaExposureTopN") + " ," + deltaRangeGreeksDetails.get("maxGammaExposureTopN") + " ," + deltaRangeGreeksDetails.get("netGammaExposureTopN")
						+ " ," + deltaRangeGreeksDetails.get("cumulativeCEAvgIVDiff") + " ," + deltaRangeGreeksDetails.get("cumulativePEAvgIVDiff") 
						+ " ," + deltaRangeGreeksDetails.get("drResAccumulatedChangein5secCeIV") + "," + deltaRangeGreeksDetails.get("drResAccumulatedChangein5secPeIV") 
						
						+ " ," + deltaRangeGreeksDetails.get("dr49AccumulatedChangein5secCeGamma")
						+ " ," + deltaRangeGreeksDetails.get("dr49AccumulatedChangein5secPeGamma")				
						+ " ," + deltaRangeGreeksDetails.get("dr49AccumulatedChangein5secCeDelta")
						+ " ," + deltaRangeGreeksDetails.get("dr49AccumulatedChangein5secPeDelta")				
						+ " ," + deltaRangeGreeksDetails.get("dr49AccumulatedChangein5secCeVega")
						+ " ," + deltaRangeGreeksDetails.get("dr49AccumulatedChangein5secPeVega")				
						+ " ," + deltaRangeGreeksDetails.get("dr49AccumulatedChangein5secCeTheta")
						+ " ," + deltaRangeGreeksDetails.get("dr49AccumulatedChangein5secPeTheta")				
						+ " ," + deltaRangeGreeksDetails.get("dr49AccumulatedChangein5secCeIv")
						+ " ," + deltaRangeGreeksDetails.get("dr49AccumulatedChangein5secPeIv")				
						+ " ," + deltaRangeGreeksDetails.get("dr49AccumulatedChangein5secCeLtp")
						+ " ," + deltaRangeGreeksDetails.get("dr49AccumulatedChangein5secPeLtp")
						
						+ " ," + deltaRangeGreeksDetails.get("dr16AccumulatedChangein5secCeGamma")
						+ " ," + deltaRangeGreeksDetails.get("dr16AccumulatedChangein5secPeGamma")				
						+ " ," + deltaRangeGreeksDetails.get("dr16AccumulatedChangein5secCeDelta")
						+ " ," + deltaRangeGreeksDetails.get("dr16AccumulatedChangein5secPeDelta")				
						+ " ," + deltaRangeGreeksDetails.get("dr16AccumulatedChangein5secCeVega")
						+ " ," + deltaRangeGreeksDetails.get("dr16AccumulatedChangein5secPeVega")				
						+ " ," + deltaRangeGreeksDetails.get("dr16AccumulatedChangein5secCeTheta")
						+ " ," + deltaRangeGreeksDetails.get("dr16AccumulatedChangein5secPeTheta")				
						+ " ," + deltaRangeGreeksDetails.get("dr16AccumulatedChangein5secCeIv")
						+ " ," + deltaRangeGreeksDetails.get("dr16AccumulatedChangein5secPeIv")				
						+ " ," + deltaRangeGreeksDetails.get("dr16AccumulatedChangein5secCeLtp")
						+ " ," + deltaRangeGreeksDetails.get("dr16AccumulatedChangein5secPeLtp")
						
						+ " ," + deltaRangeGreeksDetails.get("drWhlStrkAccumulatedChangein5secCeGamma")
						+ " ," + deltaRangeGreeksDetails.get("drWhlStrkAccumulatedChangein5secPeGamma")				
						+ " ," + deltaRangeGreeksDetails.get("drWhlStrkAccumulatedChangein5secCeDelta")
						+ " ," + deltaRangeGreeksDetails.get("drWhlStrkAccumulatedChangein5secPeDelta")				
						+ " ," + deltaRangeGreeksDetails.get("drWhlStrkAccumulatedChangein5secCeVega")
						+ " ," + deltaRangeGreeksDetails.get("drWhlStrkAccumulatedChangein5secPeVega")				
						+ " ," + deltaRangeGreeksDetails.get("drWhlStrkAccumulatedChangein5secCeTheta")
						+ " ," + deltaRangeGreeksDetails.get("drWhlStrkAccumulatedChangein5secPeTheta")				
						+ " ," + deltaRangeGreeksDetails.get("drWhlStrkAccumulatedChangein5secCeIv")
						+ " ," + deltaRangeGreeksDetails.get("drWhlStrkAccumulatedChangein5secPeIv")				
						+ " ," + deltaRangeGreeksDetails.get("drWhlStrkAccumulatedChangein5secCeLtp")
						+ " ," + deltaRangeGreeksDetails.get("drWhlStrkAccumulatedChangein5secPeLtp")
						
						+ " ," + deltaRangeGreeksDetails.get("pt200AccumulatedChangein5secCeTheta")
						+ " ," + deltaRangeGreeksDetails.get("pt200AccumulatedChangein5secPeTheta")
						
						+ " ," + deltaRangeGreeksDetails.get("drITMWhlStrkSameSizeCEAvgIv")
						+ " ," + deltaRangeGreeksDetails.get("drITMWhlStrkSameSizePEAvgIv")
						
						+ " ," + deltaRangeGreeksDetails.get("above5WhlStrkCEAvgIv")
						+ " ," + deltaRangeGreeksDetails.get("above5WhlStrkPEAvgIv")
						
						+ " ," + deltaRangeGreeksDetails.get("altAbove5WhlStrkCEAvgIv")
						+ " ," + deltaRangeGreeksDetails.get("altAbove5WhlStrkPEAvgIv")
						
						+ " ," + deltaRangeGreeksDetails.get("altAbove5WhlStrkAccmltCETheta")
						+ " ," + deltaRangeGreeksDetails.get("altAbove5WhlStrkAccmltPETheta")
						
						+ " ," + deltaRangeGreeksDetails.get("otm250x750AccmlCeTheta")
						+ " ," + deltaRangeGreeksDetails.get("otm250x750AccmlPeTheta")
						
						+ " ," + deltaRangeGreeksDetails.get("itm1000x500AvgCeIv")
						+ " ," + deltaRangeGreeksDetails.get("itm1000x500AvgPeIv")
						
						+ " ," + deltaRangeGreeksDetails.get("dr19fixedSizeCEAvgIV")
						+ " ," + deltaRangeGreeksDetails.get("dr19fixedSizePEAvgIV")
						
						+ ", " + deltaRangeGreeksDetails.get("dr19fixedSizeAccmlCETheta")
						+ ", " + deltaRangeGreeksDetails.get("dr19fixedSizeAccmlPETheta")
						
						+ ", " + mlXgbPrediction
						
						+ ", " + futureOutStanding
						
						+ ", " + deltaRangeGreeksDetails.get("range350CEAvgGamma")
						+ ", " + deltaRangeGreeksDetails.get("range350PEAvgGamma")
						
						+ ", " + deltaRangeGreeksDetails.get("range350CEAvgTheta")
						+ ", " + deltaRangeGreeksDetails.get("range350PEAvgTheta")
						
						+ ", " + deltaRangeGreeksDetails.get("range350CEAvgVega")
						+ ", " + deltaRangeGreeksDetails.get("range350PEAvgVega")
						
						+ ", " + deltaRangeGreeksDetails.get("range350CEAvgIv")
						+ ", " + deltaRangeGreeksDetails.get("range350PEAvgIv")
						
						+ ", " + deltaRangeGreeksDetails.get("range700CEAvgGamma")
						+ ", " + deltaRangeGreeksDetails.get("range700PEAvgGamma")
						
						+ ", " + deltaRangeGreeksDetails.get("range700CEAvgTheta")
						+ ", " + deltaRangeGreeksDetails.get("range700PEAvgTheta")
						
						+ ", " + deltaRangeGreeksDetails.get("range700CEAvgVega")
						+ ", " + deltaRangeGreeksDetails.get("range700PEAvgVega")
						
						+ ", " + deltaRangeGreeksDetails.get("range700CEAvgIv")
						+ ", " + deltaRangeGreeksDetails.get("range700PEAvgIv")
						
						+ " ," + deltaRangeGreeksDetails.get("top5OiDiff")
						
						+ " ," + deltaRangeGreeksDetails.get("otm250_750AccmlCeTheta")
						+ " ," + deltaRangeGreeksDetails.get("otm250_750AccmlPeTheta")
						
						+ " ," + deltaRangeGreeksDetails.get("lowerStrikeCEAvgIv")
						+ " ," + deltaRangeGreeksDetails.get("lowerStrikePEAvgIv")
						
						+ " ," + deltaRangeGreeksDetails.get("otm200_400AccmlCeTheta")
						+ " ," + deltaRangeGreeksDetails.get("otm200_400AccmlPeTheta")
						
						+ " ," + deltaRangeGreeksDetails.get("altAbove5WhlStrkCEAvgTimevalue")
						+ " ," + deltaRangeGreeksDetails.get("altAbove5WhlStrkPEAvgTimevalue")
						
						+ " ," + deltaRangeGreeksDetails.get("fullOtm0x600CEGreeks")
						+ " ," + deltaRangeGreeksDetails.get("fullOtm0x600PEGreeks")
						
						+ " ," + deltaRangeGreeksDetails.get("lowerOtm0x300CEGreeks")
						+ " ," + deltaRangeGreeksDetails.get("lowerOtm0x300PEGreeks")
						
						+ " ," + deltaRangeGreeksDetails.get("upperOtm300x600CEGreeks")
						+ " ," + deltaRangeGreeksDetails.get("upperOtm300x600PEGreeks")
						
						+ " ," + deltaRangeGreeksDetails.get("upperOtm150x300CEGreeks")
						+ " ," + deltaRangeGreeksDetails.get("upperOtm150x300PEGreeks")
						
						+ " ," + deltaRangeGreeksDetails.get("otm0x400CEGreeks")
						+ " ," + deltaRangeGreeksDetails.get("otm400x800CEGreeks")
						+ " ," + deltaRangeGreeksDetails.get("otm0x400PEGreeks")
						+ " ," + deltaRangeGreeksDetails.get("otm400x800PEGreeks")
						
						+ " ," + deltaRangeGreeksDetails.get("fullOtm0x500CEGreeks")
						+ " ," + deltaRangeGreeksDetails.get("fullOtm0x500PEGreeks")
						+ " ," + deltaRangeGreeksDetails.get("fullOtm50x400CEGreeks")
						+ " ," + deltaRangeGreeksDetails.get("fullOtm50x400PEGreeks") 
						+ ")";
				
				insertSql = insertSql.replaceAll("NaN", "0");
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
	
	public static void main(String[] args) {
		new ATMMovementAnalyzerThreadAlgoThread("HDFCBANK", "2026-06-12 09:16:00");
	}
}
