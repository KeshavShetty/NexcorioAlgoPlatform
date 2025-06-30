package com.nexcorio.algo.analytics;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

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
				log.info("Too early for ATM data, going to sleep for 30 seconds");
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
			
			Map<String, Integer> futuresMap = getFutureStandOff();
			Long elapsedTime1 = System.currentTimeMillis();
			logStr.append(", Time taken for getFutureStandOff=" +(elapsedTime1-startTime));
			startTime = elapsedTime1;
			
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
			
			Map<String, OptionGreek> atmGreeksMap = processAndSaveRawStraddleData(0.5f, futuresMap.get("Total"), futuresMap.get("Bullish"), 
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
		return retMap;
	}
	
	private Map<String, Float> getSelectiveGreeksDetails(int noOflegs) {
		Map<String, Float> retMap = new HashMap<>(); 
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
			fetchSql = "select ltp, oi, iv, delta, vega, gamma from nexcorio_option_snapshot where record_date = '" + postgresShortDateFormat.format(getCurrentTime())+ "'"
					+ " AND trading_symbol LIKE '" + optionnamePrefix + "%CE'"
					+ " AND delta >= " + lowerDelta
					+ " AND delta <= " + upperDelta
					+ " ORDER BY iv";
			fileLogTelegramWriter.write(fetchSql);
			
			ResultSet rs = stmt.executeQuery(fetchSql);
			
			float lastIvRead = 0f;
			int recCount = 0;
			float deltaRangeCEAvgLtp = 0f;
			float deltaRangeCEAvgIv = 0f;
			float deltaRangeCEAvgDelta = 0f;
			float deltaRangeCEAvgGamma = 0f;
			float deltaRangeCEAvgVega = 0f;
			
			while (rs.next()) {	
				float curIv = rs.getFloat("iv");
				if (lastIvRead<0.1f || curIv < lastIvRead +5f) {
					//System.out.println("Include "+curIv);
					deltaRangeCEAvgLtp = deltaRangeCEAvgLtp + rs.getFloat("ltp");
					deltaRangeCEAvgIv = deltaRangeCEAvgIv + curIv;
					deltaRangeCEAvgDelta = deltaRangeCEAvgDelta + Math.abs(rs.getFloat("delta"));
					deltaRangeCEAvgGamma = deltaRangeCEAvgGamma + rs.getFloat("gamma");
					deltaRangeCEAvgVega = deltaRangeCEAvgVega + rs.getFloat("vega");
					
					lastIvRead = curIv; 
					recCount++;
				} 
			}
			rs.close();
			System.out.println("CE recCount "+recCount);
			
			deltaRangeCEAvgLtp = deltaRangeCEAvgLtp/(float)recCount;
			deltaRangeCEAvgIv  = deltaRangeCEAvgIv/(float)recCount;
			deltaRangeCEAvgDelta =  deltaRangeCEAvgDelta/(float)recCount;
			deltaRangeCEAvgGamma = deltaRangeCEAvgGamma/(float)recCount;
			deltaRangeCEAvgVega = deltaRangeCEAvgVega/(float)recCount;
			
			retMap.put("deltaRangeCEAvgLtp", deltaRangeCEAvgLtp);
			retMap.put("deltaRangeCEAvgIv", deltaRangeCEAvgIv);
			retMap.put("deltaRangeCEAvgDelta", deltaRangeCEAvgDelta);
			retMap.put("deltaRangeCEAvgGamma", deltaRangeCEAvgGamma);
			retMap.put("deltaRangeCEAvgVega", deltaRangeCEAvgVega);
			
			lowerDelta = -0.9f;
			upperDelta = -0.1f;
			
			fetchSql = "select ltp, oi, iv, delta, vega, gamma from nexcorio_option_snapshot where record_date = '" + postgresShortDateFormat.format(getCurrentTime())+ "'"
					+ " AND trading_symbol LIKE '" + optionnamePrefix + "%PE'"
					+ " AND delta >= " + lowerDelta
					+ " AND delta <= " + upperDelta
					+ " ORDER BY iv";
			
			fileLogTelegramWriter.write(fetchSql);
			
			rs = stmt.executeQuery(fetchSql);
			
			lastIvRead = 0f;
			recCount = 0;
			float deltaRangePEAvgLtp = 0f;
			float deltaRangePEAvgIv = 0f;
			float deltaRangePEAvgDelta = 0f;
			float deltaRangePEAvgGamma = 0f;
			float deltaRangePEAvgVega = 0f;
			
			while (rs.next()) {	
				float curIv = rs.getFloat("iv");
				if (lastIvRead<0.1f || curIv < lastIvRead +5f) {
					//System.out.println("Include "+curIv);
					deltaRangePEAvgLtp = deltaRangePEAvgLtp + rs.getFloat("ltp");
					deltaRangePEAvgIv = deltaRangePEAvgIv + curIv;
					deltaRangePEAvgDelta = deltaRangePEAvgDelta + Math.abs(rs.getFloat("delta"));
					deltaRangePEAvgGamma = deltaRangePEAvgGamma + rs.getFloat("gamma");
					deltaRangePEAvgVega = deltaRangePEAvgVega + rs.getFloat("vega");
					
					lastIvRead = curIv; 
					recCount++;
				}
			}
			rs.close();
			System.out.println("PE recCount "+recCount);
			
			deltaRangePEAvgLtp = deltaRangePEAvgLtp/(float)recCount;
			deltaRangePEAvgIv  = deltaRangePEAvgIv/(float)recCount;
			deltaRangePEAvgDelta =  deltaRangePEAvgDelta/(float)recCount;
			deltaRangePEAvgGamma = deltaRangePEAvgGamma/(float)recCount;
			deltaRangePEAvgVega = deltaRangePEAvgVega/(float)recCount;
			
			retMap.put("deltaRangePEAvgLtp", deltaRangePEAvgLtp);
			retMap.put("deltaRangePEAvgIv", deltaRangePEAvgIv);
			retMap.put("deltaRangePEAvgDelta", deltaRangePEAvgDelta);
			retMap.put("deltaRangePEAvgGamma", deltaRangePEAvgGamma);
			retMap.put("deltaRangePEAvgVega", deltaRangePEAvgVega);
			
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
	
	private Map<String, OptionGreek> processAndSaveRawStraddleData(float baseDelta, Integer futuresTotalPoint, Integer futuresBullishPoint, float totalCEOI, float totalPEOI, float totalCEIV, float totalPEIV,
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
			
			OptionGreek ceOptionGreek = getOptionGreeks(entryStraddleOptionNames[0]);
			OptionGreek peOptionGreek = getOptionGreeks(entryStraddleOptionNames[1]);
			
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
						+ ", totalFuturePoints"
						+ ", bullishFuturePoints"
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

						+ ")" 
						+ " VALUES (nextval('nexcorio_option_atm_movement_data_id_seq')," + this.mainInstrument.getId()+ "," + this.instrumentLtp 
						+ ",'" + postgresLongDateFormat.format(getCurrentTime()) + "'"
						+ ",'" + entryStraddleOptionNames[0] + "'"
						+ ",'" + entryStraddleOptionNames[1] + "'"
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
						+ " ," + futuresTotalPoint
						+ " ," + futuresBullishPoint
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
						
						+ ")";
				log.info(insertSql);
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
		new ATMMovementAnalyzerThreadAlgoThread("NIFTY", "2025-06-27 09:17:00");		
	}

}
