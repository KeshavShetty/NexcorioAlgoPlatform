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
				
			} while(!this.exitThread);
			
			fileLogTelegramWriter.close();
		} catch (Exception e) {			
			log.error("Error"+e.getMessage(), e);
		}
	}
	
	private void processATMMovement() {
		try {
			Map<String, Integer> futuresMap = getFutureStandOff();
			//System.out.println("futuresTradingSymbol="+futuresTradingSymbol);
			float futuresLtp = getPriceFromTicks(futuresTradingSymbol);
			
			Map<String, Float> aggregateGreeks = getAggregateGreeksDetails();
			Map<String, Float> selectiveStrikeAggregateGreeks = getSelectiveAvgGamma(5);
			
//			Map<String, Float> totaOis = getTotalOis();
//			Map<String, Float> totaIVs = getTotalIVs();
//			Map<String, Float> totaGammas = getTotalGammas();
//			Map<String, Float> totaVegas = getTotalVegas();
			
//			Map<String, OptionGreek> otmGreeksMap = processAndSaveRawStraddleData(0.4f, futuresMap.get("Total"), futuresMap.get("Bullish"), 
//					totaOis.get("TotalCEOI"), totaOis.get("TotalPEOI"), 
//					totaIVs.get("TotalCEIV"), totaIVs.get("TotalPEIV"),
//					totaGammas.get("TotalCEGamma"), totaGammas.get("TotalPEGamma"), 
//					totaVegas.get("TotalCEVega"), totaVegas.get("TotalPEVega"));
			Map<String, OptionGreek> atmGreeksMap = processAndSaveRawStraddleData(0.5f, futuresMap.get("Total"), futuresMap.get("Bullish"), 
					aggregateGreeks.get("TotalCEOI"), aggregateGreeks.get("TotalPEOI"), 
					aggregateGreeks.get("TotalCEIV"), aggregateGreeks.get("TotalPEIV"),
					aggregateGreeks.get("TotalCEGamma"), aggregateGreeks.get("TotalPEGamma"), 
					aggregateGreeks.get("TotalCEVega"), aggregateGreeks.get("TotalPEVega"),
					aggregateGreeks.get("AvgCEGamma"), aggregateGreeks.get("AvgPEGamma"),
					selectiveStrikeAggregateGreeks.get("AvgCEGamma"), selectiveStrikeAggregateGreeks.get("AvgPEGamma"), 
					futuresLtp
					);
//			Map<String, OptionGreek> itmGreeksMap = processAndSaveRawStraddleData(0.6f, futuresMap.get("Total"), futuresMap.get("Bullish"), 
//					totaOis.get("TotalCEOI"), totaOis.get("TotalPEOI"), 
//					totaIVs.get("TotalCEIV"), totaIVs.get("TotalPEIV"),
//					totaGammas.get("TotalCEGamma"), totaGammas.get("TotalPEGamma"), 
//					totaVegas.get("TotalCEVega"), totaVegas.get("TotalPEVega") );
			
			//saveAdjustedData(otmGreeksMap, atmGreeksMap, itmGreeksMap);
		} catch (Exception e) {
			log.error("Error"+e.getMessage(),e);
			e.printStackTrace();
		}
	}
	
	private void saveAdjustedData(Map<String, OptionGreek> otmGreeksMap, Map<String, OptionGreek> atmGreeksMap, Map<String, OptionGreek> itmGreeksMap) {
		
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
	
	private Map<String, Float> getSelectiveAvgGamma(int noOflegs) {
		Map<String, Float> retMap = new HashMap<>(); 
		Connection conn = null;
		try {			
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			String optionnamePrefix = getCurrentWeekExpiryOptionnamePrefix();
			
			int upperBound = (int) (this.instrumentLtp + this.mainInstrument.getGapBetweenStrikes()*noOflegs);
			int lowerBound = (int) (this.instrumentLtp - this.mainInstrument.getGapBetweenStrikes()*noOflegs);
			
			String 
			fetchSql = "select avg(gamma) as avgGamma from nexcorio_option_snapshot where record_date = '" + postgresShortDateFormat.format(getCurrentTime())+ "' and trading_symbol LIKE '" + optionnamePrefix + "%CE'"
					+ " AND strike <= " + upperBound
					+ " AND strike >= " + lowerBound;
			fileLogTelegramWriter.write(fetchSql);
			
			ResultSet rs = stmt.executeQuery(fetchSql);
			
			float avgCEGamma = 0f;
			
			while (rs.next()) {
				avgCEGamma = rs.getFloat("avgGamma");
			}
			rs.close();
			
			retMap.put("AvgCEGamma", avgCEGamma);
			
			fetchSql = "select avg(gamma) as avgGamma from nexcorio_option_snapshot where record_date = '" + postgresShortDateFormat.format(getCurrentTime())+ "' and trading_symbol LIKE '" + optionnamePrefix + "%PE'"
					+ " AND strike <= " + upperBound
					+ " AND strike >= " + lowerBound;
			
			fileLogTelegramWriter.write(fetchSql);
			
			rs = stmt.executeQuery(fetchSql);
			
			float avgPEGamma = 0f;
			
			while (rs.next()) {
				avgPEGamma = rs.getFloat("avgGamma");
			}
			rs.close();
			
			retMap.put("AvgPEGamma", avgPEGamma);
			
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
	
	private Map<String, Float> getTotalOis() {
		Map<String, Float> retMap = new HashMap<>();
		Connection conn = null;
		try {			
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			String optionnamePrefix = getCurrentWeekExpiryOptionnamePrefix();
			
			String fetchSql = "select sum(oi) as totalOI from nexcorio_option_snapshot where record_date = '" + postgresShortDateFormat.format(getCurrentTime())+ "' and trading_symbol LIKE '" + optionnamePrefix + "%CE'";
			fileLogTelegramWriter.write(fetchSql);
			
			ResultSet rs = stmt.executeQuery(fetchSql);
			
			float totalCEOI = 0f;
			while (rs.next()) {
				totalCEOI = rs.getFloat("totalOI");
			}
			rs.close();
			retMap.put("TotalCEOI", totalCEOI);
			
			fetchSql = "select sum(oi) as totalOI from nexcorio_option_snapshot where record_date = '" + postgresShortDateFormat.format(getCurrentTime())+ "' and trading_symbol LIKE '" + optionnamePrefix + "%PE'";
			fileLogTelegramWriter.write(fetchSql);
			
			rs = stmt.executeQuery(fetchSql);
			
			float totalPEOI = 0f;
			while (rs.next()) {
				totalPEOI = rs.getFloat("totalOI");
			}
			rs.close();
			retMap.put("TotalPEOI", totalPEOI);
			
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
	
	private Map<String, Float> getTotalIVs() {
		Map<String, Float> retMap = new HashMap<>();
		Connection conn = null;
		try {			
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			String optionnamePrefix = getCurrentWeekExpiryOptionnamePrefix();
			
			String fetchSql = "select sum(iv)/count(*) as totalIV from nexcorio_option_snapshot where record_date = '" + postgresShortDateFormat.format(getCurrentTime())+ "' and trading_symbol LIKE '" + optionnamePrefix + "%CE'";
			fileLogTelegramWriter.write(fetchSql);
			
			ResultSet rs = stmt.executeQuery(fetchSql);
			
			float totalCEIV = 0f;
			while (rs.next()) {
				totalCEIV = rs.getFloat("totalIV");
			}
			rs.close();
			retMap.put("TotalCEIV", totalCEIV);
			
			fetchSql = "select sum(iv)/count(*) as totalIV from nexcorio_option_snapshot where record_date = '" + postgresShortDateFormat.format(getCurrentTime())+ "' and trading_symbol LIKE '" + optionnamePrefix + "%PE'";
			fileLogTelegramWriter.write(fetchSql);
			
			rs = stmt.executeQuery(fetchSql);
			
			float totalPEIV = 0f;
			while (rs.next()) {
				totalPEIV = rs.getFloat("totalIV");
			}
			rs.close();
			retMap.put("TotalPEIV", totalPEIV);
			
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
	
	private Map<String, Float> getTotalGammas() {
		Map<String, Float> retMap = new HashMap<>();
		Connection conn = null;
		try {			
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			String optionnamePrefix = getCurrentWeekExpiryOptionnamePrefix();
			
			String fetchSql = "select sum(gamma) as totalGamma, avg(gamma) as avgGamma from nexcorio_option_snapshot where record_date = '" + postgresShortDateFormat.format(getCurrentTime())+ "' and trading_symbol LIKE '" + optionnamePrefix + "%CE'";
			fileLogTelegramWriter.write(fetchSql);
			
			ResultSet rs = stmt.executeQuery(fetchSql);
			
			float totalCEGamma = 0f;
			float avgCEGamma = 0f;
			while (rs.next()) {
				totalCEGamma = rs.getFloat("totalGamma");
				avgCEGamma = rs.getFloat("avgGamma");
			}
			rs.close();
			retMap.put("TotalCEGamma", totalCEGamma);
			retMap.put("AvgCEGamma", avgCEGamma);
			
			fetchSql = "select sum(gamma) as totalGamma, avg(gamma) as avgGamma from nexcorio_option_snapshot where record_date = '" + postgresShortDateFormat.format(getCurrentTime())+ "' and trading_symbol LIKE '" + optionnamePrefix + "%PE'";
			fileLogTelegramWriter.write(fetchSql);
			
			rs = stmt.executeQuery(fetchSql);
			
			float totalPEGamma = 0f;
			float avgPEGamma = 0f;
			while (rs.next()) {
				totalPEGamma = rs.getFloat("totalGamma");
				avgPEGamma = rs.getFloat("avgGamma");
			}
			rs.close();
			retMap.put("TotalPEGamma", totalPEGamma);
			retMap.put("AvgPEGamma", avgPEGamma);
			
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
	
	private Map<String, Float> getTotalVegas() {
		Map<String, Float> retMap = new HashMap<>();
		Connection conn = null;
		try {			
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			String optionnamePrefix = getCurrentWeekExpiryOptionnamePrefix();
			
			String fetchSql = "select sum(vega) as totalVega from nexcorio_option_snapshot where record_date = '" + postgresShortDateFormat.format(getCurrentTime())+ "' and trading_symbol LIKE '" + optionnamePrefix + "%CE'";
			fileLogTelegramWriter.write(fetchSql);
			
			ResultSet rs = stmt.executeQuery(fetchSql);
			
			float totalCEVega = 0f;
			while (rs.next()) {
				totalCEVega = rs.getFloat("totalVega");
			}
			rs.close();
			retMap.put("TotalCEVega", totalCEVega);
			
			fetchSql = "select sum(vega) as totalVega from nexcorio_option_snapshot where record_date = '" + postgresShortDateFormat.format(getCurrentTime())+ "' and trading_symbol LIKE '" + optionnamePrefix + "%PE'";
			fileLogTelegramWriter.write(fetchSql);
			
			rs = stmt.executeQuery(fetchSql);
			
			float totalPEVega = 0f;
			while (rs.next()) {
				totalPEVega = rs.getFloat("totalVega");
			}
			rs.close();
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
	private Map<String, OptionGreek> processAndSaveRawStraddleData(float baseDelta, Integer futuresTotalPoint, Integer futuresBullishPoint, float totalCEOI, float totalPEOI, float totalCEIV, float totalPEIV,
			float totalCEGamma, float totalPEGamma,
			float totalCEVega, float totalPEVega,
			float avgCeGamma, float avgPeGamma,
			float selectiveStrikeAvgCeGamma, float selectiveStrikeAvgPeGamma,
			float futuresLtp) {
		Map<String, OptionGreek> retMap = null;
		
		Connection conn = null;
		try {			
			conn = HDataSource.getConnection();
			Statement stmt = conn.createStatement();
			
			String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(baseDelta, 0); // Hedge distance 0
			
			OptionGreek ceOptionGreek = getOptionGreeks(entryStraddleOptionNames[0]);
			OptionGreek peOptionGreek = getOptionGreeks(entryStraddleOptionNames[1]);
			
			if (ceOptionGreek!=null && peOptionGreek!=null) {
				String insertSql = "INSERT INTO nexcorio_option_atm_movement_data (id, f_main_instrument, instrumentltp, base_delta, record_time, ceOptionname, peOptionname"
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
						+ ", futures_Ltp"

						+ ")" 
						+ " VALUES (nextval('nexcorio_option_atm_movement_data_id_seq')," + this.mainInstrument.getId()+ "," + this.instrumentLtp +"," + baseDelta +""
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
						+ " ," + futuresLtp
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
		new ATMMovementAnalyzerThreadAlgoThread("NIFTY", "2025-06-18 09:17:00");		
	}

}
