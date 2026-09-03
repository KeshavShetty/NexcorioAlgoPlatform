package com.nexcorio.algo.analytics;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.kite.CentralCacheHandler;
import com.nexcorio.algo.util.FileLogTelegramWriter;
import com.nexcorio.algo.util.KiteUtil;
import com.nexcorio.algo.util.db.HDataSource;

public class V2GreeksMovementAnalyzerThread extends AnalyticsBaseClass implements Runnable {
	
	private static final Logger log = LogManager.getLogger(V2GreeksMovementAnalyzerThread.class);
	
	private float drOTMAccumulatedChangein5secCeTheta = 0f;
	private float drOTMAccumulatedChangein5secPeTheta = 0f;
	
	private float drITMAccumulatedChangein5secCeTheta = 0f;
	private float drITMAccumulatedChangein5secPeTheta = 0f;
	
	private List<OptionGreek> prevCeOptionGreeks = new ArrayList<OptionGreek>();
	private List<OptionGreek> prevPeOptionGreeks = new ArrayList<OptionGreek>();
	
	String futuresTradingSymbol;
	
	float prev_last_traded_price = -1f;
	long prev_volume_traded_today = 0l;
	long lastProcessedId = 0l;
	float outstandingVolume = 0f;
	
	public V2GreeksMovementAnalyzerThread(String instrumentName, String backDateStr) {
		super();
		
		this.mainInstrument = CentralCacheHandler.getTradingSymbolMainInstrumentCache(instrumentName);
		
		this.algoname=this.mainInstrument.getShortName() + "V2GreeksMovementAnalyzer";
		
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
				log.debug("Too early for V2GreeksMovementAnalyzer, going to sleep for 30 seconds");
				System.out.println("Too early for V2GreeksMovementAnalyzer, going to sleep for 30 seconds");
				sleep(30);
			}
			System.out.println("Time has come to process V2GreeksMovementAnalyzer");
			futuresTradingSymbol = getNextNFUTUREExpiryDatePrefix(this.mainInstrument.getId(), this.mainInstrument.getExchange());
			do {
				sleep(4);
				fileLogTelegramWriter.write("====================================================================================================");
				this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
				fileLogTelegramWriter.write("instrumentLtp="+instrumentLtp);
				analyseGreeksMovement();
					
				if (timeout(15, 39, 0)) {
					prepareExit(" Exiting: Timeout");
				}
				//prepareExit(" Exiting: Timeout");
			} while(!this.exitThread);
			fileLogTelegramWriter.close();
		} catch (Exception e) {			
			log.error("Error"+e.getMessage(), e);
		}
	}
	
	private void analyseGreeksMovement() {
		try {
			Map<String, Float> ratioMap = new HashMap<>();
			
			StringBuffer logStr = new StringBuffer();
			Long beginTime = System.currentTimeMillis();
			Long startTime = System.currentTimeMillis();
			
			Long elapsedTime1 = System.currentTimeMillis();
			
			float futuresLtp = getPriceFromTicks(futuresTradingSymbol);
			
			ratioMap.put("instrumentLtp", instrumentLtp);
			ratioMap.put("futuresLtp", futuresLtp);
			
			elapsedTime1 = System.currentTimeMillis();
			logStr.append(", Time taken for getPriceFromTicks=" +(elapsedTime1-startTime));
			startTime = elapsedTime1;
			
			List<OptionGreek> allOptionGreeks = getOptionGreeks();
			
			elapsedTime1 = System.currentTimeMillis();
			logStr.append(", Time taken for getOptionGreeks=" +(elapsedTime1-startTime));
			startTime = elapsedTime1;
			
			List<OptionGreek> ceOptionGreeks = new ArrayList<>();
			List<OptionGreek> peOptionGreeks = new ArrayList<>();
			
			for(OptionGreek aGreek : allOptionGreeks ) {
				if (aGreek!=null ) { // && aGreek.getStrike()%100==0
					if (aGreek.getTradingSymbol().endsWith("CE")) {
						ceOptionGreeks.add(aGreek);
					} else {
						peOptionGreeks.add(aGreek);
					}
				}
			}
			
			Collections.sort(ceOptionGreeks, new SortbyStrike());
			Collections.reverse(ceOptionGreeks);
			Collections.sort(peOptionGreeks, new SortbyStrike());
			
			float lastReadIv = -1f;
			int lowerStrike = 0;
			OptionGreek prevGreek = null;
			for(OptionGreek aGreek: ceOptionGreeks) {
				float delta = Math.abs(aGreek.getDelta());
				if (delta > 0.5f && delta < 0.9f) {
					if (lastReadIv < 0f) {
						lowerStrike = aGreek.getStrike();
					} else {
						if (Math.abs(prevGreek.getDelta()) < delta) {
							if (Math.abs(lastReadIv-aGreek.getIv()) < 5f) {
								lowerStrike = aGreek.getStrike();
							}
						} else {
							break;
						}
					}
					lastReadIv = aGreek.getIv();
				}
				prevGreek = aGreek;
			}
			
			lastReadIv = -1f;
			int upperStrike = 0;
			prevGreek = null;
			for(OptionGreek aGreek: peOptionGreeks) {
				float delta = Math.abs(aGreek.getDelta());
				if (delta > 0.5f && delta < 0.9f) {
					if (lastReadIv < 0f) {
						upperStrike = aGreek.getStrike();
						//System.out.println("Processing "+aGreek.getStrike()+" Delta="+aGreek.getDelta()+" iv="+aGreek.getIv());
					} else {
						if (Math.abs(prevGreek.getDelta()) < delta) {
							if (Math.abs(lastReadIv-aGreek.getIv()) < 5f) {
								//System.out.println("Processing "+aGreek.getStrike()+" Delta="+aGreek.getDelta()+" iv="+aGreek.getIv());
								upperStrike = aGreek.getStrike();
							}
						} else {
							break;
						}
					}
					lastReadIv = aGreek.getIv();
				}
				prevGreek = aGreek;
			}
			//System.out.println("lowerStrike="+lowerStrike+" upperStrike="+upperStrike);

			List<OptionGreek> selectedAllCEGreeks = new ArrayList<OptionGreek>();
			List<OptionGreek> selectedAllPEGreeks = new ArrayList<OptionGreek>();
			
			List<OptionGreek> outlierCEGreeks = new ArrayList<OptionGreek>();
			List<OptionGreek> outlierPEGreeks = new ArrayList<OptionGreek>();
			
			List<OptionGreek> limitedITMCEGreeks = new ArrayList<OptionGreek>();
			List<OptionGreek> limitedITMPEGreeks = new ArrayList<OptionGreek>();
			
			List<OptionGreek> limitedOTMCEGreeks = new ArrayList<OptionGreek>();
			List<OptionGreek> limitedOTMPEGreeks = new ArrayList<OptionGreek>();
			
			
			
			for(OptionGreek aGreek: ceOptionGreeks) {
				if (aGreek.getStrike() >= lowerStrike && aGreek.getStrike() <= upperStrike) {
					selectedAllCEGreeks.add(aGreek);
					//System.out.println("Adding "+aGreek.getTradingSymbol() + " to selectedAllCEGreeks");
					if (Math.abs(aGreek.getDelta()) > 0.5f) {
						if (limitedITMCEGreeks.size() < 4) {
							//System.out.println("Adding "+aGreek.getTradingSymbol() + " to limitedITMCEGreeks");
							limitedITMCEGreeks.add(aGreek);
						}
					}	
				} else {
					if (Math.abs(aGreek.getDelta()) > 0.5f && Math.abs(aGreek.getDelta()) < 0.9f ) {
						//System.out.println("Adding "+aGreek.getTradingSymbol() + " to outlierCEGreeks" + " Delta="+ aGreek.getDelta() );
						outlierCEGreeks.add(aGreek);
					}
				}
			}
			
			for(OptionGreek aGreek: peOptionGreeks) {
				if (aGreek.getStrike() >= lowerStrike && aGreek.getStrike() <= upperStrike) {
					selectedAllPEGreeks.add(aGreek);
					//System.out.println("Adding "+aGreek.getTradingSymbol() + " to selectedAllPEGreeks");
					if (Math.abs(aGreek.getDelta()) > 0.5f) {
						if (limitedITMPEGreeks.size() < 4) {
							//System.out.println("Adding "+aGreek.getTradingSymbol() + " to limitedITMPEGreeks");
							limitedITMPEGreeks.add(aGreek);
						}
					}
				} else {
					if (Math.abs(aGreek.getDelta()) > 0.5f && Math.abs(aGreek.getDelta()) < 0.9f) {
						//System.out.println("Adding "+aGreek.getTradingSymbol() + " to outlierPEGreeks" + " Delta="+ aGreek.getDelta());
						outlierPEGreeks.add(aGreek);
					}
				}
			}
			
			// OTM
			Collections.reverse(ceOptionGreeks);
			Collections.reverse(peOptionGreeks);
			
			for(OptionGreek aGreek: ceOptionGreeks) {
				if (aGreek.getStrike() >= lowerStrike && aGreek.getStrike() <= upperStrike) {
					float delta = Math.abs(aGreek.getDelta());
					if (delta < 0.5f) {
						if (limitedOTMCEGreeks.size() < 4) {
							//System.out.println("Adding "+aGreek.getTradingSymbol() + " to limitedOTMCEGreeks");
							limitedOTMCEGreeks.add(aGreek);
						} else break;
					}
				}
			}
			for(OptionGreek aGreek: peOptionGreeks) {
				if (aGreek.getStrike() >= lowerStrike && aGreek.getStrike() <= upperStrike) {
					float delta = Math.abs(aGreek.getDelta());
					if (delta < 0.5f) {
						if (limitedOTMPEGreeks.size() < 4) {
							//System.out.println("Adding "+aGreek.getTradingSymbol() + " to limitedOTMPEGreeks");
							limitedOTMPEGreeks.add(aGreek);
						} else break;
					}
				}
			}
			elapsedTime1 = System.currentTimeMillis();
			logStr.append(", Time taken for getDeltaRangeGreeksDetails=" +(elapsedTime1-startTime));
			startTime = elapsedTime1;
			
			ratioMap.put("selectedAllCEGreeksAvgIv", (float) selectedAllCEGreeks.stream().mapToDouble(d -> d.getIv()).average().orElse(0.0));
			ratioMap.put("selectedAllPEGreeksAvgIv", (float) selectedAllPEGreeks.stream().mapToDouble(d -> d.getIv()).average().orElse(0.0));
			
			ratioMap.put("lowerStrike",(float) lowerStrike);
			ratioMap.put("upperStrike",(float) upperStrike);
			ratioMap.put("limitedITMCEGreeks", (float) limitedITMCEGreeks.stream().mapToDouble(d -> d.getIv()).average().orElse(0.0));
			ratioMap.put("limitedITMPEGreeks", (float) limitedITMPEGreeks.stream().mapToDouble(d -> d.getIv()).average().orElse(0.0));
			ratioMap.put("limitedOTMCEGreeks", (float) limitedOTMCEGreeks.stream().mapToDouble(d -> d.getIv()).average().orElse(0.0));
			ratioMap.put("limitedOTMPEGreeks", (float) limitedOTMPEGreeks.stream().mapToDouble(d -> d.getIv()).average().orElse(0.0));
			
			float dr16Changein5secCeTheta = 0f; 
			float dr16Changein5secPeTheta = 0f; 
			
			float drITMChangein5secCeTheta = 0f; 
			float drITMChangein5secPeTheta = 0f; 
			
			StringBuffer logMsg = new StringBuffer();
			for(OptionGreek aGreek: limitedOTMCEGreeks) {
				for(OptionGreek prevGreeks: prevCeOptionGreeks) {
					if (aGreek.getTradingSymbol().equals(prevGreeks.getTradingSymbol())) {
						logMsg = logMsg.append(" " +  aGreek.getId() + ":" + aGreek.getTradingSymbol()+":" + aGreek.getDelta());
						dr16Changein5secCeTheta = dr16Changein5secCeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevGreeks.getTheta()));
					}
				}
			}
			for(OptionGreek aGreek: limitedOTMPEGreeks) {
				for(OptionGreek prevGreeks: prevPeOptionGreeks) {
					if (aGreek.getTradingSymbol().equals(prevGreeks.getTradingSymbol())) {
						logMsg = logMsg.append(" " +  aGreek.getId() + ":" + aGreek.getTradingSymbol()+":" + aGreek.getDelta());
						dr16Changein5secPeTheta = dr16Changein5secPeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevGreeks.getTheta()));
					}
				}
			}
			// ITM
			for(OptionGreek aGreek: limitedITMCEGreeks) {
				for(OptionGreek prevGreeks: prevCeOptionGreeks) {
					if (aGreek.getTradingSymbol().equals(prevGreeks.getTradingSymbol())) {
						drITMChangein5secCeTheta = drITMChangein5secCeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevGreeks.getTheta()));
					}
				}
			}
			for(OptionGreek aGreek: limitedITMPEGreeks) {
				for(OptionGreek prevGreeks: prevPeOptionGreeks) {
					if (aGreek.getTradingSymbol().equals(prevGreeks.getTradingSymbol())) {
						drITMChangein5secPeTheta = drITMChangein5secPeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevGreeks.getTheta()));
					}
				}
			}
			
			drOTMAccumulatedChangein5secCeTheta = drOTMAccumulatedChangein5secCeTheta + dr16Changein5secCeTheta;
			drOTMAccumulatedChangein5secPeTheta = drOTMAccumulatedChangein5secPeTheta + dr16Changein5secPeTheta;
			
			drITMAccumulatedChangein5secCeTheta = drITMAccumulatedChangein5secCeTheta + drITMChangein5secCeTheta;
			drITMAccumulatedChangein5secPeTheta = drITMAccumulatedChangein5secPeTheta + drITMChangein5secPeTheta;
			
			fileLogTelegramWriter.write("Selected Limited OTM=" + logMsg.toString() +" drOTMAccumulatedChangein5secCeTheta="+drOTMAccumulatedChangein5secCeTheta+" drOTMAccumulatedChangein5secPeTheta="+drOTMAccumulatedChangein5secPeTheta);
			
			ratioMap.put("drOTMAccumulatedChangein5secCeTheta",(float) drOTMAccumulatedChangein5secCeTheta);
			ratioMap.put("drOTMAccumulatedChangein5secPeTheta",(float) drOTMAccumulatedChangein5secPeTheta);
			
			ratioMap.put("drITMAccumulatedChangein5secCeTheta",(float) drITMAccumulatedChangein5secCeTheta);
			ratioMap.put("drITMAccumulatedChangein5secPeTheta",(float) drITMAccumulatedChangein5secPeTheta);
			
			prevCeOptionGreeks = ceOptionGreeks;
			prevPeOptionGreeks = peOptionGreeks;
			
			elapsedTime1 = System.currentTimeMillis();
			logStr.append(", Time taken for processGreeks=" +(elapsedTime1-startTime));
			startTime = elapsedTime1;
			
			String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(0.5f, 0); // Hedge distance 0
			String ceOptionName = entryStraddleOptionNames[0];
			String peOptionName = entryStraddleOptionNames[1];
			
			elapsedTime1 = System.currentTimeMillis();
			logStr.append(", Time taken for ATM data 1=" +(elapsedTime1-startTime));
			startTime = elapsedTime1;
			
			OptionGreek ceOptionGreek = getOptionGreeks(ceOptionName);
			OptionGreek peOptionGreek = getOptionGreeks(peOptionName);
			
			//System.out.println(ceOptionGreek.getId() + "ceOptionGreek "+ceOptionGreek.getTradingSymbol() + " & " +peOptionGreek.getId() + "peOptionGreek "+peOptionGreek.getTradingSymbol());
			
//			ratioMap.put("atmCeOptionGreekId", (float) ceOptionGreek.getId());
//			ratioMap.put("atmPeOptionGreekId", (float) peOptionGreek.getId());
			
			elapsedTime1 = System.currentTimeMillis();
			logStr.append(", Time taken for ATM data=" +(elapsedTime1-startTime));
			startTime = elapsedTime1;
			
			processAndSaveRawStraddleData(ratioMap, ceOptionGreek.getId(), peOptionGreek.getId(), outlierCEGreeks.size(), outlierPEGreeks.size());
			
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
	
	private List<OptionGreek> getOptionGreeks() {
		List<OptionGreek> retList = new ArrayList<OptionGreek>();
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			List<OptionGreek> allOptionGreeks = new ArrayList<OptionGreek>();
			
			if (this.backtestDate == null) { // Live
				retList = getSnapshotGreeksFromCache();
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
				retList = getOptionGreeks(optionnames, 0);
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
		return retList;
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
	
	private void processAndSaveRawStraddleData(Map<String, Float> ratioMap, Long ceOptionGreekId, Long peOptionGreekId, int outlierCEGreeksCount, int outlierPEGreeksCount) {
		Connection conn = null;
		try {			
			conn = HDataSource.getConnection();
			Statement stmt = conn.createStatement();
			
			//System.out.println("ceOptionGreekId=" + ceOptionGreekId + " peOptionGreekId=" + peOptionGreekId);
			
			String insertSql = "INSERT INTO nexcorio_option_greek_movement_data (f_main_instrument, instrumentltp, record_time"
					
					+ ", futures_ltp"
					+ ", atm_ce_optiongreek_id, atm_pe_optiongreek_id"
					+ ", selectedAllCEGreeksAvgIv, selectedAllPEGreeksAvgIv"
					+ ", lowerStrike, upperStrike"
					+ ", limitedITMCEGreeks, limitedITMPEGreeks"
					+ ", limitedOTMCEGreeks, limitedOTMPEGreeks"
					+ ", outlierCEGreeks,outlierPEGreeks"
					+ ", drOTMAccumulatedChangein5secCeTheta, drOTMAccumulatedChangein5secPeTheta"
					+ ", drITMAccumulatedChangein5secCeTheta, drITMAccumulatedChangein5secPeTheta"
					+ ")" 
					
					+ " VALUES (" + this.mainInstrument.getId()+ "," + this.instrumentLtp + ",'" + postgresLongDateFormat.format(getCurrentTime()) + "'"
					+ "," + ratioMap.get("futuresLtp") 
					+ "," + ceOptionGreekId
					+ "," + peOptionGreekId
					+ "," + ratioMap.get("selectedAllCEGreeksAvgIv")
					+ "," + ratioMap.get("selectedAllPEGreeksAvgIv")
					+ "," + ratioMap.get("lowerStrike")
					+ "," + ratioMap.get("upperStrike")
					+ "," + ratioMap.get("limitedITMCEGreeks")
					+ "," + ratioMap.get("limitedITMPEGreeks")
					+ "," + ratioMap.get("limitedOTMCEGreeks")
					+ "," + ratioMap.get("limitedOTMPEGreeks")
					
					+ "," + outlierCEGreeksCount
					+ "," + outlierPEGreeksCount
					
					+ "," + ratioMap.get("drOTMAccumulatedChangein5secCeTheta")
					+ "," + ratioMap.get("drOTMAccumulatedChangein5secPeTheta")
					
					+ "," + ratioMap.get("drITMAccumulatedChangein5secCeTheta")
					+ "," + ratioMap.get("drITMAccumulatedChangein5secPeTheta")
					
					+ ")";
			//System.out.println("insertSql="+insertSql);
			insertSql = insertSql.replaceAll("NaN", "0");
			fileLogTelegramWriter.write(insertSql);
			
			stmt.executeUpdate(insertSql);
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
		new V2GreeksMovementAnalyzerThread("NIFTY", "2026-09-02 09:16:00");
//		new V2GreeksMovementAnalyzerThread("NIFTY", "2026-08-31 09:16:00");
//		new V2GreeksMovementAnalyzerThread("NIFTY", "2026-09-01 09:16:00");
		
	}
}
