package com.nexcorio.algo.junk;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.ibm.icu.util.Calendar;
import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.util.db.HDataSource;

public class OIVolumeChangeProcessorMain {

	private String forDate ;
	private String indShortPrefix ; 
	private long mainInstrumentId ; 
	
	public OIVolumeChangeProcessorMain(String forDate, String indShortPrefix, long mainInstrumentId) {
		super();
		this.forDate = forDate;
		this.indShortPrefix = indShortPrefix;
		this.mainInstrumentId = mainInstrumentId;
	}
	
	private List<String> getOptionnames() {
		List<String> retList = new ArrayList<>();
		System.out.println("getOptionnames Begin");
		Connection conn = null;
		try {			
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			// First try to fetch from Snapshot table
			String fetchSql = "select DISTINCT(trading_symbol) as trading_symbol from nexcorio_option_snapshot"
					+ " where trading_symbol like '" + indShortPrefix + "%' "
					+ " and record_date = '" + forDate + "'";
						
			ResultSet rs = stmt.executeQuery(fetchSql);
			while (rs.next()) {
				retList.add(rs.getString("trading_symbol"));
			}
			rs.close();
			
			if (retList.size()==0) {
				fetchSql = "select DISTINCT(trading_symbol) as trading_symbol from nexcorio_option_greeks"
					+ " where trading_symbol like '" + indShortPrefix + "%' "
					+ " and f_main_instrument=" + mainInstrumentId
					+ " and quote_time > '" + forDate + " 09:15:00'"
					+ " and quote_time < '" + forDate + " 09:20:00'";
			
				rs = stmt.executeQuery(fetchSql);
				while (rs.next()) {
					retList.add(rs.getString("trading_symbol"));
				}
				rs.close();
			}
			
			stmt.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		System.out.println("Optionname " + retList.size());
		return retList;
	}
	
	private void processAtm() {
		
		List<String> optionnames = getOptionnames();
		System.out.println(optionnames.size());
		
		try {
			if (optionnames.size()>0) {
				SimpleDateFormat postgresLongDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				
				String beginTime = forDate + " 09:21:30";
				String endTime   = forDate + " 15:20:00";
				
				Calendar cal = Calendar.getInstance();
				cal.setTime(postgresLongDateFormat.parse(beginTime));
				
				Map<String, OptionGreek> prevOptionGreeks = null; 
				do {
					Map<String, OptionGreek> curOptionGreeks = new HashMap<String, OptionGreek>(); 
					
					for(String optionname:optionnames ) {
						OptionGreek aGreek = getOptionGreeks(optionname, cal.getTime());
						if (aGreek!=null) curOptionGreeks.put(aGreek.getTradingSymbol(), aGreek);
					}
					
					if (prevOptionGreeks!=null) {
						float ceOiChange = 0f;
						float peOiChange = 0f;
						
						Iterator<String> iter =  curOptionGreeks.keySet().iterator();
						while(iter.hasNext()) {
							String key = iter.next();
							OptionGreek currGreek = curOptionGreeks.get(key);
							OptionGreek prevGreek = prevOptionGreeks.get(key);
							if (currGreek!=null && prevGreek!=null) {
								float oiDiff = currGreek.getIv() - prevGreek.getIv();
								if (key.endsWith("CE")) ceOiChange = ceOiChange + oiDiff;
								else peOiChange = peOiChange + oiDiff;
							}
						}
						System.out.println(cal.getTime()+ " Diff=" + (ceOiChange-peOiChange) +" ceOiChange="+ceOiChange+" peOiChange="+peOiChange+ " Dirctn="+ (ceOiChange<peOiChange));
					}
					prevOptionGreeks = curOptionGreeks;
					cal.add(Calendar.MINUTE, 3);
				} while (cal.getTime().before(postgresLongDateFormat.parse(endTime)));
			} else {
				System.out.println("Not a trading day");
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	protected OptionGreek getOptionGreeks(String optionName, Date processingTime) {
		
		if (optionName==null || optionName.equals("")) return null;
		
		OptionGreek retVal = null;
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			Calendar cal = Calendar.getInstance();
			cal.setTime(processingTime);
			
			SimpleDateFormat postgresLongDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

			
			String fetchSql = "select iv, delta, vega, theta, gamma, ltp, oi from nexcorio_option_greeks  where trading_symbol = '" + optionName + "'"
					+ " and f_main_instrument=" + this.mainInstrumentId
					+ " and quote_time <= '" + postgresLongDateFormat.format(cal.getTime()) + "'"
					+ " order by quote_time desc limit 1";
			//System.out.println("recTimestamp="+recTimestamp+" sql=" +fetchSql);
			
			ResultSet rs = stmt.executeQuery(fetchSql);
			while (rs.next()) {
				retVal = new OptionGreek(optionName, rs.getFloat("iv"), rs.getFloat("delta"), rs.getFloat("vega"), rs.getFloat("theta"), rs.getFloat("gamma"), rs.getFloat("ltp"), rs.getFloat("oi"));
			}
			rs.close();
			stmt.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (conn!=null) conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return retVal;
	}

	public static void main(String[] args) {
		
		new OIVolumeChangeProcessorMain("2025-12-10", "NIFTY", 2L).processAtm();
		//new ATMProcessorMain("2025-11-20", "NIFTY", 2L).processAtm();
		//String forDate = "2025-11-14";
		//new ATMProcessorMain(forDate, "NIFTY", 2L).processAtm();
		//new ATMProcessorMain(forDate, "BANKNIFTY", 3L).processAtm(); //"BANKNIFTY"; SENSEX
		//new ATMProcessorMain(forDate, "SENSEX", 4L).processAtm(); 
	}

	
}
