package com.nexcorio.algo.junk;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import com.nexcorio.algo.util.db.HDataSource;

public class ATMProcessorMain {

	private String forDate ;
	private String indShortPrefix ; 
	private long mainInstrumentId ; 
	
	public ATMProcessorMain(String forDate, String indShortPrefix, long mainInstrumentId) {
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
		
		Connection conn = null;
		try {			
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
//			SimpleDateFormat postgresLongDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//			ATMThread aThread = new ATMThread(5651344L, 22664.3f, optionnames, new Timestamp(postgresLongDateFormat.parse("2026-03-23 09:48:59").getTime()) , mainInstrumentId);
			
			String fetchSql = "select id, instrumentltp, record_time from nexcorio_option_atm_movement_data where f_main_instrument=" + mainInstrumentId
					+ " AND record_time >= '" + forDate + " 09:15:10' AND record_time <= '" + forDate + " 15:30:00' order by id";
			ResultSet rs = stmt.executeQuery(fetchSql);
			while (rs.next()) {
				Long aId = rs.getLong("id");
				//System.out.println("Processing "+aId);
				ATMThread aThread = new ATMThread(aId, rs.getFloat("instrumentltp"), optionnames, rs.getTimestamp("record_time"), mainInstrumentId);
				
				while (Thread.activeCount()>=150) {
					System.out.println("Sleeping, thread count "+Thread.activeCount());
					Thread.sleep(1000);
				}
			}
			rs.close();
			
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
	}
	
	public static void main(String[] args) {
		
//		new ATMProcessorMain("2025-12-01", "NIFTY", 2L).processAtm();
		
		String fromDate = "2026-05-11";
		String toDate   = "2026-05-11";
		
		SimpleDateFormat postgresShortDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		
		try {
			Calendar cal = Calendar.getInstance();		
			cal.setTime(postgresShortDateFormat.parse(fromDate));
			
			do {
				System.out.println("Launching for day " + postgresShortDateFormat.format(cal.getTime()));
				new ATMProcessorMain(postgresShortDateFormat.format(cal.getTime()), "NIFTY", 2L).processAtm();
				cal.add(Calendar.DATE, 1);
			} while(cal.getTime().before(postgresShortDateFormat.parse(toDate)) || cal.getTime().equals(postgresShortDateFormat.parse(toDate)));
			
			
		} catch(Exception ex) {
			ex.printStackTrace();
		}
	}

	
}
