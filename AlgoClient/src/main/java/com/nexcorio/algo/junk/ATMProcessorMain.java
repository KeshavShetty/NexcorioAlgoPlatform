package com.nexcorio.algo.junk;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.nexcorio.algo.util.db.HDataSource;

public class ATMProcessorMain {

	private static List<String> getOptionnames() {
		List<String> retList = new ArrayList<>();
		
		Connection conn = null;
		try {			
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			String fetchSql = "select DISTINCT(trading_symbol) as trading_symbol from nexcorio_option_greeks"
					+ " where trading_symbol like 'NIFTY%' "
					+ " and quote_time > '2025-06-26 09:15:00'"
					+ " and quote_time < '2025-06-26 15:15:00'";
			
			ResultSet rs = stmt.executeQuery(fetchSql);
			while (rs.next()) {
				retList.add(rs.getString("trading_symbol"));
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
		System.out.println("Optionname " + retList.size());
		return retList;
	}
	
	private static void processAtm(List<String> optionnames) {
		
		Connection conn = null;
		try {			
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			String fetchSql = "select id, instrumentltp, record_time from nexcorio_option_atm_movement_data where f_main_instrument=2 AND record_time >= '2025-06-26 09:15:00' AND record_time <= '2025-06-26 15:30:00' order by id";
			ResultSet rs = stmt.executeQuery(fetchSql);
			while (rs.next()) {
				Long aId = rs.getLong("id");
				System.out.println("Processing "+aId);
				ATMThread aThread = new ATMThread(aId, rs.getFloat("instrumentltp"), optionnames, rs.getTimestamp("record_time"));
				
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
		
		List<String> optionnames = getOptionnames();
		System.out.println(optionnames.size());
		//Long atmId = 1218756L;
		processAtm(optionnames);
		
	}
}
