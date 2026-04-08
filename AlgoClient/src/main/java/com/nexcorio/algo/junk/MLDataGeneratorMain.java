package com.nexcorio.algo.junk;

import java.io.FileWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import com.nexcorio.algo.util.db.HDataSource;

public class MLDataGeneratorMain {

	private String forDate ;
	private String toDate ;
	
	private static SimpleDateFormat postgresLongDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	private static String stratgeyIds = "545, 9545, 542, 543, 540, 532, 528, 526, 523, 525, 522,  ";
	
	public MLDataGeneratorMain(String forDate, String toDate) {
		super();
		this.forDate = forDate;
		this.toDate = toDate;
	}
	
	private void processDailyOrders() {
		Connection conn = null;
		try {			
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			FileWriter writer = new FileWriter("D:\\temp\\junk\\ML-Nifty-selx.csv");
			writer.write("altabove5WhlStrkCEAvgIv, altabove5WhlStrkPEAvgIv, drWhlStrkaccumulatedchangein5seccetheta, drWhlStrkaccumulatedchangein5secpetheta,dr19fixedSizeCEAvgIV, dr19fixedSizePEAvgIV,dr19fixedSizeAccmlCETheta, dr19fixedSizeAccmlPETheta, Target\r\n");
			// First try to fetch from Snapshot table
			String fetchSql = "SELECT f_strategy, option_name, sell_price-buy_price as profit, entry_time"
					+ " FROM nexcorio_option_algo_orders" 
					+ " where f_strategy in (SELECT id from nexcorio_options_algo_strategy where f_main_instrument=2)" // (9751,9685,9796,9797)" //
					+ " AND short_date >= '" + forDate + "'"
					+ " AND short_date <= '" + toDate + "'"
					+ " AND (sell_price-buy_price > 15 or sell_price-buy_price < -15)"; //
			System.out.println(fetchSql);
						
			ResultSet rs = stmt.executeQuery(fetchSql);
			while (rs.next()) {
				processRecords(rs.getString("option_name"), rs.getTimestamp("entry_time"), rs.getFloat("profit"), writer);
			}
			writer.close();
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
	
	private void processRecords(String optionName, Timestamp entryTime, float profit, FileWriter writer) {
		
		Connection conn = null;
		try {			
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			String fetchSql = "select"
					+ " altabove5WhlStrkCEAvgIv, altabove5WhlStrkPEAvgIv, drWhlStrkaccumulatedchangein5seccetheta, drWhlStrkaccumulatedchangein5secpetheta"
					+ ",dr19fixedSizeCEAvgIV, dr19fixedSizePEAvgIV"
					+ ",dr19fixedSizeAccmlCETheta, dr19fixedSizeAccmlPETheta"
					
					+ " from nexcorio_option_atm_movement_data where f_main_instrument=2"
					+ " AND record_time <= '" + postgresLongDateFormat.format(entryTime) + "' order by record_time desc limit 1";
			
			System.out.println(fetchSql);
			ResultSet rs = stmt.executeQuery(fetchSql);
			while (rs.next()) {
				for(int i=0;i<8;i++) {
					if (i!=0) writer.write(","); 
					writer.write(rs.getFloat(i+1)+"");	
				}
			}
//			if (profit>0) {
//				if (optionName.endsWith("CE")) writer.write(",0");
//				else writer.write(",1");
//			} else {
//				if (optionName.endsWith("CE")) writer.write(",1");
//				else writer.write(",0");
//			}
			writer.write(","+profit);
			writer.write("\r\n");
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
		
		String fromDate = "2026-01-01";
		String toDate   = "2026-02-06";
		
		new MLDataGeneratorMain(fromDate, toDate).processDailyOrders();
	}
}
