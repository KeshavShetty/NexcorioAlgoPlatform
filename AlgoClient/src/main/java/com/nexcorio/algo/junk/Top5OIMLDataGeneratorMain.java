package com.nexcorio.algo.junk;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import com.ibm.icu.util.Calendar;
import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.util.db.HDataSource;

class SortbyOI implements Comparator<OptionGreek> {
    public int compare(OptionGreek a, OptionGreek b) { 
    	if (a.getOi() > b.getOi()) return -1;
    	else if (a.getOi() < b.getOi()) return 1;
    	else return 0;
    } 
}

public class Top5OIMLDataGeneratorMain {

	private String forDate ;
	
	
	
	private static String stratgeyIds = "545, 9545, 542, 543, 540, 532, 528, 526, 523, 525, 522,  ";
	
	public Top5OIMLDataGeneratorMain(String forDate) {
		super();
		this.forDate = forDate;
	}
	
	private void processDailyOrders() {
		System.out.println("Processing " + forDate);
		Connection conn = null;
		List<Top5OIMLThread> allThreads = new ArrayList<Top5OIMLThread>(); 
		try {			
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			List<String> optionnames = new ArrayList<>();
					
			String fetchSql = "select DISTINCT(trading_symbol) as trading_symbol from nexcorio_option_greeks"
					+ " where f_main_instrument = 2"
					+ " and quote_time > '" + forDate + " 09:15:00'"
					+ " and quote_time < '" + forDate + " 09:20:00'";
						
			ResultSet rs = stmt.executeQuery(fetchSql);
				
			while (rs.next()) {
				optionnames.add(rs.getString("trading_symbol"));
			}
			rs.close();
			if (optionnames.size()>0) {
				fetchSql = "SELECT id, f_strategy, option_name, sell_price-buy_price as profit, entry_time"
						+ " FROM nexcorio_option_algo_orders" 
						+ " where f_strategy in (SELECT id from nexcorio_options_algo_strategy where f_main_instrument=2)"
						+ " AND short_date = '" + forDate + "'"
						+ " AND (sell_price-buy_price > 20 or sell_price-buy_price < -20) order by short_date";
							
				rs = stmt.executeQuery(fetchSql);
				while (rs.next()) {
					Top5OIMLThread top5OIMLThread = new Top5OIMLThread(optionnames, rs.getLong("id"), rs.getString("option_name"), rs.getTimestamp("entry_time"), rs.getFloat("profit"));
					allThreads.add(top5OIMLThread);
				}
				//writer.close();
				rs.close();
			} else {
				System.out.println("Not a trading day");
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
		for(int i=0;i<allThreads.size();i++) {
			allThreads.get(i).processRecords();
		}
	}
	
	public static void main(String[] args) {
		
		String fromDate = "2025-08-01";
		String toDate   = "2025-12-12";
		
		SimpleDateFormat postgresShortDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		
		Calendar cal = Calendar.getInstance();
		try {
			cal.setTime(postgresShortDateFormat.parse(fromDate));
			cal.set(Calendar.HOUR_OF_DAY, 0);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			do {
				new Top5OIMLDataGeneratorMain(postgresShortDateFormat.format(cal.getTime())).processDailyOrders();	
				cal.add(Calendar.DATE, 1);
			} while(postgresShortDateFormat.parse(toDate).after(cal.getTime()) || postgresShortDateFormat.parse(toDate).equals(cal.getTime()));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

