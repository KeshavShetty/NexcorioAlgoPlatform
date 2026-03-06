package com.nexcorio.algo.junk;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import com.nexcorio.algo.util.FileLogTelegramWriter;
import com.nexcorio.algo.util.db.HDataSource;


public class FutureThread implements Runnable {

	String indexShortname;
	String forDate;
	
	long mainInstrumentId;
	
	public FutureThread(String indexShortname, String forDate) {
		super();
		this.indexShortname = indexShortname;
		this.forDate = forDate;
		Thread t = new Thread(this, indexShortname+forDate);
		t.setPriority(Thread.MAX_PRIORITY);
		t.start();
	}	
	
	@Override
	public void run() {
		
		String futPrefix = getNextNFUTUREExpiryDatePrefix();
		System.out.println(futPrefix);
		
		Connection conn = null;
		Statement stmt = null;
		
		float prev_last_traded_price = -1f;
		long prev_volume_traded_today = 0l;
		try {
			conn = HDataSource.getReadOnlyConnection();
			stmt = conn.createStatement();
			
			float outstandingVolume = 0;
			
			int pageNo = 0;
			int recordsPerPage = 1000;
			int processedRecords = 0;
			do {
				processedRecords = 0;
				
				String fetchSql = "SELECT ID, quote_time, last_traded_price, volume_traded_today FROM nexcorio_tick_data"
						+ " WHERE trading_symbol = '" + futPrefix + "'"
						+ " AND quote_time >= '" + forDate + " 09:20:01'"
						+ " AND quote_time <= '" + forDate + " 15:15:01'"
						+ " ORDER BY quote_time, ID LIMIT " + recordsPerPage + " OFFSET " + (pageNo*recordsPerPage);
	
				pageNo++;
				
				ResultSet rs = stmt.executeQuery(fetchSql);
				
				while(rs.next()) {
					processedRecords++;
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
					save(quoteTime, outstandingVolume);
				}
				rs.close();
			} while(processedRecords > 0);
			stmt.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (conn!=null) conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	private void save(Date quoteTime, float outstandingVolume) {
		System.out.println("For " + quoteTime + " outstandingVolume="+outstandingVolume);
		Connection conn = null;
		Statement stmt = null;
		
		try {
			conn = HDataSource.getReadOnlyConnection();
			stmt = conn.createStatement();
			
			SimpleDateFormat postgresLongDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			
			String updateSql = "update nexcorio_option_atm_movement_data set " //future_Outstanding_Volume = " + outstandingVolume
					+ " top5OiDiff=" + getTopNOiWorthDiff(quoteTime)
					+ " where id = (select id from nexcorio_option_atm_movement_data where record_time < '" + postgresLongDateFormat.format(quoteTime)+ "' and f_main_instrument=2 order by record_time desc limit 1 )";
			System.out.println(updateSql);
			stmt.executeUpdate(updateSql);
			stmt.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (conn!=null) conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	private float getTopNOiWorthDiff(Date quoteTime) {
		float retVal = 0f;
		Connection conn = null;
		Statement stmt = null;
		
		try {
			conn = HDataSource.getReadOnlyConnection();
			stmt = conn.createStatement();
			
			SimpleDateFormat postgresLongDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			SimpleDateFormat postgresShortDateFormat = new SimpleDateFormat("yyyy-MM-dd");
			
			Calendar cal = Calendar.getInstance();
			cal.setTime(quoteTime);
			cal.add(Calendar.MINUTE, -1);
			
			String fetchSql = "WITH RankedRows AS"
					+ "("
					+ " SELECT"
					+ " quote_time, trading_symbol, ltp, iv, delta, vega, theta, gamma, oi, underlying_value,"
					+ " ROW_NUMBER() OVER (PARTITION BY trading_symbol ORDER BY quote_time DESC, id DESC) AS rank"
					+ " FROM nexcorio_option_greeks"
					+ " WHERE trading_symbol IN (select trading_symbol from nexcorio_option_snapshot where trading_symbol like 'NIFTY%' and record_date = '" + postgresShortDateFormat.format(quoteTime) + "')"
					+ " AND quote_time >= '" + postgresLongDateFormat.format(cal.getTime()) + "' AND quote_time <= '" + postgresLongDateFormat.format(quoteTime)+ "'"
					+ ")"
					+ " SELECT quote_time, trading_symbol, oi*ltp/(10000000) as worth FROM RankedRows WHERE rank = 1"
					+ " and oi*ltp/10000000 > 10 order by oi desc limit 5";
			
			ResultSet rs = stmt.executeQuery(fetchSql);
			
			float ceTotal = 0f;
			float peTotal = 0f;
			while(rs.next()) {
				String tradingSymbol = rs.getString("trading_symbol");
				float  worth = rs.getFloat("worth");
				if(tradingSymbol.endsWith("CE")) {
					ceTotal = ceTotal + worth;
				} else {
					peTotal = peTotal + worth;
				}
			}
			rs.close();
			stmt.close();
			return  peTotal - ceTotal;
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
	
//	private float getFutureOI() {
//		float retVal =0f;
//		Connection conn = null;
//		try {
//			conn = HDataSource.getConnection();
//			Statement stmt = conn.createStatement();
//			
//			String futurePrefix = getNextNFUTUREExpiryDatePrefix(2L, "NFO-FUT");
//			
//			SimpleDateFormat postgresLongDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//			
//			String fetchSql = "select total_buy_qty-total_sell_qty as open_interest from nexcorio_tick_data where trading_symbol = '" + futurePrefix +"'"
//					+( " and quote_time <='" + postgresLongDateFormat.format( recTimestamp.getTime() )+ "'") 
//					+ " order by quote_time desc limit 1";
//			fileLogTelegramWriter.write(fetchSql);
//			ResultSet rs = stmt.executeQuery(fetchSql);
//			while (rs.next()) {
//				retVal = rs.getFloat("open_interest");
//			}
//			rs.close();
//			stmt.close();
//		} catch (Exception e) {
//			e.printStackTrace();
//		} finally {
//			try {
//				if (conn!=null) conn.close();
//			} catch (SQLException e) {
//				e.printStackTrace();
//			}
//		}	
//		return retVal;
//	}

	protected String getNextNFUTUREExpiryDatePrefix() {
		
		String retStr = "";
		
		Connection conn = null;
		Statement stmt = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			stmt = conn.createStatement();
			
			String fnoExchange = "NFO-FUT";
			
			String fetchSql = "SELECT fno_prefix from nexcorio_fno_expiry_dates WHERE f_main_instrument=2"
					+ " and fno_segment='" + fnoExchange + "' "
					+ " and expiry_date >= '" + forDate + "' "
					+ " ORDER BY expiry_date ASC LIMIT 1";
			
			ResultSet rs = stmt.executeQuery(fetchSql);
			
			while(rs.next()) {
				retStr = rs.getString("fno_prefix") + "FUT";
			}
			rs.close();			
			stmt.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (conn!=null) conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return retStr;
	}
	

	
	public static void main(String[] args) {
		
		
		String fromDate = "2026-01-09";
		String toDate   = "2026-01-09";
		
		SimpleDateFormat postgresShortDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		
		try {
			Calendar cal = Calendar.getInstance();		
			cal.setTime(postgresShortDateFormat.parse(fromDate));
			
			do {
				System.out.println("Launching for day " + postgresShortDateFormat.format(cal.getTime()));
				
				new FutureThread("NIFTY", postgresShortDateFormat.format(cal.getTime()));
				
				cal.add(Calendar.DATE, 1);
			} while(cal.getTime().before(postgresShortDateFormat.parse(toDate)) || cal.getTime().equals(postgresShortDateFormat.parse(toDate)));
			
			
		} catch(Exception ex) {
			ex.printStackTrace();
		}
		
	}
}
