package com.nexcorio.algo.junk;
/**
 * 
 * @author Keshav Shetty
 *
 */

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.analytics.ATMMovementAnalyzerThreadAlgoThread;

public class SyncStrategies {
	
	private static final Logger log = LogManager.getLogger(SyncStrategies.class);
	
	protected SimpleDateFormat postgresLongDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	protected SimpleDateFormat postgresShortDateFormat = new SimpleDateFormat("yyyy-MM-dd");
	

	private void saveOrUpdateFnOExpiryDate(String expiryDate, String fnoPrefix) {
		Connection terraceConn = null; 
		
		try {
			terraceConn = MultiDataSource.getTerraceConnection();
			Statement stmt = terraceConn.createStatement();
			
			String chksql = "select count(*) from nexcorio_fno_expiry_dates where expiry_date = '" + expiryDate + "' and fno_prefix = '" + fnoPrefix + "' and fno_segment = 'NFO-FUT' and f_main_instrument = 2";
			System.out.println(chksql);
			
			int recCount = 0;
			ResultSet rs = stmt.executeQuery(chksql);
			while (rs.next()) {
				recCount = rs.getInt(1);
			}
			rs.close();
			if (recCount==0) {
				chksql = "INSERT INTO nexcorio_fno_expiry_dates (id, expiry_date, fno_prefix, fno_segment, f_main_instrument) VALUES ("
						+ "nextval('nexcorio_fno_expiry_dates_id_seq'), '" + expiryDate + "', '" + fnoPrefix + "', 'NFO-FUT', 2)";
				System.out.println(chksql);
				stmt.executeUpdate(chksql);
			}
			stmt.close();
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error"+e.getMessage(),e);
		} finally {
			try {
				if (terraceConn!=null) terraceConn.close();
			} catch (SQLException e) {
				log.error(e);
			}
		}
	}
	
	private void migrateData(String forDate, String expiryDate, String fnoPrefix) {
		
		Connection rtxConn = null; 
		
		try {
			saveOrUpdateFnOExpiryDate(expiryDate, fnoPrefix);
			
			Calendar cal = Calendar.getInstance();
			cal.setTime(postgresShortDateFormat.parse(forDate));
			cal.set(Calendar.HOUR_OF_DAY, 9);
			cal.set(Calendar.MINUTE, 16);			
			Date datBeginTime= cal.getTime();
			
			cal.set(Calendar.HOUR_OF_DAY, 15);
			cal.set(Calendar.MINUTE, 30);
			Date datEndTime= cal.getTime();
			
			String sourceSql = " SELECT instrument_token, record_time, last_traded_price, openinterest, trading_symbol, total_buyquantity, total_sellquantity from zerodha_intraday_streaming_data"
					+ " where quote_time >= '" + postgresLongDateFormat.format(datBeginTime) + "' and quote_time <= '" + postgresLongDateFormat.format(datEndTime) + "'"
					+ " and (instrument_token = 13939714)"
					+ " order by quote_time";
			
			int page = 0;
			int noOfRec = 500000;
			
			boolean recExist = false;
			do {
				int threadCount = Thread.activeCount();
				System.out.println("Active threadCount="+threadCount);
				log.info("Active threadCount="+threadCount);
				while (threadCount > 200) {
					System.out.println("going to sleep");
					Thread.sleep(5000);
					threadCount = Thread.activeCount();
				}
				System.out.println("Resuming fetch from source");
				
				recExist = false;
				
				rtxConn = MultiDataSource.getRtxConnection();				
				Statement rtxStmt = rtxConn.createStatement();
				
				ResultSet rs = rtxStmt.executeQuery(sourceSql +" LIMIT " + noOfRec + " OFFSET " + (page*noOfRec));
				System.out.println(sourceSql);
				log.info(sourceSql);
				
				while (rs.next()) {
					JunkThread junkThread = new JunkThread(expiryDate, fnoPrefix, rs.getLong("instrument_token"), rs.getString("trading_symbol"), rs.getTimestamp("record_time"),
							rs.getFloat("last_traded_price"), rs.getFloat("openinterest"), rs.getFloat("total_buyquantity"), rs.getFloat("total_sellquantity")
							);
					
					recExist = true;
				}
				page++;
				
				rs.close();
				rtxStmt.close();
				rtxConn.close();
			} while(recExist == true);
			//new ATMMovementAnalyzerThreadAlgoThread("NIFTY", forDate + " 09:20:00");
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error"+e.getMessage(),e);
		} finally {
			try {
				if (rtxConn!=null) rtxConn.close();
			} catch (SQLException e) {
				log.error(e);
			}
		}
	}

	public static void syncStrategies(Connection sourceConn, Connection targetConn) {
		try {
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
	
		try {
			Connection sourceConn = MultiDataSource.getRtxConnection();
			Connection targetConn = MultiDataSource.getLocalConnection(); // Sync from RTX to Laptop. 
			
			syncStrategies(sourceConn, targetConn);
			
			sourceConn.close();
			targetConn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		
		
		
		
	}
	
}
