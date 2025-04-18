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

public class JunkMain implements Runnable {
	
	private static final Logger log = LogManager.getLogger(JunkMain.class);
	
	protected SimpleDateFormat postgresLongDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	protected SimpleDateFormat postgresShortDateFormat = new SimpleDateFormat("yyyy-MM-dd");

	String forDate;
	String expiryDate;
	String fnoPrefix; 
	
	public JunkMain(String forDate, String expiryDate, String fnoPrefix) {
		System.out.println("Main thread started for "+forDate);
		
		this.forDate = forDate;
		this.expiryDate = expiryDate;
		this.fnoPrefix = fnoPrefix;
		
		Thread t = new Thread(this, forDate);
		t.setPriority(Thread.MAX_PRIORITY);
		t.start();	
	}

	private void saveOrUpdateFnOExpiryDate(String expiryDate, String fnoPrefix) {
		Connection terraceConn = null; 
		
		try {
			terraceConn = MultiDataSource.getTerraceConnection();
			Statement stmt = terraceConn.createStatement();
			
			String chksql = "select count(*) from nexcorio_fno_expiry_dates where expiry_date = '" + expiryDate + "' and fno_prefix = '" + fnoPrefix + "' and fno_segment = 'NFO-OPT' and f_main_instrument = 2";
			System.out.println(chksql);
			
			int recCount = 0;
			ResultSet rs = stmt.executeQuery(chksql);
			while (rs.next()) {
				recCount = rs.getInt(1);
			}
			rs.close();
			if (recCount==0) {
				chksql = "INSERT INTO nexcorio_fno_expiry_dates (id, expiry_date, fno_prefix, fno_segment, f_main_instrument) VALUES ("
						+ "nextval('nexcorio_fno_expiry_dates_id_seq'), '" + expiryDate + "', '" + fnoPrefix + "', 'NFO-OPT', 2)";
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
			
			String sourceSql = " SELECT instrument_token, record_time, last_traded_price, openinterest, trading_symbol from zerodha_intraday_streaming_data"
					+ " where quote_time >= '" + postgresLongDateFormat.format(datBeginTime) + "' and quote_time <= '" + postgresLongDateFormat.format(datEndTime) + "'"
					+ " and (trading_symbol like '" + fnoPrefix + "%' or instrument_token = 256265)"
					+ " order by trading_symbol, quote_time, id ";
			
			int page = 0;
			int noOfRec = 1000;
			
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
					JunkThread junkThread = new JunkThread(expiryDate, fnoPrefix, rs.getLong("instrument_token"), rs.getString("trading_symbol"), rs.getTimestamp("record_time"), rs.getFloat("last_traded_price"), rs.getFloat("openinterest"));
					
					recExist = true;
				}
				page++;
				
				rs.close();
				rtxStmt.close();
				rtxConn.close();
			} while(recExist == true);
			new ATMMovementAnalyzerThreadAlgoThread("NIFTY", forDate + " 09:20:00");
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

	@Override
	public void run() {
		migrateData(forDate, expiryDate, fnoPrefix);
	}

	public static void main(String[] args) {
	
		new JunkMain("2025-02-28", "2025-03-06", "NIFTY25306");
		
	}
	
}
