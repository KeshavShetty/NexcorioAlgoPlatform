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
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.analytics.ATMMovementAnalyzerThreadAlgoThread;
import com.nexcorio.algo.kite.KiteHelper;
import com.nexcorio.algo.util.db.HDataSource;

public class DecayTable {
	
	private static final Logger log = LogManager.getLogger(DecayTable.class);
	
	protected SimpleDateFormat postgresLongDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	protected SimpleDateFormat postgresShortDateFormat = new SimpleDateFormat("yyyy-MM-dd");
	protected SimpleDateFormat regularShortDateFormat = new SimpleDateFormat("dd/MMM/yyyy");
	
    protected DecimalFormat df = new DecimalFormat("#,##0.00");


	private void process(String indexShortname, Date forDate) {
		
		float indexCheckValue = KiteHelper.getIndexCheck(postgresShortDateFormat.format(forDate)+" 09:20:00");
		if (indexCheckValue>0) {
			Connection conn = null;
			try {
				conn = HDataSource.getReadOnlyConnection();
				Statement stmt = conn.createStatement();
				
				// Previous trading day 3:20 straddle premium
				Calendar cal = Calendar.getInstance();
				cal.setTime(forDate);
				
				int noOfDaysPassed = 0;
				float prevDayClosingPremium = 0f;
				do {
					cal.set(Calendar.HOUR_OF_DAY, 15);
					cal.set(Calendar.MINUTE, 20);
					cal.add(Calendar.DATE, -1);
					noOfDaysPassed++;
					
					String fetchSql = "SELECT celtp+peltp as premium FROM nexcorio_option_atm_movement_data"
							+ " WHERE record_time <= '" + postgresLongDateFormat.format(cal.getTime()) + "'"
							//+ " AND record_time > ('" + postgresLongDateFormat.format(getCurrentTime()) + "' - '1 miniute'::interval)"
							+ " AND record_time > (DATE_ADD('" + postgresLongDateFormat.format(cal.getTime()) + "',INTERVAL '-1 minute')) "
							+ " AND f_main_instrument=" + (indexShortname.equals("NIFTY")?2:4) 
							+ " ORDER BY record_time DESC LIMIT 1";
					//System.out.println("1. prevDayClosingPremium-"+fetchSql);
					ResultSet rs = stmt.executeQuery(fetchSql);
					while (rs.next()) {
						prevDayClosingPremium = rs.getFloat("premium");
					}
					rs.close();
				} while(prevDayClosingPremium==0f);
				float idealPremium4Today = prevDayClosingPremium - noOfDaysPassed*12f;
				
				// Prev day closing vix (Vix at 3:20)
				float prevDayClosingVix = 0f;
				String fetchSql = "SELECT last_traded_price as prevDayClosingVix FROM nexcorio_tick_data where trading_symbol = 'VIX'"
						+ " AND quote_time <= '" + postgresLongDateFormat.format(cal.getTime()) + "'"
						+ " AND quote_time > (DATE_ADD('" + postgresLongDateFormat.format(cal.getTime()) + "',INTERVAL '-1 minute')) "
						+ " ORDER BY quote_time DESC LIMIT 1";
				//System.out.println("1. prevDayClosingPremium-"+fetchSql);
				ResultSet rs = stmt.executeQuery(fetchSql);
				while (rs.next()) {
					prevDayClosingVix = rs.getFloat("prevDayClosingVix");
				}
				rs.close();
				
				// Vix today at 9:20
				float openingVix = 0f;
				fetchSql = "SELECT last_traded_price from nexcorio_tick_data where trading_symbol = 'VIX' "
						+ " AND quote_time<'" + postgresShortDateFormat.format(forDate) + " 09:20:01'  order by quote_time desc limit 1";
				//System.out.println("2. vix-"+fetchSql);
				rs = stmt.executeQuery(fetchSql);
				while (rs.next()) {
					openingVix = rs.getFloat("last_traded_price");
				}
				rs.close();
				
				float highVix = 0f;
				float lowVix = 0f;
				fetchSql = "SELECT min(last_traded_price) as minVix, max(last_traded_price) as maxVix from nexcorio_tick_data where trading_symbol = 'VIX' "
						+ " AND quote_time >= '" + postgresShortDateFormat.format(forDate) +" 09:20:01' AND quote_time <= '" + postgresShortDateFormat.format(forDate) + " 15:20:01'";
				//System.out.println("2. Min Max vix-"+fetchSql);
				rs = stmt.executeQuery(fetchSql);
				while (rs.next()) {
					lowVix = rs.getFloat("minVix");
					highVix = rs.getFloat("maxVix");
				}
				rs.close();
				
				// Days opening premium at 9:20
				float openingPremium = 0f;
				fetchSql = "SELECT record_time, celtp+peltp as premium FROM nexcorio_option_atm_movement_data "
						+ " WHERE record_time<'" + postgresShortDateFormat.format(forDate) + " 09:20:01' AND f_main_instrument=" + (indexShortname.equals("NIFTY")?2:4) + " ORDER BY record_time DESC LIMIT 1";
				//System.out.println("3. openingPremium-"+fetchSql);
				rs = stmt.executeQuery(fetchSql);
				while (rs.next()) {
					openingPremium = rs.getFloat("premium");
				}
				rs.close();
				
				// Days high & low
				float lowPremium = 0f;
				float highPremium = 0f;
				fetchSql = "SELECT min(celtp+peltp) as lowPremium, max(celtp+peltp) as highPremium FROM nexcorio_option_atm_movement_data "
						+ " WHERE record_time>='"+ postgresShortDateFormat.format(forDate) +" 09:20:01' AND record_time<='" + postgresShortDateFormat.format(forDate) + " 15:20:01'"
						+ " AND f_main_instrument=" + (indexShortname.equals("NIFTY")?2:4);
				//System.out.println("4. Min Max-"+fetchSql);
				rs = stmt.executeQuery(fetchSql);
				while (rs.next()) {
					lowPremium = rs.getFloat("lowPremium");
					highPremium = rs.getFloat("highPremium");
				}
				rs.close();
				
				// Days close premium at 3:20
				float closingPremium = 0f;
				fetchSql = "SELECT record_time, celtp+peltp as premium FROM nexcorio_option_atm_movement_data "
						+ " WHERE record_time<'" +postgresShortDateFormat.format(forDate) + " 15:20:01' AND f_main_instrument=2 ORDER BY record_time DESC LIMIT 1";
				//System.out.println("5. closingPremium-"+fetchSql);
				rs = stmt.executeQuery(fetchSql);
				while (rs.next()) {
					closingPremium = rs.getFloat("premium");
				}
				rs.close();
				
				System.out.println("For " + new SimpleDateFormat("EEE").format(forDate) + " "+ regularShortDateFormat.format(forDate) 
						+ " prevDayClosingPremium="+df.format(prevDayClosingPremium)+"(V:" + df.format(prevDayClosingVix) + ")" 
						+ " Overnight Decay=" + df.format((openingPremium-prevDayClosingPremium)) + "(D:"+ noOfDaysPassed+")"
						+ " idealPremium4Today="+df.format(idealPremium4Today)
						+ " openingPremium="+df.format(openingPremium)+ "(V:" + df.format(openingVix) +") Days lowPremium="+df.format(lowPremium) + "(V:" + df.format(lowVix) + ")"
						+ " highPremium="+df.format(highPremium)+"(V:" + df.format(highVix)+") closingPremium="+df.format(closingPremium)
						+ " Daytime decay=" + df.format((closingPremium-openingPremium))
						);
				stmt.close();
			} catch(Exception ex) {
				ex.printStackTrace();
			}finally {
				try {
					if (conn!=null) conn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		} else {
			//System.out.println("Not a trading day");
		}
	}
	
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
	

	
	public static void main(String[] args) {
	
//		try {
//			Calendar cal = Calendar.getInstance();
//			cal.set(Calendar.DATE, 01);
//			DecayTable decayTable = new DecayTable();
//			decayTable.process("NIFTY", cal.getTime());
//			
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}	
//		
		try {
			
			String fromDate = "2025-10-23";
			String toDate = "2025-10-23";
			
			SimpleDateFormat pgLocalLongDateFormat = new SimpleDateFormat("yyyy-MM-dd");
			
			Calendar cal = Calendar.getInstance();		
			cal.setTime(pgLocalLongDateFormat.parse(fromDate));
			DecayTable decayTable = new DecayTable();
			
			
			do {
				//System.out.println("Calculate Decay table for the day " + pgLocalLongDateFormat.format(cal.getTime()));
				decayTable.process("NIFTY", cal.getTime());
				cal.add(Calendar.DATE, 1);
			} while(cal.getTime().before(pgLocalLongDateFormat.parse(toDate)) || cal.getTime().equals(pgLocalLongDateFormat.parse(toDate)));
			
			
		} catch(Exception ex) {
			ex.printStackTrace();
		}
		
		
	}
	
}
