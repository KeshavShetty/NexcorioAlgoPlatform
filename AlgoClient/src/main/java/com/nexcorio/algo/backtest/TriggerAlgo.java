package com.nexcorio.algo.backtest;

import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.experimental.theories.PotentialAssignment;

import com.nexcorio.algo.kite.KiteHelper;
import com.nexcorio.algo.util.db.HDataSource;

/**
 * 
 * @author Keshav Shetty
 *
 */
public class TriggerAlgo {
	private static final Logger log = LogManager.getLogger(TriggerAlgo.class);
	
	public static void triggerAlgo(Long napAlgoId, String forDate) {
		
		Map<Long, String> retMap = CloneAlgo.getExistingAlgo(napAlgoId);
		if (retMap==null) {
			retMap = CloneAlgo.cloneAlgo(napAlgoId, true, null);
		} else {
			CloneAlgo.deleteBacktestData(retMap.keySet().iterator().next(), forDate);
		}
		
		retMap.keySet().iterator();
		Long algoIdToRun = retMap.keySet().iterator().next();
		
		float indexCheckValue = KiteHelper.getIndexCheck(forDate);
		if (indexCheckValue>0) {
			String algoClassname = retMap.get(algoIdToRun);
			System.out.println("Going o run algo "+algoIdToRun);
			try {
				Class<?> myClass = Class.forName(algoClassname);
				Constructor<?> ctr = myClass.getConstructor(Long.class, String.class);
				Object object = ctr.newInstance(new Object[] { algoIdToRun, forDate });
			} catch (Exception e) {
				log.error(e.getMessage());
				e.printStackTrace();
			}
		} else {
			System.out.println("Not a atrading day");
		}
	}
	
	public static void triggerAlgo(Long napAlgoId, String fromDate, String toDate) {
		SimpleDateFormat postgresLongDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		try {
			Calendar cal = Calendar.getInstance();		
			cal.setTime(postgresLongDateFormat.parse(fromDate));
			
			do {
				System.out.println("Launching algo for day " + postgresLongDateFormat.format(cal.getTime()));
				triggerAlgo(napAlgoId, postgresLongDateFormat.format(cal.getTime()));
				cal.add(Calendar.DATE, 1);
			} while(cal.getTime().before(postgresLongDateFormat.parse(toDate)) || cal.getTime().equals(postgresLongDateFormat.parse(toDate)));
			
			
		} catch(Exception ex) {
			ex.printStackTrace();
		}
		
	}
	
	private static void updateParams(String neutralGreek, String ceGreek, String peGreek) {
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			stmt.executeUpdate("UPDATE nexcorio_options_algo_strategy_parameters set value='" + neutralGreek + "' WHERE id=11944");
			stmt.executeUpdate("UPDATE nexcorio_options_algo_strategy_parameters set value='" + ceGreek + "' WHERE id=11945");
			stmt.executeUpdate("UPDATE nexcorio_options_algo_strategy_parameters set value='" + peGreek + "' WHERE id=11946");
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
		
	}
	
	private static void saveResult(String neutralGreek, String ceGreek, String peGreek) {
		
		
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			
			String insertSql = "INSERT INTO multi_greekgap_result (neutralGreek, ceGreek, peGreek, month_9, month_10, month_11, month_12)"
					+ " VALUES('" + neutralGreek +"','" + ceGreek +"','" + peGreek +"'";
			
			
		    String fetchProfit = "select date_trunc('month'::text, noao.short_date)::date AS order_month, sum(noao.exit_profit)::double precision AS sumlot"
		    		+ " from nexcorio_option_algo_orders_daily_summary noao, nexcorio_options_algo_strategy noas, nexcorio_main_instruments nmi where noao.f_strategy = noas.id and noas.f_main_instrument = nmi.id "
		    		+ " and noao.f_strategy = 9695"
		    		+ " AND date_trunc('month'::text, noao.short_date)::date >= '2025-09-01'"
		    		+ " AND date_trunc('month'::text, noao.short_date)::date <= '2025-12-01'"
		    		+ " GROUP BY order_month ORDER BY  order_month";
			
			ResultSet rs = stmt.executeQuery(fetchProfit);
			while(rs.next()) {
				insertSql = insertSql + "," + +rs.getFloat("sumlot");
			}
			rs.close();
			
			insertSql = insertSql + ")";
			System.out.println(insertSql);
			
			stmt.executeUpdate(insertSql);
			
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
		
	}
	
	private static boolean resultExist(String neutralGreek, String ceGreek, String peGreek) {
		boolean retVal = false;
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			String fetchSql = "SELECT id FROM multi_greekgap_result WHERE neutralGreek='"  + neutralGreek + "' AND ceGreek='" + ceGreek + "' AND peGreek='" + peGreek + "'";
			System.out.println(fetchSql);
			ResultSet rs = stmt.executeQuery(fetchSql);
			//Long foundId = null;
			while(rs.next()) {
				retVal = true;
				//foundId = rs.getLong("id");
			}
			rs.close();
//			if (foundId!=null) {
//				stmt.execute("DELETE FROM multi_greekgap_result WHERE id="+foundId);
//			}
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
		return retVal;
	}
	
	private static Map<Long, String> getShortlistedStrategies() {
		Map<Long, String> retMap = new HashMap<Long, String>();
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			String fetchSql = "SELECT id,neutralGreek,ceGreek,peGreek FROM multi_greekgap_result WHERE test_profit=0 and (month_9+month_10+month_11+month_12)>750 ORDER BY id";
			System.out.println(fetchSql);
			ResultSet rs = stmt.executeQuery(fetchSql);
			while(rs.next()) {
				retMap.put(rs.getLong("id"), rs.getString("neutralGreek")+","+rs.getString("ceGreek")+","+rs.getString("peGreek"));
			}
			rs.close();			
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
		return retMap;
	}
	
	private static void updateTestResult(Long strategyId) {
		
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
		    String fetchProfit = "select date_trunc('month'::text, noao.short_date)::date AS order_month, sum(noao.exit_profit)::double precision AS sumlot"
		    		+ " from nexcorio_option_algo_orders_daily_summary noao, nexcorio_options_algo_strategy noas, nexcorio_main_instruments nmi where noao.f_strategy = noas.id and noas.f_main_instrument = nmi.id "
		    		+ " and noao.f_strategy = 96959"
		    		+ " AND date_trunc('month'::text, noao.short_date)::date >= '2026-01-01'"
		    		+ " AND date_trunc('month'::text, noao.short_date)::date <= '2026-01-01'"
		    		+ " GROUP BY order_month ORDER BY  order_month";
			
		    float sumLot = 0f;
			ResultSet rs = stmt.executeQuery(fetchProfit);
			while(rs.next()) {
				sumLot = rs.getFloat("sumlot");
			}
			rs.close();
			
			String updateSql = "UPDATE multi_greekgap_result set test_profit="+sumLot+" WHERE id="+strategyId;
			System.out.println(updateSql);
			
			stmt.executeUpdate(updateSql);
			
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
		
	}
	
	private static void updateTestResult(String neutralGreek, String ceGreek, String peGreek) {
		
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
		    String fetchProfit = "select date_trunc('month'::text, noao.short_date)::date AS order_month, sum(noao.exit_profit)::double precision AS sumlot"
		    		+ " from nexcorio_option_algo_orders_daily_summary noao, nexcorio_options_algo_strategy noas, nexcorio_main_instruments nmi where noao.f_strategy = noas.id and noas.f_main_instrument = nmi.id "
		    		+ " and noao.f_strategy = 9695"
		    		+ " AND date_trunc('month'::text, noao.short_date)::date >= '2026-01-01'"
		    		+ " AND date_trunc('month'::text, noao.short_date)::date <= '2026-01-01'"
		    		+ " GROUP BY order_month ORDER BY  order_month";
			
		    float sumLot = 0f;
			ResultSet rs = stmt.executeQuery(fetchProfit);
			while(rs.next()) {
				sumLot = rs.getFloat("sumlot");
			}
			rs.close();
			
			String updateSql = "UPDATE multi_greekgap_result set test_profit="+sumLot+" WHERE neutralGreek='"  + neutralGreek + "' AND ceGreek='" + ceGreek + "' AND peGreek='" + peGreek + "'";
			System.out.println(updateSql);
			
			stmt.executeUpdate(updateSql);
			
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
		
	}
	
	public static void main(String[] args) {
		String forDate = "2025-09-01";
		String toDate  = "202-10-01";

		triggerAlgo(796L, "2026-02-09 09:20:00", "2026-02-09 12:26:00");
//		triggerAlgo(685L, "2026-02-04 09:20:00", "2026-02-04 12:26:00");
//		triggerAlgo(787L, "2025-09-01 09:20:00", "2026-02-01 12:26:00");
//		triggerAlgo(788L, "2025-09-01 09:20:00", "2026-02-01 12:26:00");
//		triggerAlgo(789L, "2025-09-01 09:20:00", "2026-02-01 12:26:00");
//		triggerAlgo(790L, "2025-09-01 09:20:00", "2026-02-01 12:26:00");
//		triggerAlgo(791L, "2025-09-01 09:20:00", "2026-02-01 12:26:00");
//		triggerAlgo(792L, "2025-09-01 09:20:00", "2026-02-01 12:26:00");
//		triggerAlgo(793L, "2025-09-01 09:20:00", "2026-02-01 12:26:00");
		
		//triggerAlgo(751L, "2026-01-29 09:20:00", "2026-01-29 12:26:00");
		//triggerAlgo(737L, "2025-09-01 09:20:00", "2026-01-21 12:26:00");
	}
	
//	public static void main(String[] args) {
//		Map<Long, String> strategies = getShortlistedStrategies();
//		
//		Iterator<Long> iter =  strategies.keySet().iterator();
//		while(iter.hasNext()) {
//			Long strategyId = iter.next();
//			String[] params = strategies.get(strategyId).split(",");
//			updateParams(params[0],params[1],params[2]);
//			
//			int threadCount = Thread.activeCount();
//			
//			String forDate = "2026-01-01";
//			String toDate  = "2026-01-14";
//			
//			triggerAlgo(695L, forDate + " 09:20:00", toDate + " 12:26:00");
//			do {
//				try {
//					System.out.println("Original thread count "+threadCount+" current active "+Thread.activeCount());
//					Thread.sleep(60000);
//				} catch (InterruptedException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//			} while(Thread.activeCount()>threadCount);
//			updateTestResult(strategyId);
//					
//		}
//	}
	
//	public static void main(String[] args) {
//		
////		String[] allStrategies = {
////				"dr1-6AvgIv", "dr4-9AvgIv","Top5OiWorth","avggamma","selectiveavggamma","avgiv","deltaRangeAvgIV","deltaRangeAvgGamma","deltaRangeAvgLtp",
////				"deltaRangeDeltaOI","deltaRangeGmaOI","drFullAvgIV","drTop5DeltaOI","dr49TotalIV","gammaExposure","accmltd5secIVChg",
////				"gammaExposureWthStrk","dr49accmlGama","dr49accmlVega","dr49accmlDelta","dr49accmlTheta","dr49accmlIv","dr49accmlLtp",
////				"dr16accmlGama","dr16accmlVega","dr16accmlDelta","dr16accmlTheta","dr16accmlIv","dr16accmlLtp",
////				"drWhlStrkaccmlGama","drWhlStrkaccmlVega","drWhlStrkaccmlDelta","drWhlStrkaccmlTheta","drWhlStrkaccmlIv",
////				"drWhlStrkaccmlLtp"};
//		
////		String[] allStrategies = {
////				"drWhlStrkaccmlTheta",
////				"deltaPrbltOiWorth",
////				"deltaRangeGmaOI",
////				"dr19WholeStrikeAvgIV",
////				"accmltd5secIVChg",
////				"drOutlierRatio",
////				"drWhlStrkaccmlVega",
////				"dr1-6AvgIv", 				
////				"selectiveavggamma",
////				"drFullAvgIV",
////				"gammaExposureWthStrk",
////				"dr4-9AvgIv",				
////				"deltaRangeAvgIV",
////				"Top5OiWorth"
////		};
//		
//		String[] neutralPreference = {  "--dr49accmlTheta"};//, "drWhlStrkaccmlTheta","drITMWhlStrkAvgIv", "drOutlierRatio",  "dr4-9AvgI", "dr1-6AvgI"};//,"dr49accmlTheta", "dr4-9AvgIv",   "dr1-6AvgIv", "selectiveavggamma","deltaPrbltOiWorth"}; // "drWhlStrkaccmlIv",
//		
//		String[] cePreference = { "drWhlStrkaccmlTheta", "drITMWhlStrkAvgIv","drOutlierRatio",  "accmltd5secIVChg", "Top5OiWorth","dr49accmlTheta"};//, "dr49accmlTheta", "Top5OiWorth", "deltaPrbltOiWorth","accmltd5secIVChg","drWhlStrkaccmlVega","drOutlierRatio"}; // "dr1-6AvgIv","drFullAvgIV",
//		
//		String[] pePreference = {"drITMWhlStrkAvgIv","accmltd5secIVChg", "drWhlStrkaccmlVega", "dr4-9AvgIv", "Top5OiWorth","dr49accmlTheta"}; //{"drWhlStrkaccmlTheta", "dr49accmlTheta", "deltaRangeGmaOI", "dr4-9AvgIv","accmltd5secIVChg","drOutlierRatio","drFullAvgIV","dr1-6AvgIv", "dr19WholeStrikeAvgIV"}; 
//		
//		//int noofStratgeies = allStrategies.length;
//		for(int i=0;i<neutralPreference.length;i++) {
//			for(int j=0;j<cePreference.length;j++) {
//				for(int k=0;k<pePreference.length;k++) {
//					if (resultExist(neutralPreference[i], cePreference[j],pePreference[k])) {
//						continue;
//					} else {
//						updateParams(neutralPreference[i], cePreference[j],pePreference[k]);
//						int threadCount = Thread.activeCount();
//						
//						String forDate = "2025-09-01";
//						String toDate  = "2026-01-14";
//						
//						triggerAlgo(695L, forDate + " 09:20:00", toDate + " 12:26:00");
//						do {
//							try {
//								System.out.println("Original thread count "+threadCount+" current active "+Thread.activeCount());
//								Thread.sleep(30000);
//							} catch (InterruptedException e) {
//								// TODO Auto-generated catch block
//								e.printStackTrace();
//							}
//						} while(Thread.activeCount()>threadCount);
//						saveResult(neutralPreference[i], cePreference[j],pePreference[k]);
//						updateTestResult(neutralPreference[i], cePreference[j],pePreference[k]);
//					}
//				}
//			}
//		}
//	}
	
}
