package com.nexcorio.algo.backtest;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import com.nexcorio.algo.util.db.HDataSource;

public class CloneAlgo {

	public static Map<Long, String> getExistingAlgo(Long napAlgoId) {
		Map<Long, String> retMap = null;
		Connection conn = null;
		try {
			conn = HDataSource.getConnection();
			Statement stmt = conn.createStatement();
			
			String fetchSql = "SELECT id, algo_class_name FROM nexcorio_options_algo_strategy WHERE f_parent="+napAlgoId + " and algoname like 'Test%'";
			ResultSet rs = stmt.executeQuery(fetchSql);
			while (rs.next()) {
				retMap = new HashMap<Long, String>();
				retMap.put(rs.getLong("id"), rs.getString("algo_class_name"));
				break;
			}
			rs.close();
			stmt.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return retMap;
	}
	
	public static void deleteBacktestData(Long napAlgoId, String forDate) {
		Connection conn = null;
		try {
			conn = HDataSource.getConnection();
			Statement stmt = conn.createStatement();
			
			SimpleDateFormat postgresShortDateFormat = new SimpleDateFormat("yyyy-MM-dd");
			SimpleDateFormat postgresLongDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

			
			String deleteStr = "DELETE FROM nexcorio_option_algo_orders WHERE f_strategy = " + napAlgoId + " "
					+ " and short_date = '" + postgresShortDateFormat.format(postgresLongDateFormat.parse(forDate)) + "'";
			System.out.println("1. deleteStr="+deleteStr);
			stmt.execute(deleteStr);
			
			deleteStr = "DELETE FROM nexcorio_option_algo_orders_daily_summary WHERE f_strategy = " + napAlgoId + " "
					+ " and short_date = '" + postgresShortDateFormat.format(postgresLongDateFormat.parse(forDate)) + "'";
			System.out.println("1. deleteStr="+deleteStr);
			stmt.execute(deleteStr);
			stmt.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static Map<Long, String> cloneAlgo(Long napAlgoId, boolean isTestAlgo, Long targetInstrument) {
		Map<Long, String> retMap = new HashMap<Long, String>();
		Connection conn = null;
		try {
			conn = HDataSource.getConnection();
			Statement stmt = conn.createStatement();
			
			boolean isActive = true;
			String fetchNextSeq = "select nextval('nexcorio_options_algo_strategy_id_seq') as nextId";
	    	if (isTestAlgo) {
	    		fetchNextSeq = "select nextval('nexcorio_options_algo_strategy_test_id_seq') as nextId";
	    		isActive = false;
	    	}
			Long newAlgoId = null;
	    	ResultSet rs = stmt.executeQuery(fetchNextSeq);
			while (rs.next()) {
				newAlgoId = rs.getLong("nextId");
			}
			rs.close();
			
			String opOIFetch = "select f_user, f_main_instrument, algoname, entry_time, exit_time, no_of_lots, max_fund_allocated, target, stoploss, trailing_stoploss, max_allowed_nooforders, "
					+ " order_enabled_monday, order_enabled_tuesday, order_enabled_wednesday, order_enabled_thursday, order_enabled_friday, algo_class_name from nexcorio_options_algo_strategy where id = " + napAlgoId;			  
			
			String insertSqlPart = " INSERT INTO nexcorio_options_algo_strategy (id, f_parent, f_user, f_main_instrument, algoname, entry_time, exit_time, no_of_lots, max_fund_allocated, target, stoploss, trailing_stoploss, max_allowed_nooforders,"
					+ " order_enabled_monday, order_enabled_tuesday, order_enabled_wednesday, order_enabled_thursday, order_enabled_friday, algo_class_name, isactive) VALUES(" + newAlgoId+"," + napAlgoId;
			System.out.println("opOIFetch="+opOIFetch);
			
			String algoClassname = null;
			rs = stmt.executeQuery(opOIFetch);
			while (rs.next()) {
				algoClassname = rs.getString("algo_class_name");
				insertSqlPart = insertSqlPart + "," + rs.getLong("f_user");
				
				if (targetInstrument != null) {
					insertSqlPart = insertSqlPart + "," + targetInstrument;
				} else {
					insertSqlPart = insertSqlPart + "," + rs.getLong("f_main_instrument");
				}
				
				insertSqlPart = insertSqlPart + ",'" + (isTestAlgo ? "Test" : "") + rs.getString("algoname") +"'";
				
				insertSqlPart = insertSqlPart + ",'" + rs.getString("entry_time") +"'";
				insertSqlPart = insertSqlPart + ",'" + rs.getString("exit_time") +"'";
				insertSqlPart = insertSqlPart + "," + rs.getInt("no_of_lots");
				insertSqlPart = insertSqlPart + "," + rs.getInt("max_fund_allocated");
				
				insertSqlPart = insertSqlPart + "," + rs.getInt("target");
				insertSqlPart = insertSqlPart + "," + rs.getInt("stoploss");
				insertSqlPart = insertSqlPart + "," + rs.getInt("trailing_stoploss");
				insertSqlPart = insertSqlPart + "," + rs.getInt("max_allowed_nooforders");
				
				insertSqlPart = insertSqlPart + "," + Boolean.FALSE; //rs.getBoolean("order_enabled_monday"); 
				insertSqlPart = insertSqlPart + "," + Boolean.FALSE; //rs.getBoolean("order_enabled_tuesday");
				insertSqlPart = insertSqlPart + "," + Boolean.FALSE; //rs.getBoolean("order_enabled_wednesday");
				insertSqlPart = insertSqlPart + "," + Boolean.FALSE; //rs.getBoolean("order_enabled_thursday");
				insertSqlPart = insertSqlPart + "," + Boolean.FALSE; //rs.getBoolean("order_enabled_friday");
				insertSqlPart = insertSqlPart + ",'" + algoClassname +"'";
				
				insertSqlPart = insertSqlPart + "," + isActive + ")"; // IsActive
				
			}
			rs.close();			
			System.out.println(insertSqlPart);
			stmt.execute(insertSqlPart);
			
			opOIFetch = "select name, data_type, value from nexcorio_options_algo_strategy_parameters where f_strategy = " + napAlgoId;			  
			  
			System.out.println("opOIFetch="+opOIFetch);
			
			rs = stmt.executeQuery(opOIFetch);
			while (rs.next()) {
				String name = rs.getString("name");
				String dataType = rs.getString("data_type");
				String value = rs.getString("value");
				String insertSubSql = "INSERT INTO nexcorio_options_algo_strategy_parameters(id, f_strategy, name, data_type, value) VALUES(nextval('nexcorio_options_algo_strategy_parameters_id_seq')," + newAlgoId + ",'" + name + "','" + dataType +"','" + value +"')";
				System.err.println("insertSubSql="+insertSubSql);
				stmt.addBatch(insertSubSql);
			}
			rs.close();
			// submit a batch of update commands for execution
			stmt.executeBatch();
			stmt.close();
			
			retMap.put(newAlgoId, algoClassname);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return retMap;
	}
	
	public static void main(String[] args) {
		cloneAlgo(53L, false, 7L);
		cloneAlgo(53L, false, 8L);
		cloneAlgo(53L, false, 9L);
		cloneAlgo(53L, false, 10L);
		
	}
}
