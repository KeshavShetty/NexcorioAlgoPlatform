package com.nexcorio.algo.kite;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.dto.MainInstruments;
import com.nexcorio.algo.dto.OptionFnOInstrument;
import com.nexcorio.algo.util.db.HDataSource;

/**
 * 
 * @author Keshav Shetty
 *
 */
public class KiteCache {
	
	private static final Logger log = LogManager.getLogger(KiteCache.class);
	
	private static Map<Long, String> instrumentTokenToTradingSymbolCache = new HashMap<Long, String>(); 
	private static Map<String, MainInstruments> tradingSymbolMainInstrumentCache = new HashMap<String, MainInstruments>();
	private static Map<String, String> tradingSymbolExchangeCache= new HashMap<String, String>();
	
	private static Map<String, OptionFnOInstrument> tradingSymbolToOptionInstrument = new HashMap<String, OptionFnOInstrument>();

	public static void putInstrumentTokenToTradingSymbolCache(Long keyVal, String value) {
		instrumentTokenToTradingSymbolCache.put(keyVal, value);
	}
	
	public static void putTradingSymbolMainInstrumentCache(String keyVal, MainInstruments mainInstrument) {
		tradingSymbolMainInstrumentCache.put(keyVal, mainInstrument);
	}
	
	public static void putTradingSymbolExchangeCache(String keyVal, String value) {
		tradingSymbolExchangeCache.put(keyVal, value);
	}

	public static void putTradingSymbolToOptionInstrument(String keyVal, OptionFnOInstrument value) {
		tradingSymbolToOptionInstrument.put(keyVal, value);
	}
	
	public static String getInstrumentTokenToTradingSymbolCache(Long keyVal) {
		return instrumentTokenToTradingSymbolCache.get(keyVal);
	}
	
	public static MainInstruments getTradingSymbolMainInstrumentCache(String keyVal) {
		MainInstruments retInstrument = tradingSymbolMainInstrumentCache.get(keyVal);
		if (retInstrument==null) {
			
			MainInstruments mainInstrument = null;
			Connection conn = null;
			Statement stmt = null;
			try {
				conn = HDataSource.getConnection();
				stmt = conn.createStatement();
				
				ResultSet rs = stmt.executeQuery("SELECT id, name, short_name, instrument_type, exchange,"
						+ " zerodha_instrument_token, expiry_day, gap_between_strikes, order_freezing_quantity,"
						+ " no_of_future_expiry_data, no_of_options_expiry_data, no_of_options_strike_points, straddle_margin"
						+ " FROM nexcorio_main_instruments WHERE short_name='"+keyVal+"'"); 
				while(rs.next()) {
					mainInstrument = new MainInstruments();
					mainInstrument.setId(rs.getLong("id"));
					mainInstrument.setName(rs.getString("name"));
					mainInstrument.setShortName(rs.getString("short_name"));
					mainInstrument.setInstrumentType(rs.getString("instrument_type"));
					mainInstrument.setExchange(rs.getString("exchange"));
					mainInstrument.setZerodhaInstrumentToken(rs.getLong("zerodha_instrument_token"));
					mainInstrument.setExpiryDay(rs.getInt("expiry_day"));
					mainInstrument.setNoOfFutureExpiryData(rs.getInt("no_of_future_expiry_data"));
					mainInstrument.setNoOfOptionsExpiryData(rs.getInt("no_of_options_expiry_data")); 
					mainInstrument.setNoOfOptionsStrikePoints(rs.getInt("no_of_options_strike_points"));
					mainInstrument.setGapBetweenStrikes(rs.getInt("gap_between_strikes"));
					mainInstrument.setOrderFreezingQuantity(rs.getInt("order_freezing_quantity"));
					mainInstrument.setStraddleMargin(rs.getFloat("straddle_margin"));
				}
				rs.close();
				
				stmt.close();
			} catch (Exception ex) {
				ex.printStackTrace();
			} finally {
				try {
					if (conn!=null) conn.close();
				} catch (SQLException e) {
					log.error(e);
				}
			}
			tradingSymbolMainInstrumentCache.put(keyVal, retInstrument);
			retInstrument = mainInstrument;
			
		}
		return retInstrument;
	}
	
	public static String getTradingSymbolExchangeCache(String keyVal) {
		return tradingSymbolExchangeCache.get(keyVal);
	}
	
	public static OptionFnOInstrument getTradingSymbolToOptionInstrument(String keyVal) {
		return tradingSymbolToOptionInstrument.get(keyVal);
	}
	
	
}
