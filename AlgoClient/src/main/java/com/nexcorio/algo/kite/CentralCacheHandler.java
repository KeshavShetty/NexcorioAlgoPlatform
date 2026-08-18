package com.nexcorio.algo.kite;

import java.util.List;

import com.nexcorio.algo.dto.MainInstruments;
import com.nexcorio.algo.dto.OptionFnOInstrument;
import com.nexcorio.algo.dto.OptionGreek;

/**
 * 
 * @author Keshav Shetty
 *
 */
public class CentralCacheHandler {
	
	public static String getInstrumentTokenToTradingSymbolCache(Long keyVal) {
		String retObj = CaffeineCache.getInstrumentTokenToTradingSymbolCache(keyVal);
		if (retObj==null) {
			retObj = RedisCache.getInstrumentTokenToTradingSymbolCache(keyVal);
			if (retObj!=null) {
				CaffeineCache.putInstrumentTokenToTradingSymbolCache(keyVal, retObj);
			}
		}
		return retObj;
	}
	
	public static void putInstrumentTokenToTradingSymbolCache(Long keyVal, String value) {
		CaffeineCache.putInstrumentTokenToTradingSymbolCache(keyVal, value);
		RedisCache.putInstrumentTokenToTradingSymbolCache(keyVal, value);
	}
	
	public static String getTradingSymbolExchangeCache(String keyVal) {
		String retObj = CaffeineCache.getTradingSymbolExchangeCache(keyVal);
		if (retObj==null) {
			retObj = RedisCache.getTradingSymbolExchangeCache(keyVal);
			if (retObj!=null) {
				CaffeineCache.putTradingSymbolExchangeCache(keyVal, retObj);
			}
		}
		return retObj;
	}
	
	public static void putTradingSymbolExchangeCache(String keyVal, String value) {
		CaffeineCache.putTradingSymbolExchangeCache(keyVal, value);
		RedisCache.putTradingSymbolExchangeCache(keyVal, value);
	}

	public static OptionFnOInstrument getTradingSymbolToOptionInstrument(String keyVal) {
		OptionFnOInstrument retObj = CaffeineCache.getTradingSymbolToOptionInstrument(keyVal);
		if (retObj==null) {
			retObj = RedisCache.getTradingSymbolToOptionInstrument(keyVal);
			if (retObj!=null) {
				CaffeineCache.putTradingSymbolToOptionInstrument(keyVal, retObj);
			}
		}
		return retObj;
	}
	
	public static void putTradingSymbolToOptionInstrument(String keyVal, OptionFnOInstrument value) {
		CaffeineCache.putTradingSymbolToOptionInstrument(keyVal, value);
		RedisCache.putTradingSymbolToOptionInstrument(keyVal, value);
	}
	
	public static MainInstruments getTradingSymbolMainInstrumentCache(String keyVal) {
		MainInstruments retObj = CaffeineCache.getTradingSymbolMainInstrumentCache(keyVal);
		if (retObj==null) {
			retObj = RedisCache.getTradingSymbolMainInstrumentCache(keyVal);
			if (retObj!=null) {
				CaffeineCache.putTradingSymbolMainInstrumentCache(keyVal, retObj);
			}
		}
		return retObj;
	}

	public static void putTradingSymbolMainInstrumentCache(String keyVal, MainInstruments value) {
		CaffeineCache.putTradingSymbolMainInstrumentCache(keyVal, value);
		RedisCache.putTradingSymbolMainInstrumentCache(keyVal, value);
	}
	
	public static Float getTickPrice(String keyVal) {
		Float retObj = CaffeineCache.getTickPrice(keyVal);
		if (retObj==null) {
			retObj = RedisCache.getTickPrice(keyVal);
			if (retObj!=null) {
				CaffeineCache.putTickPrice(keyVal, retObj);
			}
		}
		return retObj;
	}
	
	public static void putTickPrice(String keyVal, Float value) {
		CaffeineCache.putTickPrice(keyVal, value);
		RedisCache.putTickPrice(keyVal, value);
	}
	
	public static OptionGreek getOptionGreek(String keyVal) {
		OptionGreek retObj = CaffeineCache.getOptionGreek(keyVal);
		if (retObj==null) {
			retObj = RedisCache.getOptionGreek(keyVal);
			if (retObj!=null) {
				CaffeineCache.putOptionGreek(keyVal, retObj);
			}
		}
		return retObj;
	}
	
	public static void putOptionGreek(String keyVal, OptionGreek value) {
		CaffeineCache.putOptionGreek(keyVal, value);
		RedisCache.putOptionGreek(keyVal, value);
	}
	
	public static Long getSnapshotId(String keyVal) {
		Long retObj = CaffeineCache.getSnapshotId(keyVal);
		if (retObj==null) {
			retObj = RedisCache.getSnapshotId(keyVal);
			if (retObj!=null) {
				CaffeineCache.putSnapshotId(keyVal, retObj);
			}
		}
		return retObj;
	}
	
	public static void putSnapshotId(String keyVal, Long value) {
		CaffeineCache.putSnapshotId(keyVal, value);
		RedisCache.putSnapshotId(keyVal, value);
	}
	
	public static MainInstruments getMainInstrumentsById(String keyVal) {
		MainInstruments retObj = CaffeineCache.getMainInstrumentsById(keyVal);
		if (retObj==null) {
			retObj = RedisCache.getMainInstrumentsById(keyVal);
			if (retObj!=null) {
				CaffeineCache.putMainInstrumentsById(keyVal, retObj);
			}
		}
		return retObj;
	}
	
	public static void putMainInstrumentsById(String keyVal, MainInstruments value) {
		CaffeineCache.putMainInstrumentsById(keyVal, value);
		RedisCache.putMainInstrumentsById(keyVal, value);
	}
	
	
	public static List<OptionGreek> getMatchingOptionGreek(String mainInstrumentShortName) {
		
//		List<OptionGreek> retList = CaffeineCache.getMatchingOptionGreek(mainInstrumentShortName);
//		
//		if (retList.size()==0) {
//			retList = RedisCache.getMatchingOptionGreek("OptionGreek:"+mainInstrumentShortName+"*");
//		}
//		return retList;
		return RedisCache.getMatchingOptionGreek("OptionGreek:"+mainInstrumentShortName+"*");
	}
	
	
//	private static final Logger log = LogManager.getLogger(KiteCache.class);
//	
//	private static Map<Long, String> instrumentTokenToTradingSymbolCache = new HashMap<Long, String>(); 
//	private static Map<String, MainInstruments> tradingSymbolMainInstrumentCache = new HashMap<String, MainInstruments>();
//	private static Map<String, String> tradingSymbolExchangeCache= new HashMap<String, String>();
//	
//	private static Map<String, OptionFnOInstrument> tradingSymbolToOptionInstrument = new HashMap<String, OptionFnOInstrument>();
//	
//	public static Cache<String, Float> tickPriceCache = Caffeine.newBuilder()
//			  .expireAfterWrite(10, TimeUnit.MINUTES)
//			  .maximumSize(10000)
//			  .build();
//	
//	public static Cache<String, OptionGreek> optionGreekCache = Caffeine.newBuilder()
//			  .expireAfterWrite(10, TimeUnit.MINUTES)
//			  .maximumSize(10000)
//			  .build();
//
//	public static Cache<String, Long> snapshotIdCache = Caffeine.newBuilder()
//			  .expireAfterAccess(10, TimeUnit.MINUTES)
//			  .maximumSize(10000)
//			  .build();
//	
//	public static Cache<Long, MainInstruments> mainInstrumentsByIdCache = Caffeine.newBuilder()
//			  .expireAfterAccess(30, TimeUnit.MINUTES)
//			  .maximumSize(10000)
//			  .build();
//
//	
//	public static void putInstrumentTokenToTradingSymbolCache(Long keyVal, String value) {
//		instrumentTokenToTradingSymbolCache.put(keyVal, value);
//	}
//	
//	public static void putTradingSymbolMainInstrumentCache(String keyVal, MainInstruments mainInstrument) {
//		tradingSymbolMainInstrumentCache.put(keyVal, mainInstrument);
//	}
//	
//	public static void putTradingSymbolExchangeCache(String keyVal, String value) {
//		tradingSymbolExchangeCache.put(keyVal, value);
//	}
//
//	public static void putTradingSymbolToOptionInstrument(String keyVal, OptionFnOInstrument value) {
//		tradingSymbolToOptionInstrument.put(keyVal, value);
//	}
//	
//	public static String getInstrumentTokenToTradingSymbolCache(Long keyVal) {
//		return instrumentTokenToTradingSymbolCache.get(keyVal);
//	}
//	
//	public static MainInstruments getTradingSymbolMainInstrumentCache(String keyVal) {
//		MainInstruments retInstrument = tradingSymbolMainInstrumentCache.get(keyVal);
//		if (retInstrument==null) {
//			
//			MainInstruments mainInstrument = null;
//			Connection conn = null;
//			Statement stmt = null;
//			try {
//				conn = HDataSource.getReadOnlyConnection();
//				stmt = conn.createStatement();
//				
//				ResultSet rs = stmt.executeQuery("SELECT id, name, short_name, instrument_type, exchange,"
//						+ " zerodha_instrument_token, expiry_day, gap_between_strikes, order_freezing_quantity,"
//						+ " no_of_future_expiry_data, no_of_options_expiry_data, no_of_options_strike_points, straddle_margin"
//						+ " FROM nexcorio_main_instruments WHERE short_name='"+keyVal+"'"); 
//				while(rs.next()) {
//					mainInstrument = new MainInstruments();
//					mainInstrument.setId(rs.getLong("id"));
//					mainInstrument.setName(rs.getString("name"));
//					mainInstrument.setShortName(rs.getString("short_name"));
//					mainInstrument.setInstrumentType(rs.getString("instrument_type"));
//					mainInstrument.setExchange(rs.getString("exchange"));
//					mainInstrument.setZerodhaInstrumentToken(rs.getLong("zerodha_instrument_token"));
//					mainInstrument.setExpiryDay(rs.getInt("expiry_day"));
//					mainInstrument.setNoOfFutureExpiryData(rs.getInt("no_of_future_expiry_data"));
//					mainInstrument.setNoOfOptionsExpiryData(rs.getInt("no_of_options_expiry_data")); 
//					mainInstrument.setNoOfOptionsStrikePoints(rs.getInt("no_of_options_strike_points"));
//					mainInstrument.setGapBetweenStrikes(rs.getInt("gap_between_strikes"));
//					mainInstrument.setOrderFreezingQuantity(rs.getInt("order_freezing_quantity"));
//					mainInstrument.setStraddleMargin(rs.getFloat("straddle_margin"));
//				}
//				rs.close();
//				
//				stmt.close();
//			} catch (Exception ex) {
//				ex.printStackTrace();
//			} finally {
//				try {
//					if (conn!=null) conn.close();
//				} catch (SQLException e) {
//					log.error(e);
//				}
//			}
//			tradingSymbolMainInstrumentCache.put(keyVal, retInstrument);
//			retInstrument = mainInstrument;
//			
//		}
//		return retInstrument;
//	}
//	
//	public static String getTradingSymbolExchangeCache(String keyVal) {
//		return tradingSymbolExchangeCache.get(keyVal);
//	}
//	
//	public static OptionFnOInstrument getTradingSymbolToOptionInstrument(String keyVal) {
//		return tradingSymbolToOptionInstrument.get(keyVal);
//	}
	
	
}
