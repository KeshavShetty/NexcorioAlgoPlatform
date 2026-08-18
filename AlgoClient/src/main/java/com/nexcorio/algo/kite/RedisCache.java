package com.nexcorio.algo.kite;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.nexcorio.algo.dto.MainInstruments;
import com.nexcorio.algo.dto.OptionFnOInstrument;
import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.util.db.HDataSource;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;
import redis.clients.jedis.Response;


public class RedisCache {

	private static final Logger log = LogManager.getLogger(RedisCache.class);
	
	private static final JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), "localhost", 6379);
	private static final //Gson gson = new GsonBuilder().setLenient().create();
	Gson gson = new GsonBuilder()
	        .setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX") // Supports milliseconds and ISO timezones
	        .create();
	
	@SuppressWarnings("unchecked")
	public static Object getGenericObject(String cacheKey, Class classname) {
		Object retObj = null;
		//System.out.println("cacheKey="+cacheKey);
		try (Jedis jedis = jedisPool.getResource()) {
			String cachedJson = jedis.get(cacheKey);
			if (cachedJson != null) {
				retObj = gson.fromJson(cachedJson, classname);
			}
		} catch (Exception e) {
			System.err.println("Redis error for " + cacheKey + e.getMessage());
			e.printStackTrace();
		}
		return retObj;
	}
	
	/**
	 * 		InstrumentTokenToTradingSymbol
	 */
	public static String getInstrumentTokenToTradingSymbolCache(Long keyVal) {
		return (String) getGenericObject("InstrumentTokenToTradingSymbol:" + keyVal, String.class);
	}
	
	public static void putInstrumentTokenToTradingSymbolCache(Long keyVal, String value) {
		String cacheKey = "InstrumentTokenToTradingSymbol:" + keyVal;
		
		try (Jedis jedis = jedisPool.getResource()) {
			 String jsonToCache = gson.toJson(value);
             jedis.set(cacheKey, jsonToCache, redis.clients.jedis.params.SetParams.setParams().ex(10*60*60)); 
        } catch (Exception e) {
            System.err.println("Redis error, putInstrumentTokenToTradingSymbolCache: " + e.getMessage());
        }
	}
	
	
	public static String getTradingSymbolExchangeCache(String keyVal) {
		return (String) getGenericObject("TradingSymbolExchange:" + keyVal, String.class);
	}
	
	public static void putTradingSymbolExchangeCache(String keyVal, String value) {
		String cacheKey = "TradingSymbolExchange:" + keyVal;
		
		try (Jedis jedis = jedisPool.getResource()) {
			 String jsonToCache = gson.toJson(value);
             jedis.set(cacheKey, jsonToCache, redis.clients.jedis.params.SetParams.setParams().ex(10*60*60)); 
        } catch (Exception e) {
            System.err.println("Redis error, putTradingSymbolExchangeCache: " + e.getMessage());
        }
	}

	public static OptionFnOInstrument getTradingSymbolToOptionInstrument(String keyVal) {
		return (OptionFnOInstrument) getGenericObject("TradingSymbolToOptionInstrument:" + keyVal, OptionFnOInstrument.class);
	}
	
	public static void putTradingSymbolToOptionInstrument(String keyVal, OptionFnOInstrument value) {
		String cacheKey = "TradingSymbolToOptionInstrument:" + keyVal;
		
		try (Jedis jedis = jedisPool.getResource()) {
			 String jsonToCache = gson.toJson(value);
             jedis.set(cacheKey, jsonToCache, redis.clients.jedis.params.SetParams.setParams().ex(10*60*60)); 
        } catch (Exception e) {
            System.err.println("Redis error, putTradingSymbolExchangeCache: " + e.getMessage());
        }
	}
	
	public static void putTradingSymbolMainInstrumentCache(String keyVal, MainInstruments value) {
		String cacheKey = "TradingSymbolMainInstrument:" + keyVal;
		
		try (Jedis jedis = jedisPool.getResource()) {
			 String jsonToCache = gson.toJson(value);
             jedis.set(cacheKey, jsonToCache, redis.clients.jedis.params.SetParams.setParams().ex(10*60*60)); 
        } catch (Exception e) {
            System.err.println("Redis error, putTradingSymbolExchangeCache: " + e.getMessage());
        }
	}
	
	public static MainInstruments getTradingSymbolMainInstrumentCache(String keyVal) {
		MainInstruments retInstrument = (MainInstruments) getGenericObject("TradingSymbolMainInstrument:" + keyVal, MainInstruments.class);
		
		if (retInstrument==null) {
			
			MainInstruments mainInstrument = null;
			Connection conn = null;
			Statement stmt = null;
			try {
				conn = HDataSource.getReadOnlyConnection();
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
				putTradingSymbolMainInstrumentCache(keyVal, mainInstrument);
				
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
			retInstrument = mainInstrument;
		}
		return retInstrument;
	}
	
	public static Float getTickPrice(String keyVal) {
		return (Float) getGenericObject("TickPrice:" + keyVal, Float.class);
	}
	
	public static void putTickPrice(String keyVal, Float value) {
		String cacheKey = "TickPrice:" + keyVal;
		
		try (Jedis jedis = jedisPool.getResource()) {
			 String jsonToCache = gson.toJson(value);
             jedis.set(cacheKey, jsonToCache, redis.clients.jedis.params.SetParams.setParams().ex(10*60)); 
        } catch (Exception e) {
            System.err.println("Redis error, putTradingSymbolExchangeCache: " + e.getMessage());
            e.printStackTrace();
        }
	}
	
	
	public static OptionGreek getOptionGreek(String keyVal) {
		return (OptionGreek) getGenericObject("OptionGreek:" + keyVal, OptionGreek.class);
	}
	
	public static void putOptionGreek(String keyVal, OptionGreek value) {
		String cacheKey = "OptionGreek:" + keyVal;
		
		try (Jedis jedis = jedisPool.getResource()) {
			 String jsonToCache = gson.toJson(value);
             jedis.set(cacheKey, jsonToCache, redis.clients.jedis.params.SetParams.setParams().ex(10*60)); 
        } catch (Exception e) {
        	e.printStackTrace();
            System.err.println("Redis error, putTradingSymbolExchangeCache: " + e.getMessage());
        }
	}
	
	
	public static Long getSnapshotId(String keyVal) {
		return (Long) getGenericObject("SnapshotId:" + keyVal, Long.class);
	}
	
	public static void putSnapshotId(String keyVal, Long value) {
		String cacheKey = "SnapshotId:" + keyVal;
		
		try (Jedis jedis = jedisPool.getResource()) {
			 String jsonToCache = gson.toJson(value);
             jedis.set(cacheKey, jsonToCache, redis.clients.jedis.params.SetParams.setParams().ex(10*60)); 
        } catch (Exception e) {
        	e.printStackTrace();
            System.err.println("Redis error, putTradingSymbolExchangeCache: " + e.getMessage());
        }
	}
	
	
	
	public static MainInstruments getMainInstrumentsById(String keyVal) {
		return (MainInstruments) getGenericObject("MainInstrumentsById:" + keyVal, MainInstruments.class);
	}
	
	public static void putMainInstrumentsById(String keyVal, MainInstruments value) {
		String cacheKey = "MainInstrumentsById:" + keyVal;
		
		try (Jedis jedis = jedisPool.getResource()) {
			 String jsonToCache = gson.toJson(value);
             jedis.set(cacheKey, jsonToCache, redis.clients.jedis.params.SetParams.setParams().ex(30*60)); 
        } catch (Exception e) {
        	e.printStackTrace();
            System.err.println("Redis error, putTradingSymbolExchangeCache: " + e.getMessage());
        }
	}
	
	
	public static List<OptionGreek> getMatchingOptionGreek(String targetPattern) {
		// The specific pattern to match "OptionGreek:NIFTY*"
        Set<String> discoveredKeys = new HashSet<String>();

        // Step 1: Scan for the keys matching "Product:1*"
        try (Jedis jedis = jedisPool.getResource()) {
            String cursor = ScanParams.SCAN_POINTER_START; // Initialized to "0"
            ScanParams scanParams = new ScanParams().match(targetPattern).count(100);

            do {
                ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
                discoveredKeys.addAll(scanResult.getResult());
                cursor = scanResult.getCursor(); // Track the next block position
            } while (!cursor.equals(ScanParams.SCAN_POINTER_START));
        } catch (Exception e) {
            System.err.println("Scan error: " + e.getMessage());
            e.printStackTrace();
            return new ArrayList<OptionGreek>();
        }

        if (discoveredKeys.isEmpty()) {
            return new ArrayList<>();
        }

        // Step 2: Use a pipeline to fetch all data contents concurrently
        List<OptionGreek> matches = new ArrayList<>();
        try (Jedis jedis = jedisPool.getResource()) {
            Pipeline pipeline = jedis.pipelined();
            List<Response<String>> dataFutures = new ArrayList<>();

            for (String key : discoveredKeys) {
                dataFutures.add(pipeline.get(key));
            }
            pipeline.sync(); // Run all calls in one single network blast

            // Step 3: Map JSON string arrays directly to your Object definitions
            for (Response<String> futureJson : dataFutures) {
                String rawJson = futureJson.get();
                if (rawJson != null) {
                    matches.add(gson.fromJson(rawJson, OptionGreek.class));
                }
            }
        } catch (Exception e) {
            System.err.println("Pipeline error: " + e.getMessage());
        }

        return matches;
	}
	
	
	
	
	
	
	public static void main(String[] args) {
		
		putOptionGreek("NIFTY123", new OptionGreek("NIFTY123", 0, 0, 0, 0, 0));
		putOptionGreek("NIFTY124", new OptionGreek("NIFTY124", 0, 0, 0, 0, 0));
		putOptionGreek("NIFTY13",  new OptionGreek("NIFTY13", 0, 0, 0, 0, 0));
		putOptionGreek("NIFTY2",  new OptionGreek("NIFTY2", 0, 0, 0, 0, 0));
		
		List<OptionGreek> optionGreeks = getMatchingOptionGreek("OptionGreek:NIFTY2*");
		for(int i=0;i<optionGreeks.size();i++) {
			System.out.println("1->" +  optionGreeks.get(i).getTradingSymbol());	
		}		
        // Clean up pool when application shuts down
        jedisPool.close();
    }
}
