package com.nexcorio.algo.kite;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.nexcorio.algo.dto.MainInstruments;
import com.nexcorio.algo.dto.OptionFnOInstrument;
import com.nexcorio.algo.dto.OptionGreek;

/**
 * 
 * @author Keshav Shetty
 *
 */
public class CaffeineCache {
	
	public static Cache<Long, String> instrumentTokenToTradingSymbolCache    = Caffeine.newBuilder().expireAfterWrite(10*60, TimeUnit.MINUTES).maximumSize(10000).build();
	public static Cache<String, String> tradingSymbolExchangeCache 		     = Caffeine.newBuilder().expireAfterWrite(10*60, TimeUnit.MINUTES).maximumSize(10000).build();
	public static Cache<String, OptionFnOInstrument> tradingSymbolToOptionInstrumentCache = Caffeine.newBuilder().expireAfterWrite(10*60, TimeUnit.MINUTES).maximumSize(10000).build();
	public static Cache<String, MainInstruments> tradingSymbolMainInstrumentCache = Caffeine.newBuilder().expireAfterWrite(10*60, TimeUnit.MINUTES).maximumSize(10000).build();
	public static Cache<String, MainInstruments> mainInstrumentsByIdCache = Caffeine.newBuilder().expireAfterWrite(10*60, TimeUnit.MINUTES).maximumSize(10000).build();
	public static Cache<String, Long> snapshotIdCache = Caffeine.newBuilder().expireAfterWrite(10*60, TimeUnit.MINUTES).maximumSize(10000).build();
	
	public static Cache<String, Float> tickPriceCache = Caffeine.newBuilder().expireAfterWrite(1, TimeUnit.SECONDS).maximumSize(10000).build();
	public static Cache<String, OptionGreek> optionGreekCache = Caffeine.newBuilder().expireAfterWrite(1, TimeUnit.SECONDS).maximumSize(10000).build();
	
	public static String getInstrumentTokenToTradingSymbolCache(Long keyVal) {
		return instrumentTokenToTradingSymbolCache.getIfPresent(keyVal);
	}
	
	public static void putInstrumentTokenToTradingSymbolCache(Long keyVal, String value) {
		instrumentTokenToTradingSymbolCache.put(keyVal, value);
	}
	
	public static String getTradingSymbolExchangeCache(String keyVal) {
		return tradingSymbolExchangeCache.getIfPresent(keyVal);
	}
	
	public static void putTradingSymbolExchangeCache(String keyVal, String value) {
		tradingSymbolExchangeCache.put(keyVal, value);
	}

	public static OptionFnOInstrument getTradingSymbolToOptionInstrument(String keyVal) {
		return tradingSymbolToOptionInstrumentCache.getIfPresent(keyVal);
	}
	
	public static void putTradingSymbolToOptionInstrument(String keyVal, OptionFnOInstrument value) {
		tradingSymbolToOptionInstrumentCache.put(keyVal, value);
	}
	
	public static void putTradingSymbolMainInstrumentCache(String keyVal, MainInstruments value) {
		tradingSymbolMainInstrumentCache.put(keyVal, value);
	}
	
	public static MainInstruments getTradingSymbolMainInstrumentCache(String keyVal) {
		return tradingSymbolMainInstrumentCache.getIfPresent(keyVal);
	}
	
	public static Float getTickPrice(String keyVal) {
		return tickPriceCache.getIfPresent(keyVal);
	}
	
	public static void putTickPrice(String keyVal, Float value) {
		tickPriceCache.put(keyVal, value);
	}
	
	public static OptionGreek getOptionGreek(String keyVal) {
		return optionGreekCache.getIfPresent(keyVal);
	}
	
	public static void putOptionGreek(String keyVal, OptionGreek value) {
		optionGreekCache.put(keyVal, value);
	}
	
	public static Long getSnapshotId(String keyVal) {
		return snapshotIdCache.getIfPresent(keyVal);
	}
	
	public static void putSnapshotId(String keyVal, Long value) {
		snapshotIdCache.put(keyVal, value);
	}
	
	public static MainInstruments getMainInstrumentsById(String keyVal) {
		return mainInstrumentsByIdCache.getIfPresent(keyVal);
	}
	
	public static void putMainInstrumentsById(String keyVal, MainInstruments value) {
		mainInstrumentsByIdCache.put(keyVal, value);
	}
	
	public static List<OptionGreek> getMatchingOptionGreek(String mainInstrumentShortName) {
		List<OptionGreek> retList = new ArrayList<OptionGreek>();
		
		ConcurrentMap<String, OptionGreek> caffeineObjects =CaffeineCache.optionGreekCache.asMap();
		
		Iterator<String> iter = caffeineObjects.keySet().iterator();
		while(iter.hasNext()) {
			String keyStr = iter.next();
			
			if (keyStr.startsWith(mainInstrumentShortName)) {
				retList.add(caffeineObjects.get(keyStr));
			}
		}
		return retList;
	}
}
