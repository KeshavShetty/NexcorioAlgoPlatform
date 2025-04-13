package com.nexcorio.algo.kite;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.dto.MainInstruments;
import com.nexcorio.algo.dto.OptionFnOInstrument;

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
		return tradingSymbolMainInstrumentCache.get(keyVal);
	}
	
	public static String getTradingSymbolExchangeCache(String keyVal) {
		return tradingSymbolExchangeCache.get(keyVal);
	}
	
	public static OptionFnOInstrument getTradingSymbolToOptionInstrument(String keyVal) {
		return tradingSymbolToOptionInstrument.get(keyVal);
	}
	
	
}
