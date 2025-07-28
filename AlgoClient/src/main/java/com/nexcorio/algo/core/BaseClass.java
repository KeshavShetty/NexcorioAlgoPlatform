package com.nexcorio.algo.core;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONException;

import com.nexcorio.algo.dto.MainInstruments;
import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.kite.KiteCache;
import com.nexcorio.algo.util.FileLogTelegramWriter;
import com.nexcorio.algo.util.db.HDataSource;
import com.zerodhatech.kiteconnect.KiteConnect;
import com.zerodhatech.kiteconnect.kitehttp.exceptions.KiteException;
import com.zerodhatech.models.LTPQuote;
import com.zerodhatech.models.Margin;

/**
 * 
 * @author Keshav Shetty
 *
 */
public class BaseClass {

	private static final Logger log = LogManager.getLogger(BaseClass.class);
	
	protected float instrumentLtp = 0f;
	
	protected String algoname = null;
	
	protected Long userId = -1L;
	
	protected FileLogTelegramWriter fileLogTelegramWriter = null;
	
	protected Calendar backtestDate = null;
	
	protected boolean exitThread = false;
	protected String exitReason = null;
	
	protected SimpleDateFormat postgresLongDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	protected SimpleDateFormat postgresShortDateFormat = new SimpleDateFormat("yyyy-MM-dd");
	
	protected MainInstruments mainInstrument = null;
	
	protected void sleep(int seconds) {
		if (backtestDate==null)
			try {
				Thread.sleep(1000*seconds);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		else {
			backtestDate.add(Calendar.SECOND, seconds);
			System.out.println("------ Now "+backtestDate!=null?backtestDate.getTime():null);
		}
	}
	
	public Date getDailyCustomTime(int hour, int minute, int second) {
		Calendar cal = Calendar.getInstance();
		if (backtestDate!=null)	cal.setTime(backtestDate.getTime());
		cal.set(Calendar.HOUR_OF_DAY, hour);
		cal.set(Calendar.MINUTE, minute);
		cal.set(Calendar.SECOND, second);
		//System.out.println("In getDailyCustomTime "+  cal.getTime());
		return cal.getTime();
	}
	
	protected boolean timeout(int hour, int minute, int second) {
		boolean retVal = false;
		
		Date refernceDateTime = new Date();
		if (backtestDate!=null) refernceDateTime = backtestDate.getTime();
		if (refernceDateTime.after(getDailyCustomTime(hour, minute, second)) ) retVal = true;
		
		return retVal;
	}
	
	public void prepareExit(String exitMessage) {
		fileLogTelegramWriter.write( "Winding up for the day, "+exitMessage);
		this.exitReason = exitMessage;
		this.exitThread = true;
	}
	
	public float getPriceFromTicks(String instrumentName) {
		float retVal = 0f;
		
		if(backtestDate == null) { // Live data check in cache
			Float priceFromCache = KiteCache.tickPriceCache.getIfPresent(instrumentName);
			if ( priceFromCache != null ) {
				fileLogTelegramWriter.write("Found in cache");
				return priceFromCache;
			}
		}
		
		Connection conn = null;
		try {
			SimpleDateFormat postgresLongDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			String fetchSql = "select quote_time, last_traded_price from nexcorio_tick_data where trading_symbol = '" + instrumentName +"'"
					+ (backtestDate!=null ? ( " and quote_time <='" + postgresLongDateFormat.format(backtestDate.getTime() )+ "'") : "")
					+ " order by quote_time desc limit 1";
			fileLogTelegramWriter.write(fetchSql);
			ResultSet rs = stmt.executeQuery(fetchSql);
			while (rs.next()) {
				retVal = rs.getFloat("last_traded_price");
			}
			rs.close();
			stmt.close();
		} catch (Exception e) {
			log.error("Error"+e.getMessage(),e);
		} finally {
			try {
				if (conn!=null) conn.close();
			} catch (SQLException e) {
				log.error(e);
			}
		}
		
		return retVal;
	}
	
	protected OptionGreek getOptionGreeks(String optionName) {
		
		if (optionName==null || optionName.equals("")) return null;
		
		if(backtestDate == null) { // Live data check in cache
			OptionGreek optionGreekFromCache = KiteCache.optionGreekCache.getIfPresent(optionName);
			if ( optionGreekFromCache != null ) {
				fileLogTelegramWriter.write("Greek Found in cache");
				return optionGreekFromCache;
			}
		}
		
		OptionGreek retVal = null;
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			String fetchSql = "select iv, delta, vega, theta, gamma, ltp, oi from nexcorio_option_greeks  where trading_symbol = '" + optionName + "'"
					+ ( backtestDate!=null ? ( " and quote_time <='" + postgresLongDateFormat.format(backtestDate.getTime())+ "'") : "" )
					+ " order by quote_time desc limit 1";
			fileLogTelegramWriter.write("In getOptionGreeks fetchSql="+fetchSql);
			ResultSet rs = stmt.executeQuery(fetchSql);
			while (rs.next()) {
				retVal = new OptionGreek(optionName, rs.getFloat("iv"), rs.getFloat("delta"), rs.getFloat("vega"), rs.getFloat("theta"), rs.getFloat("gamma"), rs.getFloat("ltp"), rs.getFloat("oi"));
			}
			rs.close();
			stmt.close();
			//System.out.println("retVal="+retVal);
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error"+e.getMessage(),e);
		} finally {
			try {
				if (conn!=null) conn.close();
			} catch (SQLException e) {
				log.error(e);
			}
		}
		return retVal;
	}
	
	protected String getCurrentWeekExpiryOptionnamePrefix() {
		String retStr = "";
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			String fnoExchange = "NFO-OPT";
			if (mainInstrument.getExchange().equalsIgnoreCase("BSE")) fnoExchange = "BFO-OPT";
			
			Calendar cal = Calendar.getInstance();
			if (backtestDate!=null) cal.setTime(backtestDate.getTime());
			cal.add(Calendar.DATE, -1);
			
			String fetchSql = "SELECT fno_prefix from nexcorio_fno_expiry_dates WHERE f_main_instrument="+mainInstrument.getId()+ ""
					+ " and fno_segment='" + fnoExchange + "' "
					+ " and expiry_date > '" + postgresShortDateFormat.format(cal.getTime()) + "' "
					+ " ORDER BY expiry_date ASC LIMIT 1";
			fileLogTelegramWriter.write("Fetch sql="+fetchSql);
			
			ResultSet rs = stmt.executeQuery(fetchSql);
			while (rs.next()) {
				retStr = rs.getString("fno_prefix");
			}
			rs.close();
			stmt.close();
		} catch(Exception ex) {
			ex.printStackTrace();
			log.error("Error"+ex.getMessage(),ex);
		}finally {
			try {
				if (conn!=null) conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		fileLogTelegramWriter.write("In getCurrentWeekExpiryOptionnamePrefix retStr="+retStr);
		return retStr;
	}
	
	protected String getCurrentWeekExpiryOptionnamePrefix(Long mainInstrumentId) {
		String retStr = "";
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			String fnoExchange = "NFO-OPT";
			if (mainInstrument.getExchange().equalsIgnoreCase("BSE")) fnoExchange = "BFO-OPT";
			
			Calendar cal = Calendar.getInstance();
			if (backtestDate!=null) cal.setTime(backtestDate.getTime());
			cal.add(Calendar.DATE, -1);
			
			String fetchSql = "SELECT fno_prefix from nexcorio_fno_expiry_dates WHERE f_main_instrument="+mainInstrumentId+ ""
					+ " and fno_segment='" + fnoExchange + "' "
					+ " and expiry_date > '" + postgresShortDateFormat.format(cal.getTime()) + "' "
					+ " ORDER BY expiry_date ASC LIMIT 1";
			fileLogTelegramWriter.write("Fetch sql="+fetchSql);
			
			ResultSet rs = stmt.executeQuery(fetchSql);
			while (rs.next()) {
				retStr = rs.getString("fno_prefix");
			}
			rs.close();
			stmt.close();
		} catch(Exception ex) {
			ex.printStackTrace();
			log.error("Error"+ex.getMessage(),ex);
		}finally {
			try {
				if (conn!=null) conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		fileLogTelegramWriter.write("In getCurrentWeekExpiryOptionnamePrefix retStr="+retStr);
		return retStr;
	}
	
	protected Date getOptionCurrentWeekExpiryDate() {
		Date expiryDate = null;
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			String fnoExchange = "NFO-OPT";
			if (mainInstrument.getExchange().equalsIgnoreCase("BSE")) fnoExchange = "BFO-OPT";
			
			Calendar cal = Calendar.getInstance();
			if (backtestDate!=null) cal.setTime(backtestDate.getTime());
			cal.add(Calendar.DATE, -1);
			
			String fetchSql = "SELECT expiry_date from nexcorio_fno_expiry_dates WHERE f_main_instrument="+mainInstrument.getId()+ ""
					+ " and fno_segment='" + fnoExchange + "' "
					+ " and expiry_date > '" + postgresShortDateFormat.format(cal.getTime()) + "' "
					+ " ORDER BY expiry_date ASC LIMIT 1";
			fileLogTelegramWriter.write("Fetch sql="+fetchSql);
			
			ResultSet rs = stmt.executeQuery(fetchSql);
			while (rs.next()) {
				expiryDate = rs.getDate("expiry_date");
			}
			rs.close();
			stmt.close();
		} catch(Exception ex) {
			ex.printStackTrace();
			log.error("Error"+ex.getMessage(),ex);
		}finally {
			try {
				if (conn!=null) conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		fileLogTelegramWriter.write("In getOptionCurrentWeekExpiryDate retStr="+expiryDate);
		return expiryDate;
	}
	
	private float getGreekValue(String greekname, OptionGreek optionGreek) {
		if (greekname.equalsIgnoreCase("delta")) return Math.abs(optionGreek.getDelta());
		if (greekname.equalsIgnoreCase("vega"))  return Math.abs(optionGreek.getVega());
		if (greekname.equalsIgnoreCase("theta")) return Math.abs(optionGreek.getTheta());
		if (greekname.equalsIgnoreCase("gamma")) return Math.abs(optionGreek.getGamma());
		if (greekname.equalsIgnoreCase("iv"))    return Math.abs(optionGreek.getIv());
		if (greekname.equalsIgnoreCase("ltp"))   return Math.abs(optionGreek.getLtp());
		if (greekname.equalsIgnoreCase("delta/gamma"))   return Math.abs(optionGreek.getDelta()/optionGreek.getGamma());
		return 0;
	}
	
	private float getGreekDiff(String greekname, OptionGreek optionGreek1, OptionGreek optionGreek2) {
		if (greekname.equalsIgnoreCase("delta")) return Math.abs(optionGreek1.getDelta()-optionGreek2.getDelta());
		if (greekname.equalsIgnoreCase("vega"))  return Math.abs(optionGreek1.getVega()-optionGreek2.getVega());
		if (greekname.equalsIgnoreCase("theta")) return Math.abs(optionGreek1.getTheta()-optionGreek2.getTheta());
		if (greekname.equalsIgnoreCase("gamma")) return Math.abs(optionGreek1.getGamma()-optionGreek2.getGamma());
		if (greekname.equalsIgnoreCase("iv"))    return Math.abs(optionGreek1.getIv()-optionGreek2.getIv());
		if (greekname.equalsIgnoreCase("ltp"))   return Math.abs(optionGreek1.getLtp()-optionGreek2.getLtp());
		if (greekname.equalsIgnoreCase("delta/gamma"))   return Math.abs(optionGreek1.getDelta()/optionGreek1.getGamma() - optionGreek2.getDelta()/optionGreek2.getGamma());
		return 0;
	}
	
	protected String[] getStraddleOptionNamesByGreekOptimised(String greekname, float baseDelta, int hedgeDistance) {
		
		String[] retStr = null;
		
		String[] entryStraddleOptionNames1 = getStraddleOptionNamesByDeltaOptimised(baseDelta, 0);
		
		OptionGreek ceOptionGreek = getOptionGreeks(entryStraddleOptionNames1[0]);
		OptionGreek peOptionGreek = getOptionGreeks(entryStraddleOptionNames1[1]);		
		float diff1 = getGreekDiff(greekname, ceOptionGreek, peOptionGreek);
		
		String[] entryStraddleOptionNames2 = getStraddleOptionNamesByGreek(greekname, getGreekValue(greekname, ceOptionGreek), 0);
		float diff2 = getGreekDiff(greekname, getOptionGreeks(entryStraddleOptionNames2[0]), getOptionGreeks(entryStraddleOptionNames2[1]));
		
		String[] entryStraddleOptionNames3 = getStraddleOptionNamesByGreek(greekname, getGreekValue(greekname, peOptionGreek), 0);
		float diff3 = getGreekDiff(greekname, getOptionGreeks(entryStraddleOptionNames3[0]), getOptionGreeks(entryStraddleOptionNames3[1]));
		
		String localCeHedgeOptionName =  "";
		String localPeHedgeOptionName =  "";
		if (hedgeDistance>0) {
			String optionnamePrefix = getCurrentWeekExpiryOptionnamePrefix();
			int centerStrike = getOptionCenterStrike(optionnamePrefix);
			localCeHedgeOptionName =  optionnamePrefix + (centerStrike+hedgeDistance) + "CE";
			localPeHedgeOptionName =  optionnamePrefix + (centerStrike-hedgeDistance) + "PE";
		} 
		fileLogTelegramWriter.write("diff1="+diff1+" diff2="+diff2+" diff3="+diff3);
		if (diff1 < diff2) {
			if (diff1 < diff3) {
				// Diff1 lowest
				retStr = new String[]{entryStraddleOptionNames1[0], entryStraddleOptionNames1[1], localCeHedgeOptionName, localPeHedgeOptionName};
			} else {
				// Diff3 loest
				retStr = new String[]{entryStraddleOptionNames3[0], entryStraddleOptionNames3[1], localCeHedgeOptionName, localPeHedgeOptionName};
			}
		} else {
			if (diff2 < diff3) {
				// Diff2 lowest
				retStr = new String[]{entryStraddleOptionNames2[0], entryStraddleOptionNames2[1], localCeHedgeOptionName, localPeHedgeOptionName};
			} else {
				// Diff3 loest
				retStr = new String[]{entryStraddleOptionNames3[0], entryStraddleOptionNames3[1], localCeHedgeOptionName, localPeHedgeOptionName};
			}
		}
		return retStr;
	}
	
	private String[] getStraddleOptionNamesByGreek(String greekname, float requiredValue, int hedgeDistance) {
		String[] retStr = null;
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
	
			String optionnamePrefix = getCurrentWeekExpiryOptionnamePrefix();
			
			String ceTradingSymbol = null;
			float ceGreek = 0f;
			
			if (backtestDate == null) {			
				String fetchSql = "select trading_symbol, " + greekname + " as greek, abs(" + requiredValue + "-abs(" + greekname+ ")) as greekDiff from nexcorio_option_snapshot where trading_symbol like '" + optionnamePrefix + "%CE' "	
						+ " and record_date = '" + postgresShortDateFormat.format(getCurrentTime()) + "'"
						+ " order by greekDiff limit 1";
				fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
				
				ResultSet rs = stmt.executeQuery(fetchSql);
				
				while (rs.next()) {
					ceTradingSymbol = rs.getString("trading_symbol");
					ceGreek = rs.getFloat("greek");
				}
				rs.close();
			} else {
				String fetchSql = "select trading_symbol, " + greekname + " as greek, abs(" + requiredValue + "-abs(" + greekname+ ")) as greekDiff, quote_time from nexcorio_option_greeks where trading_symbol like '" + optionnamePrefix + "%CE' "
						+ " and quote_time <= '"+ postgresLongDateFormat.format(getCurrentTime()) + "'"	
						+ " and quote_time >  '"+ postgresLongDateFormat.format(getCurrentTime(-1)) + "'"
						+ " order by greekDiff";
				fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
				
				ResultSet rs = stmt.executeQuery(fetchSql);
				List<String> tradingSymbols = new ArrayList<String>();
				List<Float> delta = new ArrayList<Float>();
				List<Float> deltaDiff = new ArrayList<Float>();
				List<Date> quote_times = new ArrayList<Date>();
				while (rs.next()) {
					tradingSymbols.add(rs.getString("trading_symbol"));
					delta.add(rs.getFloat("greek"));
					deltaDiff.add(rs.getFloat("greekDiff"));
					quote_times.add(rs.getDate("quote_time"));
				}
				rs.close();
				if (tradingSymbols.size()==1) {
					ceTradingSymbol = tradingSymbols.get(0);
					ceGreek = delta.get(0);
				} else {
					for(int i=0;i<tradingSymbols.size()-1;i++) {
						boolean thisIsBest = true;
						for(int j=1;j<tradingSymbols.size();j++) {
							if (quote_times.get(i).after(quote_times.get(j))
									&& tradingSymbols.get(i).equals(tradingSymbols.get(j))) {
								thisIsBest  = false;
							}
						}
						if (thisIsBest) {
							ceTradingSymbol = tradingSymbols.get(i);
							ceGreek = delta.get(i);
							break;
						}
					}
					if (ceTradingSymbol==null) { // Not found, then use first one
						ceTradingSymbol = tradingSymbols.get(0);
						ceGreek = delta.get(0);
					}
				}
			}
			
			String peTradingSymbol = null;
			float peGreek = 0f;
			if (backtestDate == null) {
				String fetchSql = "select trading_symbol, " + greekname + " as greek, abs(" + requiredValue + "-abs(" + greekname+ ")) as greekDiff from nexcorio_option_snapshot where trading_symbol like '" + optionnamePrefix + "%PE' "					
						+ " and record_date = '" + postgresShortDateFormat.format(getCurrentTime()) + "'"
						+ " order by greekDiff limit 1";
				fileLogTelegramWriter.write("2. fetchSql="+fetchSql);
				
				ResultSet rs = stmt.executeQuery(fetchSql);
				
				while (rs.next()) {
					peTradingSymbol = rs.getString("trading_symbol");
					peGreek = rs.getFloat("greek");
				}
				rs.close();
			} else {
				String fetchSql = "select trading_symbol, " + greekname + " as greek, abs(" + requiredValue + "-abs(" + greekname+ ")) as greekDiff, quote_time from nexcorio_option_greeks where trading_symbol like '" + optionnamePrefix + "%PE' "
						+ " and quote_time <= '"+ postgresLongDateFormat.format(getCurrentTime()) + "'"	
						+ " and quote_time >  '"+ postgresLongDateFormat.format(getCurrentTime(-1)) + "'"
						+ " order by greekDiff";
				fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
				
				ResultSet rs = stmt.executeQuery(fetchSql);
				List<String> tradingSymbols = new ArrayList<String>();
				List<Float> delta = new ArrayList<Float>();
				List<Float> deltaDiff = new ArrayList<Float>();
				List<Date> quote_times = new ArrayList<Date>();
				while (rs.next()) {
					tradingSymbols.add(rs.getString("trading_symbol"));
					delta.add(rs.getFloat("greek"));
					deltaDiff.add(rs.getFloat("greekDiff"));
					quote_times.add(rs.getDate("quote_time"));
				}
				rs.close();
				if (tradingSymbols.size()==1) {
					peTradingSymbol = tradingSymbols.get(0);
					peGreek = delta.get(0);
				} else {
					for(int i=0;i<tradingSymbols.size()-1;i++) {
						boolean thisIsBest = true;
						for(int j=1;j<tradingSymbols.size();j++) {
							if (quote_times.get(i).after(quote_times.get(j))
									&& tradingSymbols.get(i).equals(tradingSymbols.get(j))) {
								thisIsBest  = false;
							}
						}
						if (thisIsBest) {
							peTradingSymbol = tradingSymbols.get(i);
							peGreek = delta.get(i);
							break;
						}
					}
					if (peTradingSymbol==null) { // Not found, then use first one
						peTradingSymbol = tradingSymbols.get(0);
						peGreek = delta.get(0);
					}
				}
			}
			
			stmt.close();
			
			String localCeStraddleOptionName =  ceTradingSymbol;
			String localPeStraddleOptionName =  peTradingSymbol;
			
			String localCeHedgeOptionName =  "";
			String localPeHedgeOptionName =  "";
			if (hedgeDistance>0) {
				int centerStrike = getOptionCenterStrike(optionnamePrefix);
				localCeHedgeOptionName =  optionnamePrefix + (centerStrike+hedgeDistance) + "CE";
				localPeHedgeOptionName =  optionnamePrefix + (centerStrike-hedgeDistance) + "PE";
			} 	
			
			retStr = new String[]{localCeStraddleOptionName, localPeStraddleOptionName, localCeHedgeOptionName, localPeHedgeOptionName};
			fileLogTelegramWriter.write("In getStraddleOptionNamesByGreekOptimised for "+requiredValue +" CE " +ceTradingSymbol +" ceDelta="+ceGreek+", " + peTradingSymbol +" peDelta="+peGreek);
		} catch(Exception ex) {
			ex.printStackTrace();
			log.error("Error"+ex.getMessage(),ex);
		}finally {
			try {
				if (conn!=null) conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return retStr;
	}
	
	protected String[] getStraddleOptionNamesByDeltaOptimisedFromATMData(float requiredDelta, int hedgeDistance) {
		String[] retStr = null;
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			String fetchSql = "select ceoptionname, peoptionname from nexcorio_option_atm_movement_data where f_main_instrument="+this.mainInstrument.getId()
					+ " and record_time <=  '"+ postgresLongDateFormat.format(getCurrentTime()) + "'"
					+ " order by record_time desc LIMIT 1"; 
			fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
			
			String ceTradingSymbol = null;
			String peTradingSymbol = null;
			
			ResultSet rs = stmt.executeQuery(fetchSql);
			while (rs.next()) {
				ceTradingSymbol = rs.getString("ceoptionname");
				peTradingSymbol = rs.getString("peoptionname");
			}
			rs.close();
			
			String localCeHedgeOptionName =  "";
			String localPeHedgeOptionName =  "";
			if (hedgeDistance>0) {
				String optionnamePrefix = getCurrentWeekExpiryOptionnamePrefix();
				int centerStrike = getOptionCenterStrike(optionnamePrefix);
				localCeHedgeOptionName =  optionnamePrefix + (centerStrike+hedgeDistance) + "CE";
				localPeHedgeOptionName =  optionnamePrefix + (centerStrike-hedgeDistance) + "PE";
			}
			retStr = new String[]{ceTradingSymbol, peTradingSymbol, localCeHedgeOptionName, localPeHedgeOptionName};
			
			stmt.close();
		} catch(Exception ex) {
			ex.printStackTrace();
			log.error("Error"+ex.getMessage(),ex);
		}finally {
			try {
				if (conn!=null) conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return retStr;
		
	}
	
	protected String[] getStraddleOptionNamesByDeltaOptimisedFromOptionGreeks(float requiredDelta, int hedgeDistance) {
		String[] retStr = null;
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			String fetchSql = "select DISTINCT(trading_symbol) as trading_symbol from nexcorio_option_greeks"
					+ " where trading_symbol like '" + mainInstrument.getShortName() + "%' "
					+ " and quote_time > '" + postgresShortDateFormat.format(getCurrentTime()) + " 09:15:00'"
					+ " and quote_time < '" + postgresShortDateFormat.format(getCurrentTime()) + " 15:15:00'";
			
			fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
			
			List<String> optionnames = new ArrayList<>();			
			ResultSet rs = stmt.executeQuery(fetchSql);
			while (rs.next()) {
				optionnames.add(rs.getString("trading_symbol"));
			}
			rs.close();
			fileLogTelegramWriter.write("optionnames.size="+optionnames.size());
			
			List<OptionGreek> ceOptionGreeks = new ArrayList<>();
			List<OptionGreek> peOptionGreeks = new ArrayList<>();
			for(String optionname:optionnames ) {
				OptionGreek aGreek = getOptionGreeks(optionname);
				if (aGreek!=null) {
					if (optionname.endsWith("CE")) {
						ceOptionGreeks.add(aGreek);
					} else {
						peOptionGreeks.add(aGreek);
					}
				}
			}
			// First search CE matching required delta
			String optionname = "";
			float minDeltaGap = 1f;
			for(OptionGreek aGreek: ceOptionGreeks) {
				float deltaGap = Math.abs(Math.abs(aGreek.getDelta())-requiredDelta);
				if (deltaGap < minDeltaGap) {
					minDeltaGap = deltaGap;
					optionname = aGreek.getTradingSymbol();
				}
			}
			String ceTradingSymbol = optionname;
			
			// Next search PE matching required delta
			optionname = "";
			minDeltaGap = 1f;
			for(OptionGreek aGreek: peOptionGreeks) {
				float deltaGap = Math.abs(Math.abs(aGreek.getDelta())-requiredDelta);
				if (deltaGap < minDeltaGap) {
					minDeltaGap = deltaGap;
					optionname = aGreek.getTradingSymbol();
				}
			}
			String peTradingSymbol = optionname;
			
			String localCeHedgeOptionName =  "";
			String localPeHedgeOptionName =  "";
			if (hedgeDistance>0) {
				String optionnamePrefix = getCurrentWeekExpiryOptionnamePrefix();
				int centerStrike = getOptionCenterStrike(optionnamePrefix);
				localCeHedgeOptionName =  optionnamePrefix + (centerStrike+hedgeDistance) + "CE";
				localPeHedgeOptionName =  optionnamePrefix + (centerStrike-hedgeDistance) + "PE";
			}
			retStr = new String[]{ceTradingSymbol, peTradingSymbol, localCeHedgeOptionName, localPeHedgeOptionName};
			
			stmt.close();
		} catch(Exception ex) {
			ex.printStackTrace();
			log.error("Error"+ex.getMessage(),ex);
		}finally {
			try {
				if (conn!=null) conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return retStr;
		
	}

	protected String[] getStraddleOptionNamesByDeltaOptimised(float requiredDelta, int hedgeDistance) {
		
		 if (backtestDate != null ) {
			 return getStraddleOptionNamesByDeltaOptimisedFromOptionGreeks(requiredDelta, hedgeDistance) ;
		 } else {
			String[] retStr = null;
			Connection conn = null;
			try {
				conn = HDataSource.getReadOnlyConnection();
				Statement stmt = conn.createStatement();
		
				String optionnamePrefix = getCurrentWeekExpiryOptionnamePrefix();
				
				String ceTradingSymbol = null;
				float ceDelta = 0f;
				
				if (backtestDate == null) {			
					String fetchSql = "select trading_symbol, delta, abs(" + requiredDelta + "-abs(delta)) as deltaDiff from nexcorio_option_snapshot where trading_symbol like '" + optionnamePrefix + "%CE' "	
							+ " and record_date = '" + postgresShortDateFormat.format(getCurrentTime()) + "'"
							+ " order by deltaDiff limit 1";
					fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
					
					ResultSet rs = stmt.executeQuery(fetchSql);
					
					while (rs.next()) {
						ceTradingSymbol = rs.getString("trading_symbol");
						ceDelta = rs.getFloat("delta");
					}
					rs.close();
				} else {
					String fetchSql = "select trading_symbol, delta, abs(" + requiredDelta + "-abs(delta)) as deltaDiff, quote_time from nexcorio_option_greeks where trading_symbol like '" + optionnamePrefix + "%CE' "
							+ " and quote_time <= '"+ postgresLongDateFormat.format(getCurrentTime()) + "'"	
							+ " and quote_time >  '"+ postgresLongDateFormat.format(getCurrentTimeDifferSeconds(-2)) + "'"
							+ " order by id desc"; // by deltaDiff
					fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
					
					ResultSet rs = stmt.executeQuery(fetchSql);
					List<String> tradingSymbols = new ArrayList<String>();
					List<Float> delta = new ArrayList<Float>();
					List<Float> deltaDiff = new ArrayList<Float>();
					while (rs.next()) {
						tradingSymbols.add(rs.getString("trading_symbol"));
						delta.add(rs.getFloat("delta"));
						deltaDiff.add(rs.getFloat("deltaDiff"));
						
						//fileLogTelegramWriter.write("tradingSymbols="+tradingSymbols.get(tradingSymbols.size()-1) + " delta="+delta.get(delta.size()-1) + " deltaDiff="+deltaDiff.get(deltaDiff.size()-1));
						
					}
					rs.close();
					
					// Remove the duplicates
					for(int bottomPt = tradingSymbols.size()-1; bottomPt>0;bottomPt--) {
						for(int topPt = 0; topPt < bottomPt;topPt++) {
							if ( tradingSymbols.get(bottomPt).equals(tradingSymbols.get(topPt)) ) {
								tradingSymbols.remove(bottomPt);
								delta.remove(bottomPt);
								deltaDiff.remove(bottomPt);
								break;
							}
						}
					}				
					
					// Find the best row with minimal delta dif
					float minimalDiff = deltaDiff.get(0);
					int bestPosition = 0;
					for(int i=1;i<deltaDiff.size();i++) {
						if (deltaDiff.get(i) < minimalDiff) {
							minimalDiff = deltaDiff.get(i);
							bestPosition = i;
						}
					}				
					ceTradingSymbol = tradingSymbols.get(bestPosition);
					ceDelta = delta.get(bestPosition);
				}
				
				String peTradingSymbol = null;
				float peDelta = 0f;
				if (backtestDate == null) {
					String fetchSql = "select trading_symbol, delta, abs(" + requiredDelta + "-abs(delta)) as deltaDiff from nexcorio_option_snapshot where trading_symbol like '" + optionnamePrefix + "%PE' "					
							+ " and record_date = '" + postgresShortDateFormat.format(getCurrentTime()) + "'"
							+ " order by deltaDiff limit 1";
					fileLogTelegramWriter.write("2. fetchSql="+fetchSql);
					
					ResultSet rs = stmt.executeQuery(fetchSql);
					
					while (rs.next()) {
						peTradingSymbol = rs.getString("trading_symbol");
						peDelta = rs.getFloat("delta");
					}
					rs.close();
				} else {
					String fetchSql = "select trading_symbol, delta, abs(" + requiredDelta + "-abs(delta)) as deltaDiff, quote_time from nexcorio_option_greeks where trading_symbol like '" + optionnamePrefix + "%PE' "
							+ " and quote_time <= '"+ postgresLongDateFormat.format(getCurrentTime()) + "'"	
							+ " and quote_time >  '"+ postgresLongDateFormat.format(getCurrentTimeDifferSeconds(-2)) + "'"
							+ " order by id desc";
					fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
					
					ResultSet rs = stmt.executeQuery(fetchSql);
					List<String> tradingSymbols = new ArrayList<String>();
					List<Float> delta = new ArrayList<Float>();
					List<Float> deltaDiff = new ArrayList<Float>();
					while (rs.next()) {
						tradingSymbols.add(rs.getString("trading_symbol"));
						delta.add(rs.getFloat("delta"));
						deltaDiff.add(rs.getFloat("deltaDiff"));
					}
					rs.close();
					
					// Remove the duplicates
					for(int bottomPt = tradingSymbols.size()-1; bottomPt>0;bottomPt--) {
						for(int topPt = 0; topPt < bottomPt;topPt++) {
							if ( tradingSymbols.get(bottomPt).equals(tradingSymbols.get(topPt)) ) {
								tradingSymbols.remove(bottomPt);
								delta.remove(bottomPt);
								deltaDiff.remove(bottomPt);
								break;
							}
						}
					}				
					
					// Find the best row with minimal delta dif
					float minimalDiff = deltaDiff.get(0);
					int bestPosition = 0;
					for(int i=1;i<deltaDiff.size();i++) {
						if (deltaDiff.get(i) < minimalDiff) {
							minimalDiff = deltaDiff.get(i);
							bestPosition = i;
						}
					}				
					peTradingSymbol = tradingSymbols.get(bestPosition);
					peDelta = delta.get(bestPosition);
				
				}
				
				stmt.close();
				
				String localCeStraddleOptionName =  ceTradingSymbol;
				String localPeStraddleOptionName =  peTradingSymbol;
				
				String localCeHedgeOptionName =  "";
				String localPeHedgeOptionName =  "";
				if (hedgeDistance>0) {
					int centerStrike = getOptionCenterStrike(optionnamePrefix);
					localCeHedgeOptionName =  optionnamePrefix + (centerStrike+hedgeDistance) + "CE";
					localPeHedgeOptionName =  optionnamePrefix + (centerStrike-hedgeDistance) + "PE";
				} 	
				
				retStr = new String[]{localCeStraddleOptionName, localPeStraddleOptionName, localCeHedgeOptionName, localPeHedgeOptionName};
				fileLogTelegramWriter.write(" for requiredDelta "+requiredDelta +" CE " +ceTradingSymbol +" ceDelta="+ceDelta+", " + peTradingSymbol +" peDelta="+peDelta);
			} catch(Exception ex) {
				ex.printStackTrace();
				log.error("Error"+ex.getMessage(),ex);
			}finally {
				try {
					if (conn!=null) conn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			return retStr;
		 }
	}
	
	public int getOptionCenterStrike(String optionnamePrefix) {
		int basePrice = 0;
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			int scripSpotPrice  = (int) getPriceFromTicks(this.mainInstrument.getShortName());
			
			// make last decimal zero
			scripSpotPrice = scripSpotPrice - (scripSpotPrice%10);
			
			basePrice = scripSpotPrice;
			
			for(int i=0;i<10;i++) {
				String checkCEUpStr   = optionnamePrefix + (scripSpotPrice + i*10) + "CE";
				String checkCEDownStr = optionnamePrefix + (scripSpotPrice - i*10) + "CE";
				
				String fetchSql = "select trading_symbol, zerodha_instrument_token from nexcorio_fno_instruments"
						+ " where trading_symbol in ('" + checkCEUpStr+ "','"+checkCEDownStr+"')";
				fileLogTelegramWriter.write("In getOptionCenterStrike fetchSql="+fetchSql);
				
				ResultSet rs = stmt.executeQuery(fetchSql);
				String foundInDB = null;
				while(rs.next()) {
					foundInDB = rs.getString("trading_symbol");
					break;
				}
				rs.close();
				if (foundInDB!=null) {
					if (foundInDB.equals(checkCEUpStr)) basePrice = scripSpotPrice + i*10;
					else basePrice = scripSpotPrice - i*10;
					break;
				}
			}
			
			stmt.close();
		} catch(Exception ex) {
			log.error("Error"+ex.getMessage(),ex);
			ex.printStackTrace();
		}finally {
			try {
				if (conn!=null) conn.close();
			} catch (SQLException e) {
				log.error("Error"+e.getMessage(),e);
				e.printStackTrace();
			}
		}
		return basePrice;
	}
	
	public MainInstruments getMainInstrumentDtoById(Long id) {
		MainInstruments mainInstrument = null;
		Connection conn = null;
		Statement stmt = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			stmt = conn.createStatement();
			
			String fetchSql = "SELECT id, name, short_name, instrument_type, exchange,"
					+ " zerodha_instrument_token, expiry_day, gap_between_strikes, order_freezing_quantity,"
					+ " no_of_future_expiry_data, no_of_options_expiry_data, no_of_options_strike_points, straddle_margin, half_Straddle_Margin, lot_size"
					+ " FROM nexcorio_main_instruments WHERE id="+id;
			System.out.println("In getMainInstrumentDtoById fetchSql="+fetchSql);
			
			ResultSet rs = stmt.executeQuery(fetchSql);
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
				mainInstrument.setHalfStraddleMargin(rs.getFloat("half_Straddle_Margin"));
				mainInstrument.setLotSize(rs.getInt("lot_size"));
			}
			rs.close();
			
			stmt.close();
		} catch (Exception ex) {
			ex.printStackTrace();
			log.error("Error"+ex.getMessage(),ex);
		} finally {
			try {
				if (conn!=null) conn.close();
			} catch (SQLException e) {
				log.error(e);
			}
		}
		return mainInstrument;
	}
	
	protected Date getCurrentTime() {
		return backtestDate!=null?backtestDate.getTime():new Date();
	}
	
	protected Date getCurrentTime(int minute) {
		
		Calendar cal = Calendar.getInstance();
		if (backtestDate!=null) cal.setTime(backtestDate.getTime());
		cal.add(Calendar.MINUTE, minute);
		
		return cal.getTime();
	}
	
	protected Date getCurrentTimeDifferSeconds(int seconds) {
		
		Calendar cal = Calendar.getInstance();
		if (backtestDate!=null) cal.setTime(backtestDate.getTime());
		cal.add(Calendar.SECOND, seconds);
		
		return cal.getTime();
	}
	
	public KiteConnect getKiteConnect(Long userId) {
		
		KiteConnect retKiteConnect = null;
		
		Connection conn = null;
		Statement stmt = null;
		
		try {
			conn = HDataSource.getReadOnlyConnection();
			stmt = conn.createStatement();
			
			String fetchSql = "select id, zerodha_user_id, zerodha_api_key, zerodha_api_secret_key, zerodha_service_token, zerodha_access_token, zerodha_public_token FROM nexcorio_users WHERE id='" + userId + "'";
				
			ResultSet rs = stmt.executeQuery(fetchSql);
			while(rs.next()) {
				retKiteConnect = new KiteConnect(rs.getString("zerodha_api_key"));
				retKiteConnect.setUserId(rs.getString("zerodha_user_id"));
				retKiteConnect.setAccessToken(rs.getString("zerodha_access_token"));
				retKiteConnect.setPublicToken(rs.getString("zerodha_public_token"));
			}
			rs.close();
			stmt.close();
		} catch (Exception ex) {
			log.error("Error"+ex.getMessage(),ex);
			ex.printStackTrace();
		} finally {
			try {
				if (conn!=null) conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return retKiteConnect;
	}
	
	public static float getAvailableMargin(KiteConnect kiteconnect, String segment) { // segment like "commodity" or "equity"
		float retVal = 0f;
		try {
			Map<String, Margin> availableMargins = kiteconnect.getMargins();
			Iterator<String> iter = availableMargins.keySet().iterator();
			while(iter.hasNext()) {
				String mapKey = (String) iter.next();
				System.out.println(" Key = " +mapKey);
				Margin aMargin = availableMargins.get(mapKey);
				System.out.println(aMargin.net+"-" + aMargin.available + "-"+aMargin.utilised);
				System.out.println(aMargin.toString());
				if (mapKey.equals(segment)) {
					retVal = Float.parseFloat(aMargin.net);
				}
			}
		} catch (Exception | KiteException e) {			
			e.printStackTrace();
			log.error("Error in checkDailyMarginUsed"+e.getMessage(), e);
		}
		return retVal;
	}
	
	public int getDaysBetween(Date startDate, Date endDate) {
		long diffInMillies = Math.abs(startDate.getTime() - endDate.getTime());
		return (int) (diffInMillies / (1000 * 60 * 60 * 24));
	}
	
	public void printFields(Object aObj) {
		StringBuffer allFieldDetails = new StringBuffer();
		try {
			allFieldDetails.append("\n    Instrument=" + this.mainInstrument.getShortName() );
			
			Field[] fields = this.getClass().getDeclaredFields();
			
			for(int i=0;i<fields.length;i++) {
				Field aField = fields[i];
				
				if (Modifier.isPublic(aField.getModifiers())) {
					allFieldDetails.append("\n    " + aField.getName() + "="+  aField.get(aObj)  );
				}
			}
		} catch (IllegalArgumentException | IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		fileLogTelegramWriter.write("Field values" + allFieldDetails.toString());
	}
	
	protected float getATMStraddlePremium() {
		float avgeAtmPremium = 0f;
		
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			String fetchSql = "select celtp+peltp as atmPremium from nexcorio_option_atm_movement_data where f_main_instrument = " + this.mainInstrument.getId();
			
			if (this.backtestDate!=null) {
				fetchSql = fetchSql + " and record_time <= '" + postgresLongDateFormat.format(getCurrentTime()) + "'";
			}
			fetchSql = fetchSql + " order by record_time desc limit 3";
			
			fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
			ResultSet rs = stmt.executeQuery(fetchSql);
			
			// We only need avg(2 closest among 3), this is because we observed some sharp big spike/fall in atm premium lasting 1 sec or burst found, to eliminate such outlier 
			
			List<Float> allNumbers = new ArrayList<>();
			while (rs.next()) {
				float currentAtmPremium = rs.getFloat("atmPremium");
				allNumbers.add(currentAtmPremium);
				avgeAtmPremium = avgeAtmPremium + currentAtmPremium;
			}
			rs.close();			
			stmt.close();
			
			avgeAtmPremium = avgeAtmPremium/3f;
			
			List<Float> leftSideNumbers = new ArrayList<>();
			List<Float> rightSideNumbers = new ArrayList<>();
			
			for(int i=0;i<allNumbers.size();i++) {
				if (allNumbers.get(i) > avgeAtmPremium) leftSideNumbers.add(allNumbers.get(i));
				else rightSideNumbers.add(allNumbers.get(i));
			}
			
			if (leftSideNumbers.size()>1) {
				avgeAtmPremium = (leftSideNumbers.get(0) + leftSideNumbers.get(1))/2f;
			} else {
				avgeAtmPremium = (rightSideNumbers.get(0) + rightSideNumbers.get(1))/2f;
			}
			fileLogTelegramWriter.write("In getATMStraddlePremium returning="+avgeAtmPremium);
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
			
		return avgeAtmPremium;
	}
	
	protected Map<String, Float> getATMStraddleData() {
		
		Map<String, Float> retMap = new HashMap<>();
		
		float avgeAtmPremium = 0f;
		
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			String fetchSql = "select ceiv, peiv, cegamma, pegamma, celtp+peltp as atmPremium from nexcorio_option_atm_movement_data where f_main_instrument = " + this.mainInstrument.getId();
			
			if (this.backtestDate!=null) {
				fetchSql = fetchSql + " and record_time <= '" + postgresLongDateFormat.format(getCurrentTime()) + "'";
			}
			fetchSql = fetchSql + " order by record_time desc limit 1";
			
			fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
			ResultSet rs = stmt.executeQuery(fetchSql);
			
			while (rs.next()) {
				retMap.put("ceiv", rs.getFloat("ceiv"));
				retMap.put("peiv", rs.getFloat("peiv"));
				retMap.put("cegamma", rs.getFloat("cegamma"));
				retMap.put("pegamma", rs.getFloat("pegamma"));
				retMap.put("atmPremium", rs.getFloat("atmPremium"));
			}
			rs.close();			
			stmt.close();
			
			fileLogTelegramWriter.write("In getATMStraddleData returning ceiv="+retMap.get("ceiv")+ " peiv=" + retMap.get("peiv")+" cegamma="+retMap.get("cegamma")+" pegamma="+retMap.get("pegamma")+" atmPremium="+retMap.get("atmPremium"));
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
	
	protected Map<String, Float> getCurrentATMIV() {
		Map<String, Float> ivMap = new HashMap<>();
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			String fetchSql = "select ceiv, peiv from nexcorio_option_atm_movement_data where f_main_instrument = " + this.mainInstrument.getId();
			
			if (this.backtestDate!=null) {
				fetchSql = fetchSql + " and record_time <= '" + postgresLongDateFormat.format(getCurrentTime()) + "'";
			}
			fetchSql = fetchSql + " order by record_time desc limit 1";
			
			fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
			ResultSet rs = stmt.executeQuery(fetchSql);
			
			while (rs.next()) {
				ivMap.put("CE", rs.getFloat("ceiv"));
				ivMap.put("PE", rs.getFloat("peiv"));
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
		return ivMap;
	}
	
	protected int getOptimalHedgeDistance(int defaultHedgeDistance, float dailyMaxHedgeCostPerLeg) {
		int returnHedgeDistance = defaultHedgeDistance;
		
		try {
			String optionnamePrefix = getCurrentWeekExpiryOptionnamePrefix();
			
			String exchangeToUse = "NFO";
			if (this.mainInstrument.getExchange().equals("BSE")) exchangeToUse = "BFO";
			
			int centerStrike = getOptionCenterStrike(optionnamePrefix);
			
			fileLogTelegramWriter.write("In getOptimalHedgeDistance optionnamePrefix="+optionnamePrefix+" centerStrike="+centerStrike);
			
			List<String> OpInstruments = new ArrayList<String>(); 
			for (int i=0;i<=500;i+=100) {
				
				String ceOptionName =  optionnamePrefix + (centerStrike+defaultHedgeDistance + i) + "CE";
				String peOptionName =  optionnamePrefix + (centerStrike-defaultHedgeDistance - i) + "PE";
				
				OpInstruments.add(exchangeToUse+":"+ ceOptionName);
				OpInstruments.add(exchangeToUse+":"+ peOptionName);
			}
			
			Map<String, LTPQuote> optionLtp;
			
			String[] OpStrings = OpInstruments.stream().toArray(String[]::new);

			
			optionLtp = getKiteConnect(this.userId).getLTP(OpStrings);
			
			Iterator<String> optionKeys = optionLtp.keySet().iterator();
	    	while(optionKeys.hasNext()) {
	    		String aOptionKey = optionKeys.next();
	    		float currentOptionPrice = (float) (optionLtp.get(aOptionKey).lastPrice);
	    		log.debug("Key="+ aOptionKey+" " + currentOptionPrice);
	    	}
	    	
	    	float hedgeTargetPrice = dailyMaxHedgeCostPerLeg*getWorkingDaysTillNextExpiry(new Date(),  getOptionCurrentWeekExpiryDate());
	    	fileLogTelegramWriter.write("hedgeTargetPrice="+hedgeTargetPrice);
	    	
			for (int i=0;i<=500;i+=100) {
				
				String ceOptionName =  optionnamePrefix + (centerStrike+defaultHedgeDistance + i) + "CE";
				String peOptionName =  optionnamePrefix + (centerStrike-defaultHedgeDistance - i) + "PE";
				
				float ceOptionPrice = (float) (optionLtp.get(exchangeToUse+":"+ceOptionName).lastPrice);
				float peOptionPrice = (float) (optionLtp.get(exchangeToUse+":"+peOptionName).lastPrice);
				fileLogTelegramWriter.write("ceOptionPrice="+ceOptionPrice+" peOptionPrice="+peOptionPrice);
				
				returnHedgeDistance = defaultHedgeDistance + i;
				
				if ( (ceOptionPrice + peOptionPrice) <= 2f*hedgeTargetPrice ) {
				//if ( ceOptionPrice < hedgeTargetPrice ||  peOptionPrice < hedgeTargetPrice ) {
					fileLogTelegramWriter.write("Stopping here");
					break;
				}
	    	}
			fileLogTelegramWriter.write("optionnamePrefix="+optionnamePrefix+" hedgeTargetPrice="+hedgeTargetPrice+" defaultHedgeDistance="+defaultHedgeDistance+" returnHedgeDistance="+returnHedgeDistance+" dailyMaxHedgeCostPerLeg="+dailyMaxHedgeCostPerLeg);
		} catch (JSONException | IOException | KiteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return returnHedgeDistance;
	}
	
	private int getWorkingDaysTillNextExpiry(Date currentDate, Date nextExpiryDate) {
		int workingDays = 4;
		
		try {
			LocalDate starDate = LocalDate.of(currentDate.getYear()+1900, currentDate.getMonth()+1, currentDate.getDate());
			LocalDate endDate = LocalDate.of(nextExpiryDate.getYear()+1900, nextExpiryDate.getMonth()+1, nextExpiryDate.getDate());
			
			
			final DayOfWeek startW = starDate.getDayOfWeek();
		    final DayOfWeek endW = endDate.getDayOfWeek();
	
		    final long days = ChronoUnit.DAYS.between(starDate, endDate)+1;
		    
		    final long daysWithoutWeekends = days - 2 * ((days + startW.getValue()) / 7);
		    
			fileLogTelegramWriter.write("days="+days+"daysWithoutWeekends="+daysWithoutWeekends);
			
		    
		    //adjust for starting and ending on a Sunday:
		    workingDays =  (int) (daysWithoutWeekends + (startW == DayOfWeek.SUNDAY ? 1 : 0) + (endW == DayOfWeek.SUNDAY ? 1 : 0));
		    
		    fileLogTelegramWriter.write("workingDays="+workingDays);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return workingDays;
	}
	
	protected String getNextNFUTUREExpiryDatePrefix(Long mainInstrumentId, String exchange) {
		fileLogTelegramWriter.write("In getNextNFUTUREExpiryDatePrefix exchange="+exchange);
		String retStr = "";
		
		Connection conn = null;
		Statement stmt = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			stmt = conn.createStatement();
			
			String fnoExchange = "NFO-FUT";
			if (exchange.equalsIgnoreCase("BSE")) fnoExchange = "BFO-FUT";
			
			Calendar cal = Calendar.getInstance();
			if (backtestDate!=null) cal.setTime(backtestDate.getTime());
			cal.add(Calendar.DATE, -1);
			
			String fetchSql = "SELECT fno_prefix from nexcorio_fno_expiry_dates WHERE f_main_instrument="+mainInstrumentId+ ""
					+ " and fno_segment='" + fnoExchange + "' "
					+ " and expiry_date > '" + postgresShortDateFormat.format(cal.getTime()) + "' "
					+ " ORDER BY expiry_date ASC LIMIT 1";
			
			ResultSet rs = stmt.executeQuery(fetchSql);
			
			while(rs.next()) {
				retStr = rs.getString("fno_prefix") + "FUT";
			}
			rs.close();			
			stmt.close();
			fileLogTelegramWriter.write("retStr="+retStr);
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (conn!=null) conn.close();
			} catch (SQLException e) {
				log.error(e);
			}
		}
		return retStr;
	}
	
	protected int getStrikePriceFromOptionName(String optionName) {
		int retVal = 0;
		retVal = Integer.parseInt(optionName.substring(optionName.length()-7,optionName.length()-2));
		return retVal;
	}
	
	protected float getTimeValue(String optionName, float indexLtp, float optionLtp) {
		float timeValue = 0f;
		int optionStrikePrice = getStrikePriceFromOptionName(optionName);
		if (optionName.endsWith("CE")) {
			if (indexLtp > optionStrikePrice ) {
				timeValue = optionLtp - (indexLtp - optionStrikePrice) ;
			} else {
				timeValue = optionLtp;
			}
		} else { // PE
			if (indexLtp < optionStrikePrice ) {
				timeValue = optionLtp - (optionStrikePrice - indexLtp) ;
			} else {
				timeValue = optionLtp ;
			}
		}
		return timeValue;
	}
	
}
