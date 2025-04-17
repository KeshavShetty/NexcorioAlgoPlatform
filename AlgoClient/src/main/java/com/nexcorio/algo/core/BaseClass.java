package com.nexcorio.algo.core;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.dto.MainInstruments;
import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.util.FileLogTelegramWriter;
import com.nexcorio.algo.util.db.HDataSource;
import com.zerodhatech.kiteconnect.KiteConnect;
import com.zerodhatech.kiteconnect.kitehttp.exceptions.KiteException;
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
		
		Connection conn = null;
		try {
			SimpleDateFormat postgresLongDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			
			conn = HDataSource.getConnection();
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
		
		OptionGreek retVal = null;
		Connection conn = null;
		try {
			conn = HDataSource.getConnection();
			Statement stmt = conn.createStatement();
			
			String fetchSql = "select iv, delta, vega, theta, gamma, ltp from nexcorio_option_greeks  where trading_symbol = '" + optionName + "'"
					+ ( backtestDate!=null ? ( " and quote_time <='" + postgresLongDateFormat.format(backtestDate.getTime())+ "'") : "" )
					+ " order by quote_time desc limit 1";
			fileLogTelegramWriter.write("In getOptionGreeks fetchSql="+fetchSql);
			ResultSet rs = stmt.executeQuery(fetchSql);
			while (rs.next()) {
				retVal = new OptionGreek(optionName, rs.getFloat("iv"), rs.getFloat("delta"), rs.getFloat("vega"), rs.getFloat("theta"), rs.getFloat("gamma"), rs.getFloat("ltp"));
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
			conn = HDataSource.getConnection();
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
	
	protected Date getOptionCurrentWeekExpiryDate() {
		Date expiryDate = null;
		Connection conn = null;
		try {
			conn = HDataSource.getConnection();
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
			conn = HDataSource.getConnection();
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
				stmt.close();
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

	protected String[] getStraddleOptionNamesByDeltaOptimised(float requiredDelta, int hedgeDistance) {
		String[] retStr = null;
		Connection conn = null;
		try {
			conn = HDataSource.getConnection();
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
						+ " and quote_time >  '"+ postgresLongDateFormat.format(getCurrentTime(-1)) + "'"
						+ " order by deltaDiff";
				fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
				
				ResultSet rs = stmt.executeQuery(fetchSql);
				List<String> tradingSymbols = new ArrayList<String>();
				List<Float> delta = new ArrayList<Float>();
				List<Float> deltaDiff = new ArrayList<Float>();
				List<Date> quote_times = new ArrayList<Date>();
				while (rs.next()) {
					tradingSymbols.add(rs.getString("trading_symbol"));
					delta.add(rs.getFloat("delta"));
					deltaDiff.add(rs.getFloat("deltaDiff"));
					quote_times.add(rs.getDate("quote_time"));
				}
				rs.close();
				if (tradingSymbols.size()==1) {
					ceTradingSymbol = tradingSymbols.get(0);
					ceDelta = delta.get(0);
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
							ceDelta = delta.get(i);
							break;
						}
					}
					if (ceTradingSymbol==null) { // Not found, then use first one
						ceTradingSymbol = tradingSymbols.get(0);
						ceDelta = delta.get(0);
					}
				}
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
				stmt.close();
			} else {
				String fetchSql = "select trading_symbol, delta, abs(" + requiredDelta + "-abs(delta)) as deltaDiff, quote_time from nexcorio_option_greeks where trading_symbol like '" + optionnamePrefix + "%PE' "
						+ " and quote_time <= '"+ postgresLongDateFormat.format(getCurrentTime()) + "'"	
						+ " and quote_time >  '"+ postgresLongDateFormat.format(getCurrentTime(-1)) + "'"
						+ " order by deltaDiff";
				fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
				
				ResultSet rs = stmt.executeQuery(fetchSql);
				List<String> tradingSymbols = new ArrayList<String>();
				List<Float> delta = new ArrayList<Float>();
				List<Float> deltaDiff = new ArrayList<Float>();
				List<Date> quote_times = new ArrayList<Date>();
				while (rs.next()) {
					tradingSymbols.add(rs.getString("trading_symbol"));
					delta.add(rs.getFloat("delta"));
					deltaDiff.add(rs.getFloat("deltaDiff"));
					quote_times.add(rs.getDate("quote_time"));
				}
				rs.close();
				if (tradingSymbols.size()==1) {
					peTradingSymbol = tradingSymbols.get(0);
					peDelta = delta.get(0);
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
							peDelta = delta.get(i);
							break;
						}
					}
					if (peTradingSymbol==null) { // Not found, then use first one
						peTradingSymbol = tradingSymbols.get(0);
						peDelta = delta.get(0);
					}
				}
			
			}
			
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
	
	public int getOptionCenterStrike(String optionnamePrefix) {
		int basePrice = 0;
		Connection conn = null;
		try {
			conn = HDataSource.getConnection();
			Statement stmt = conn.createStatement();
			
			int scripSpotPrice  = (int) this.instrumentLtp;
			
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
			conn = HDataSource.getConnection();
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
	
	public KiteConnect getKiteConnect(Long userId) {
		
		KiteConnect retKiteConnect = null;
		
		Connection conn = null;
		Statement stmt = null;
		
		try {
			conn = HDataSource.getConnection();
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
			log.info("Error in checkDailyMarginUsed"+e.getMessage(), e);
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
}
