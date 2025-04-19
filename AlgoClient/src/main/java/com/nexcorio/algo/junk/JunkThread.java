package com.nexcorio.algo.junk;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.util.BSOption;

/**
 * 
 * @author Keshav Shetty
 *
 */
public class JunkThread implements Runnable {
	
	private static final Logger log = LogManager.getLogger(JunkThread.class);
	
	protected SimpleDateFormat postgresLongDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	protected SimpleDateFormat postgresShortDateFormat = new SimpleDateFormat("yyyy-MM-dd");
	
	String expiryDate;
	String fnoPrefix;
	Long instrument_token;
	String trading_symbol;
	Timestamp record_time;
	Float ltp;
	Float openinterest;
	Long fStreamingId = null;
	
	private static float INTEREST_RATE = 0.1f;
	
	public JunkThread(String expiryDate, String fnoPrefix, Long instrument_token, String trading_symbol, Timestamp record_time, Float ltp, Float openinterest) {
		
		this.expiryDate = expiryDate;
		this.fnoPrefix = fnoPrefix;
		this.instrument_token = instrument_token;
		this.trading_symbol = trading_symbol;
		this.record_time = record_time;
		this.ltp = ltp;
		this.openinterest = openinterest;
		
		Thread t = new Thread(this, trading_symbol+record_time);
		t.setPriority(Thread.MAX_PRIORITY);
		t.start();	
	}
	

	@Override
	public void run() {
		try {
			//System.out.println(instrument_token+"-" + trading_symbol+"-"+record_time+"-"+ltp+"-"+openinterest);
			if (this.instrument_token != 256265)  {
				saveOrUpdateFnOInstrument(expiryDate, fnoPrefix);
			}
			
			saveTick();
			
			if (this.instrument_token != 256265)  { // This is Option, calculate greeks
				float indexPrice = getUnderlyingPriceFromTicks(record_time);
				String optionType = trading_symbol.endsWith("CE")?"CE":"PE";
				
				Date expDate = postgresShortDateFormat.parse(this.expiryDate);
				
				float iv = guessTheIV(ltp, indexPrice, getStrike(trading_symbol), optionType, expDate);
				calculateAndSaveOptionGreeks(optionType, trading_symbol, ltp, indexPrice, getStrike(trading_symbol), iv, expDate, this.record_time);
			}
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void saveTick() {
		Connection terraceConn = null; 
		
		try {
			terraceConn = MultiDataSource.getTerraceConnection();
			Statement stmt = terraceConn.createStatement();
			
			String fetchNextSeq = "select nextval('nexcorio_tick_data_id_seq') as nextId";
	    	
	    	this.fStreamingId = null;
	    	ResultSet rs = stmt.executeQuery(fetchNextSeq);
			while (rs.next()) {
				fStreamingId = rs.getLong("nextId");
			}
			rs.close();
			
			String trading_symbolToUse = trading_symbol;
			if (trading_symbolToUse.equals("NIFTY 50")) {
				trading_symbolToUse = "NIFTY";
			}
			
			String insertSql = "INSERT INTO nexcorio_tick_data (id, f_main_instrument, trading_symbol, record_time, last_traded_price, open_interest, quote_time) "
					+ " VALUES (" + fStreamingId +  ", 2, '" + trading_symbolToUse + "', '" + postgresLongDateFormat.format(record_time) + "', " + ltp + ", " + openinterest + ",'"+ postgresLongDateFormat.format(record_time)+ "')";
			System.out.println(insertSql);
			stmt.execute(insertSql);
			
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

	private void saveOrUpdateFnOInstrument(String expiryDate, String fnoPrefix) {
		Connection terraceConn = null; 
		
		try {
			terraceConn = MultiDataSource.getTerraceConnection();
			Statement stmt = terraceConn.createStatement();
			
			String chksql = "select count(*) from nexcorio_fno_instruments where zerodha_instrument_token = " + instrument_token + "";
			System.out.println(chksql);
			
			int recCount = 0;
			ResultSet rs = stmt.executeQuery(chksql);
			while (rs.next()) {
				recCount = rs.getInt(1);
			}
			rs.close();
			if (recCount==0) {
				chksql = "INSERT INTO nexcorio_fno_instruments (id, trading_symbol, zerodha_instrument_token, f_main_instrument, strike, expiry_date, exchange) VALUES ("
						+ "nextval('nexcorio_fno_instruments_id_seq'), '" + this.trading_symbol + "', " + this.instrument_token + ",2," + getStrike(this.trading_symbol) + ",'" + expiryDate + "','NFO')";
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
	
	public float getUnderlyingPriceFromTicks(Date quoteTime) {
		float retVal = 0f;
		
		Connection conn = null;
		try {
			SimpleDateFormat postgresLongDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			
			conn = MultiDataSource.getTerraceConnection();
			Statement stmt = conn.createStatement();
			
			String fetchSql = "select quote_time, last_traded_price from nexcorio_tick_data where trading_symbol = 'NIFTY'"
					+ " and quote_time <='" + postgresLongDateFormat.format( quoteTime )+ "'"
					+ " order by quote_time desc limit 1";
			
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
	
	private int getStrike(String tradingSymbol) {
		int retVal = 0;
		retVal = Integer.parseInt(tradingSymbol.substring(tradingSymbol.length()-7, tradingSymbol.length()-2));
		//System.out.println(retVal);
		return retVal;
	}
	
	public float guessTheIV(double optionPrice, double underlyingValue, double strikePrice, String optionType, Date expDate) {
		float retVal = 0f;
		try {
			log.info("fStreamingId="+this.fStreamingId+" for  " + this.trading_symbol + " guessTheIV optionPrice="+optionPrice+" underlyingValue="+underlyingValue+" strikePrice="+strikePrice+" optionType="+optionType+" expDate="+expDate);
			
			Calendar cal = Calendar.getInstance();
			cal.setTime(expDate);
			cal.set(Calendar.HOUR_OF_DAY, 15);
			cal.set(Calendar.MINUTE, 30);
			//System.out.println("for "+expDate+" caltime=" + cal.getTime());
			
			long diffInMillies = Math.abs(cal.getTimeInMillis() - this.record_time.getTime()); 
			
			float fractioAsDayinYears = ((float)diffInMillies)/(1000f*60f*60f*24f*365f);
			
			double upperIV = 100d;
			double lowerIV = 1d;
						
			double midPointPrice = 0f;
			double midPoint = 0f;
			int iterCount = 0;
			
			do {
				iterCount++;
				midPoint = (upperIV+lowerIV)/2d; 
				BSOption midPointPriceIV = new BSOption(underlyingValue, strikePrice, fractioAsDayinYears, INTEREST_RATE, 0f, midPoint/100f, 0f, optionType);
				midPointPrice = midPointPriceIV.computePrice();
				//System.out.println(iterCount+" midPoint="+ midPoint + " midPointIVBS="+midPointPrice);
				
				if (optionPrice>midPointPrice) {
					lowerIV = midPoint;
				} else {
					upperIV = midPoint;
				}
				if (iterCount>20) {
					retVal = 0;
					break;
				} else {
					retVal = (float) midPoint;
				}
			} while(Math.abs(midPointPrice-optionPrice)>0.01d);
			
			//System.out.println("Final IV="+midPoint+" Calculate Price="+midPointPrice);
			
		} catch(Exception ex) {
			ex.printStackTrace();
			log.error("Error"+ex.getMessage(), ex);
		}
		return retVal;
	}

	public OptionGreek calculateAndSaveOptionGreeks(String optionType, String optionName, double lastPrice, double underlyingValue, double strikePrice, double impliedVolatility, Date expDate, Date latestTickQuoteTime) {
		//System.out.println("nseIdentifier="+nseIdentifier+" optionName="+optionName+" lastPrice="+lastPrice+" underlyingValue="+underlyingValue+" impliedVolatility="+impliedVolatility);
		OptionGreek retVal = null;
		Calendar cal = Calendar.getInstance();
		cal.setTime(expDate);
		cal.set(Calendar.HOUR_OF_DAY, 15);
		cal.set(Calendar.MINUTE, 30);
		//System.out.println("for "+expDate+" caltime=" + cal.getTime());
		
		long diffInMillies = Math.abs(cal.getTimeInMillis() - (latestTickQuoteTime).getTime());
		
		float fractioAsDayinYears = ((float)diffInMillies)/(1000f*60f*60f*24f*365f);
		
		BSOption aBs = new BSOption(underlyingValue, strikePrice, fractioAsDayinYears, INTEREST_RATE, 0f, impliedVolatility/100f, 0f, optionType);
		
		double calculatedOptionPrice = aBs.computePrice();
		double[] greeks = aBs.computeGreeks();
		//System.out.println(calculatedOptionPrice);
		
		double delta = !Double.isNaN(greeks[0])?greeks[0]:0; 
		double vega  = !Double.isNaN(greeks[1])?greeks[1]:0;
		double psi   = !Double.isNaN(greeks[2])?greeks[2]:0;
		double theta = !Double.isNaN(greeks[3])?greeks[3]:0;
		double rho   = !Double.isNaN(greeks[4])?greeks[4]:0;
		double gamma = !Double.isNaN(greeks[5])?greeks[5]:0;
		double volga = !Double.isNaN(greeks[6])?greeks[6]:0;
		
		retVal = new OptionGreek(optionName, (float)impliedVolatility, (float)delta, (float)vega, (float)theta, (float)gamma );
		retVal.setUnderlyingValue((float)underlyingValue);
		
		Connection conn = null;
		try {
			conn = MultiDataSource.getTerraceConnection();
			Statement stmt = conn.createStatement();
			
			String insertSql = "INSERT INTO nexcorio_option_greeks (id, trading_symbol, quote_time, record_time, ltp, oi, underlying_value, iv, delta, vega, theta, gamma)"
					+ " VALUES (" + this.fStreamingId + ",'" + this.trading_symbol+ "','" + postgresLongDateFormat.format(latestTickQuoteTime) + "'"
					+ ",'" + postgresLongDateFormat.format(this.record_time) + "'"
					+ "," + lastPrice + "," + this.openinterest  +"," + underlyingValue 
					+"," + (float)impliedVolatility +"," + (float)delta+"," + (float)vega+"," + (float)theta+"," + (float)gamma + ")";
			System.out.println(insertSql);
			stmt.execute(insertSql);
			
			stmt.close();
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error"+e.getMessage(), e);
		} finally {
			try {
				if (conn!=null) conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return retVal;
	}
	
	public static void main(String[] args) {
		//getStrike("NIFTY2540922200PE");
	}
}
