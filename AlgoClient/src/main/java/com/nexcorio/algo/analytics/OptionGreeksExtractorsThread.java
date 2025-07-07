package com.nexcorio.algo.analytics;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.dto.MainInstruments;
import com.nexcorio.algo.dto.OptionFnOInstrument;
import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.kite.KiteCache;
import com.nexcorio.algo.kite.KiteHelper;
import com.nexcorio.algo.util.BSOption;
import com.nexcorio.algo.util.db.HDataSource;

/**
 * 
 * @author Keshav Shetty
 *
 */
public class OptionGreeksExtractorsThread implements Runnable {

	private static final Logger log = LogManager.getLogger(OptionGreeksExtractorsThread.class);
	
	DateTimeFormatter postgresLongDateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
	
	private static float INTEREST_RATE = 0.1f;
	
	Long fStreamingId;
	String tradingSymbol;
	float ltp;
	float openIterest;
	Date tickTimestamp;
	
	public OptionGreeksExtractorsThread(Long fStreamingId, String tradingSymbol, float ltp, float openIterest, Date tickTimestamp) {
		super();
		this.fStreamingId = fStreamingId;
		this.tradingSymbol = tradingSymbol;
		this.ltp = ltp;
		this.openIterest = openIterest;
		this.tickTimestamp = tickTimestamp;
		
		log.debug("OptionGreeksExtractorsThread fStreamingId="+fStreamingId+" tradingSymbol="+tradingSymbol+" ltp="+ltp+" openIterest="+openIterest+" tickTimestamp="+tickTimestamp);
		
		Thread t = new Thread(this, "FnoAnalyticsExtractors"+fStreamingId);
		t.setPriority(Thread.MAX_PRIORITY);
		t.start();
	}
	
	@Override
	public void run() {
		Long beginTime = System.currentTimeMillis();
		StringBuffer logStr = new StringBuffer();
		Long startTime = System.currentTimeMillis();
		
		OptionFnOInstrument optionInstrument= getOptionInstrument(tradingSymbol); 
		String optionType = tradingSymbol.endsWith("CE")?"CE":"PE";
		
		int strikePrice = optionInstrument.getStrike().intValue();
		float underlyingValue = getPriceFromTicks(optionInstrument.getfMainInstrument());
		
		Long elapsedTime1 = System.currentTimeMillis();
		logStr.append(", Time taken for getPriceFromTicks=" +(elapsedTime1-startTime));
		startTime = elapsedTime1;
		
		float optionIV = guessTheIV(this.ltp, underlyingValue, strikePrice, optionType, optionInstrument.getExpiryDate()); 
		elapsedTime1 = System.currentTimeMillis();
		logStr.append(", Time taken for guessTheIV=" +(elapsedTime1-startTime));
		startTime = elapsedTime1;
		
		if (optionIV!=0) {
			OptionGreek optionGreekDto = calculateAndSaveOptionGreeks(optionType, tradingSymbol, this.ltp, underlyingValue, strikePrice, optionIV, optionInstrument.getExpiryDate(), tickTimestamp);
			KiteCache.optionGreekCache.put(tradingSymbol, optionGreekDto);
		}
		elapsedTime1 = System.currentTimeMillis();
		logStr.append(", Time taken for calculateAndSaveOptionGreeks=" +(elapsedTime1-startTime));
		startTime = elapsedTime1;
		
		Long endTime = System.currentTimeMillis();
		Long timeTaken = endTime-beginTime;
		if (timeTaken>500) {
			log.error("Delay in calculateAndSaveOptionGreeks for " + tradingSymbol+" timeTaken="+timeTaken+ logStr.toString());
		}
		//System.out.println(logStr.toString());
	}
	
	public float guessTheIV(double optionPrice, double underlyingValue, double strikePrice, String optionType, Date expDate) {
		float retVal = 0f;
		try {
			log.debug("fStreamingId="+this.fStreamingId+" for  " + this.tradingSymbol + " guessTheIV optionPrice="+optionPrice+" underlyingValue="+underlyingValue+" strikePrice="+strikePrice+" optionType="+optionType+" expDate="+expDate);
			
			Calendar cal = Calendar.getInstance();
			cal.setTime(expDate);
			cal.set(Calendar.HOUR_OF_DAY, 15);
			cal.set(Calendar.MINUTE, 30);
			//System.out.println("for "+expDate+" caltime=" + cal.getTime());
			
			long diffInMillies = Math.abs(cal.getTimeInMillis() - (new Date()).getTime());
			
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
		
		long diffInMillies = Math.abs(cal.getTimeInMillis() - (new Date()).getTime());
		
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
		retVal.setLtp((float) lastPrice);
		retVal.setOi(this.openIterest);
		
		Connection conn = null;
		try {
			conn = HDataSource.getConnection();
			Statement stmt = conn.createStatement();
						
			SimpleDateFormat postgresLongDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			SimpleDateFormat postgresShortDateFormat = new SimpleDateFormat("yyyy-MM-dd");
			
			String insertSql = "INSERT INTO nexcorio_option_greeks (id, trading_symbol, quote_time, ltp, oi, underlying_value, iv, delta, vega, theta, gamma)"
					+ " VALUES (" + this.fStreamingId + ",'" + this.tradingSymbol+ "','" + postgresLongDateFormat.format(latestTickQuoteTime) + "'," + lastPrice + "," + this.openIterest  +"," + underlyingValue 
					+"," + (float)impliedVolatility +"," + (float)delta+"," + (float)vega+"," + (float)theta+"," + (float)gamma + ")";
			log.debug(insertSql);
			stmt.execute(insertSql);
			
			Long snapshotId = null;
			// get from cache first
			snapshotId = KiteCache.snapshotIdCache.getIfPresent(this.tradingSymbol);
			if (snapshotId==null) {
				// Insert into snapshot, first check if exists			
				String fetchSql = "select id from nexcorio_option_snapshot where trading_symbol='" + this.tradingSymbol + "' and record_date='" + postgresShortDateFormat.format(latestTickQuoteTime) + "'";
				log.debug(fetchSql);
				
				ResultSet rs = stmt.executeQuery(fetchSql);
				
				
				while(rs.next()) {
					snapshotId = rs.getLong("id");
				}
				rs.close();
				if(snapshotId!=null) KiteCache.snapshotIdCache.put(this.tradingSymbol, snapshotId);
			} 
			
			if (snapshotId!=null) { // Already exist
				String updateSql = "UPDATE nexcorio_option_snapshot set last_updated_time='" + postgresLongDateFormat.format(latestTickQuoteTime) + "', ltp=" + lastPrice + ", oi=" + this.openIterest  
						+", iv=" + (float)impliedVolatility +", delta=" + (float)delta+ ", vega=" + (float)vega+ ", theta=" + (float)theta+ ", gamma=" + (float)gamma + " where id=" + snapshotId;
				log.debug(updateSql);
				stmt.execute(updateSql);
				
			} else { // insert
				insertSql = "INSERT INTO nexcorio_option_snapshot (id, trading_symbol, strike, last_updated_time, record_date, ltp, oi, iv, delta, vega, theta, gamma)"
						+ " VALUES (nextval('nexcorio_option_snapshot_id_seq'),'" + this.tradingSymbol+ "'," + strikePrice 
						+ ",'" + postgresLongDateFormat.format(latestTickQuoteTime) + "','" + postgresShortDateFormat.format(latestTickQuoteTime) + "'," + lastPrice + "," + this.openIterest     
						+"," + (float)impliedVolatility +"," + (float)delta+"," + (float)vega+"," + (float)theta+"," + (float)gamma + ")";
				log.debug(insertSql);
				stmt.execute(insertSql);
			}
			
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
	
	public OptionFnOInstrument getOptionInstrument(String tradingSymbol) {
		OptionFnOInstrument optionFnOInstrument= null;
		
		if (KiteCache.getTradingSymbolToOptionInstrument(tradingSymbol)!=null) {
			return KiteCache.getTradingSymbolToOptionInstrument(tradingSymbol);
		} else {
			
			Connection conn = null;
			Statement stmt = null;
			try {
				conn = HDataSource.getReadOnlyConnection();
				stmt = conn.createStatement();
				
				String fetchSql = "SELECT id, trading_symbol, zerodha_instrument_token, f_main_instrument, exchange, strike, expiry_date from nexcorio_fno_instruments WHERE trading_symbol='"+tradingSymbol+"' ";
				ResultSet rs = stmt.executeQuery(fetchSql);
				while(rs.next()) {
					optionFnOInstrument = new OptionFnOInstrument(rs.getLong("id"), rs.getString("trading_symbol"), rs.getLong("f_main_instrument"), rs.getLong("zerodha_instrument_token"), rs.getString("exchange"), rs.getInt("strike"), rs.getDate("expiry_date"));
				} 
				rs.close();
				stmt.close();
				
				if (optionFnOInstrument!=null) {
					KiteCache.putTradingSymbolToOptionInstrument(tradingSymbol, optionFnOInstrument);
				}
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
			return optionFnOInstrument;
		}
	}
	
	public float getPriceFromTicks(Long mainInstrumentId) {
		float retVal = 0f;
		
		MainInstruments mainInstrument = KiteCache.mainInstrumentsByIdCache.getIfPresent(mainInstrumentId);
		
		if (mainInstrument==null) {
			mainInstrument = getMainInstrumentDtoById(mainInstrumentId);
			KiteCache.mainInstrumentsByIdCache.put(mainInstrumentId, mainInstrument);
		}
		Float priceFromCache = KiteCache.tickPriceCache.getIfPresent(mainInstrument.getShortName());
		if ( priceFromCache != null ) {	
			//System.out.println("Greek Extracor, price found in cache"+priceFromCache);
			return priceFromCache;
		} else {
			Connection conn = null;
			Statement stmt = null;
			try {
				conn = HDataSource.getReadOnlyConnection();
				stmt = conn.createStatement();
				
				String fetchSql = "SELECT last_traded_price from nexcorio_tick_data WHERE trading_symbol=(select short_name from nexcorio_main_instruments where id="+mainInstrumentId+") ORDER BY quote_time DESC LIMIT 1";
				ResultSet rs = stmt.executeQuery(fetchSql);
				while(rs.next()) {
					retVal = rs.getFloat("last_traded_price");
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
			return retVal;
		}
	}
	
	public MainInstruments getMainInstrumentDtoById(Long id) {
		MainInstruments mainInstrument = null;
		Connection conn = null;
		Statement stmt = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			stmt = conn.createStatement();
			
			ResultSet rs = stmt.executeQuery("SELECT id, name, short_name, instrument_type, exchange,"
					+ " zerodha_instrument_token, expiry_day, gap_between_strikes, order_freezing_quantity,"
					+ " no_of_future_expiry_data, no_of_options_expiry_data, no_of_options_strike_points, straddle_margin"
					+ " FROM nexcorio_main_instruments WHERE id="+id);
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
		return mainInstrument;
	}
}
