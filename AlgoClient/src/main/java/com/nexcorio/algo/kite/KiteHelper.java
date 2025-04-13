package com.nexcorio.algo.kite;

import java.awt.Desktop;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONException;

import com.neovisionaries.ws.client.WebSocketException;
import com.nexcorio.algo.dto.MainInstruments;
import com.nexcorio.algo.dto.OptionFnOInstrument;
import com.nexcorio.algo.util.ApplicationConfig;
import com.nexcorio.algo.util.KiteUtil;
import com.nexcorio.algo.util.db.HDataSource;
import com.zerodhatech.kiteconnect.KiteConnect;
import com.zerodhatech.kiteconnect.kitehttp.SessionExpiryHook;
import com.zerodhatech.kiteconnect.kitehttp.exceptions.KiteException;
import com.zerodhatech.models.LTPQuote;
import com.zerodhatech.models.Tick;
import com.zerodhatech.models.User;
import com.zerodhatech.ticker.KiteTicker;
import com.zerodhatech.ticker.OnConnect;
import com.zerodhatech.ticker.OnDisconnect;
import com.zerodhatech.ticker.OnTicks;

/**
 * 
 * @author Keshav Shetty
 *
 */
public class KiteHelper {
	
	private static final Logger log = LogManager.getLogger(KiteHelper.class);
	
	KiteConnect kiteConnect = null;
	
	protected SimpleDateFormat postgresLongDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	protected SimpleDateFormat postgresShortDateFormat = new SimpleDateFormat("yyyy-MM-dd");
	
	public KiteConnect login() {
		kiteConnect = null;	
		try {
			String USER_ID = ApplicationConfig.getProperty("zerodha.user.id");
			
			ZerodhaAccountKeys zerodhaAccountKeys = getZerodhaAccountKeys(USER_ID);
			kiteConnect = new KiteConnect(zerodhaAccountKeys.getApiKey());				
			kiteConnect.setUserId(USER_ID);
            String kiteLoginURL = kiteConnect.getLoginURL();
            
            String requestToken = getRequestToken(USER_ID, kiteLoginURL);
            User userModel =  kiteConnect.generateSession(requestToken, zerodhaAccountKeys.getApiSecretKey());

            kiteConnect.setAccessToken(userModel.accessToken);
            kiteConnect.setPublicToken(userModel.publicToken);
            
            log.info("kiteconnect kiteLoginURL={"+kiteLoginURL+"}");
            log.info("kiteconnect AccessToken={"+kiteConnect.getAccessToken()+"}");
            log.info("kiteconnect ApiKey={"+kiteConnect.getApiKey()+"}");
            log.info("kiteconnect PublicToken={"+kiteConnect.getPublicToken()+"}");
            log.info("kiteconnect UserId={"+kiteConnect.getUserId()+"}");
            
            saveKiteAccessCodes(USER_ID, requestToken, kiteConnect.getAccessToken(), kiteConnect.getPublicToken()); // For future when decide multi user system
            // Set session expiry callback.
            kiteConnect.setSessionExpiryHook((new SessionExpiryHook() {
				@Override
				public void sessionExpired() {}
			}));
		} catch(Exception ex) {
			ex.printStackTrace();
		} catch (KiteException e) {
			e.printStackTrace();
		}
		return kiteConnect;
	}
	
	private ZerodhaAccountKeys getZerodhaAccountKeys(String zerodhaUserId) {
		ZerodhaAccountKeys retZerodhaKey = null;
		
		Connection conn = null;
		Statement stmt = null;
		try {
			conn = HDataSource.getConnection();
			stmt = conn.createStatement();
			
			String fetchSql = "select id, zerodha_api_key, zerodha_api_secret_key, zerodha_service_token, zerodha_access_token, zerodha_public_token FROM nexcorio_users WHERE zerodha_user_id='" + zerodhaUserId + "'";
				
			ResultSet rs = stmt.executeQuery(fetchSql);
			while(rs.next()) {
				retZerodhaKey = new ZerodhaAccountKeys();
				retZerodhaKey.setId(rs.getLong("id"));
				retZerodhaKey.setApiKey(rs.getString("zerodha_api_key"));
				retZerodhaKey.setApiSecretKey(rs.getString("zerodha_api_secret_key"));
				retZerodhaKey.setServiceToken(rs.getString("zerodha_service_token"));
				retZerodhaKey.setAccessToken(rs.getString("zerodha_access_token"));
				retZerodhaKey.setPublicToken(rs.getString("zerodha_public_token"));
				break;
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
		return retZerodhaKey;
	}
	
	private String getRequestToken(String zerodhaUserId, String kiteLoginUrl) {
		String retVal = null;
		Connection conn = null;
		Statement stmt = null;
		try {
			conn = HDataSource.getConnection();
			stmt = conn.createStatement();
			
			stmt.execute("update nexcorio_users set zerodha_service_token = NULL WHERE zerodha_user_id='" + zerodhaUserId + "'");
			Desktop.getDesktop().browse(new URI(kiteLoginUrl));			
			do {
				System.out.println("Sleeping till service token available");				
				Thread.sleep(5*1000);
				String sqlString = "select zerodha_service_token from nexcorio_users where zerodha_user_id='" + zerodhaUserId + "'";
				ResultSet rs = stmt.executeQuery(sqlString);
				while(rs.next()) {
					retVal = rs.getString("zerodha_service_token");
					break;
				}
				rs.close();
			} while(retVal==null);
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
		return retVal;
	}
	
	private void saveKiteAccessCodes(String zerodhaUserId, String serviceToken, String accessToken, String publicToken) {
		Connection conn = null;
		Statement stmt = null;
		try {
			conn = HDataSource.getConnection();
			stmt = conn.createStatement();
			
			stmt.execute("update nexcorio_users set zerodha_service_token='" + serviceToken + "', zerodha_access_token='" + accessToken +"', zerodha_public_token='"+ publicToken + "', zerodha_last_login_time= now() WHERE zerodha_user_id='" + zerodhaUserId + "'");
			
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
	}
	
	public void populateInstruments() {
		URLConnection urlConnection;
		try {
			
			Map<String, Long> mainInstruments = getMainInstruments();
			
			URL exchangeLink = new URL("https://api.kite.trade/instruments");
			urlConnection = exchangeLink.openConnection();
			urlConnection.setConnectTimeout(30000);
			urlConnection.setReadTimeout(30000);
			BufferedReader in = new BufferedReader(	new InputStreamReader(urlConnection.getInputStream()));
			String inputLine;
			boolean skippedHeader = false;
			while ((inputLine = in.readLine()) != null){				
				if(!skippedHeader) {
					skippedHeader = true;
					continue;
				}
				String[] eachFields = inputLine.split(",");
				if (eachFields.length>10) { // 256265,1001,NIFTY 50,"NIFTY 50",0,,0,0,0,EQ,INDICES,NSE
					
					String instrument_token = eachFields[0].trim();
					String exchange_token = eachFields[1].trim();
					String tradingsymbol = eachFields[2].trim(); 
					String underlyingName = getClearAlphaNumericText(eachFields[3].trim());
					String last_price = eachFields[4].trim();
					String expiry = eachFields[5].trim();
					String strike = eachFields[6].trim();
					String tick_size = eachFields[7].trim();
					String lot_size = eachFields[8].trim();
					String instrument_type = eachFields[9].trim(); // FUT or CE or PE or EQ
					String segment = eachFields[10].trim(); // BFO-FUT or NFO-FUT or INDICES
					String exchange = eachFields[11].trim(); // NFO or BFO

					if (exchange.equals("NFO") || exchange.equals("BFO")) {
						if (mainInstruments.get(underlyingName)!=null) { // Scrip found in main instruments table
							Long mainInstrumentId = mainInstruments.get(underlyingName);
							
							String fnoPrefix = "";
							if (instrument_type.equals("FUT")) {
								fnoPrefix = tradingsymbol.substring(0,tradingsymbol.indexOf("FUT"));
							} else if (instrument_type.equals("CE") || instrument_type.equals("PE")) {
								fnoPrefix = tradingsymbol.substring(0,tradingsymbol.indexOf(strike+""));
							}
							saveExpiryDate(mainInstrumentId, expiry, segment, fnoPrefix);
							
							saveFnOInstruments(mainInstrumentId, tradingsymbol, instrument_token, exchange, Integer.parseInt(strike), expiry);
							
							
						}
						
					}
					
				}
			}
			in.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private String getClearAlphaNumericText(String inputString) {
		return inputString.replaceAll("[^A-Za-z0-9]", "");
	}
	
	private Map<String, Long> getMainInstruments() {
		Map<String, Long> mainInstruments = new HashMap<String, Long>();
		Connection conn = null;
		Statement stmt = null;
		try {
			conn = HDataSource.getConnection();
			stmt = conn.createStatement();
			
			ResultSet rs = stmt.executeQuery("SELECT short_name, id FROM nexcorio_main_instruments WHERE IS_ACTIVE=TRUE");
			while(rs.next()) {
				mainInstruments.put(rs.getString("short_name"), rs.getLong("id"));
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
		return mainInstruments;
	}
	
	public List<MainInstruments> getMainInstrumentsDto() {
		List<MainInstruments> mainInstruments = new ArrayList<MainInstruments>();
		Connection conn = null;
		Statement stmt = null;
		try {
			conn = HDataSource.getConnection();
			stmt = conn.createStatement();
			
			ResultSet rs = stmt.executeQuery("SELECT id, name, short_name, instrument_type, exchange,"
					+ " zerodha_instrument_token, expiry_day, gap_between_strikes, order_freezing_quantity,"
					+ " no_of_future_expiry_data, no_of_options_expiry_data, no_of_options_strike_points, straddle_margin, half_Straddle_Margin, lot_size"
					+ " FROM nexcorio_main_instruments WHERE IS_ACTIVE=TRUE");
			while(rs.next()) {
				MainInstruments mainInstrument = new MainInstruments();
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
				mainInstruments.add(mainInstrument);
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
		return mainInstruments;
	}
	
	public MainInstruments getMainInstrumentDtoById(Long id) {
		MainInstruments mainInstrument = null;
		Connection conn = null;
		Statement stmt = null;
		try {
			conn = HDataSource.getConnection();
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
	
	private void saveExpiryDate(Long mainInstrumentId, String expiry, String segment, String fnoPrefix) {
		//log.info("In saveExpiryDate mainInstrumentId="+mainInstrumentId+ " expiry="+expiry+" segment="+segment+" fnoPrefix="+fnoPrefix);
		
		Connection conn = null;
		Statement stmt = null;
		try {
			conn = HDataSource.getConnection();
			stmt = conn.createStatement();
			
			String fetchSql = "SELECT id from nexcorio_fno_expiry_dates WHERE f_main_instrument="+mainInstrumentId+" AND expiry_date='"+expiry+"' AND fno_segment='"+segment+"' AND fno_prefix='"+fnoPrefix+"'";
			ResultSet rs = stmt.executeQuery(fetchSql);
			boolean recordExist = false;
			while(rs.next()) {
				recordExist = true;
			}
			rs.close();
			
			if (!recordExist) {
				log.info("Inserting");
				stmt.executeUpdate("INSERT INTO nexcorio_fno_expiry_dates (id, f_main_instrument, expiry_date, fno_segment, fno_prefix) "
						+ "VALUES (nextval('nexcorio_fno_expiry_dates_id_seq'),"+mainInstrumentId+",'"+expiry+"','"+segment+"','"+fnoPrefix+"')");
			} else {
				//log.info("Record already exist");
			}
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
		
		
	}
	
	private void saveFnOInstruments(Long mainInstrumentId, String tradingsymbol, String instrument_token, String exchange, int strike, String expiryDateStr) {
		//log.info("In saveFnOInstruments " + mainInstrumentId + "," + tradingsymbol + "," + instrument_token + "," + exchange);
		
		Connection conn = null;
		Statement stmt = null;
		try {
			conn = HDataSource.getConnection();
			stmt = conn.createStatement();
			
			String fetchSql = "SELECT id from nexcorio_fno_instruments WHERE trading_symbol='"+tradingsymbol+ "'";
			ResultSet rs = stmt.executeQuery(fetchSql);
			boolean recordExist = false;
			while(rs.next()) {
				recordExist = true;
			}
			rs.close();
			
			if (!recordExist) {
				log.info("Inserting");
				stmt.executeUpdate("INSERT INTO nexcorio_fno_instruments (id, f_main_instrument, trading_symbol, zerodha_instrument_token, exchange, strike, expiry_date) "
						+ "VALUES (nextval('nexcorio_fno_instruments_id_seq'),"+mainInstrumentId+ ",'" + tradingsymbol + "',"+instrument_token+",'"+exchange+"',"+strike+",'"+expiryDateStr+"')");
			} else {
				//log.info("Record already exist");
			}
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
	}
	
	public List<Long>  getZerodhaTokensToSubscribe() {
		List<Long> retList = new ArrayList<Long>();
		List<MainInstruments> mainInstruments = getMainInstrumentsDto();
		
		for(MainInstruments aMainInstrument : mainInstruments) {
			retList.add(aMainInstrument.getZerodhaInstrumentToken()); // Add self first, followed by next future and then options
			KiteCache.putInstrumentTokenToTradingSymbolCache(aMainInstrument.getZerodhaInstrumentToken(), aMainInstrument.getShortName());
			KiteCache.putTradingSymbolMainInstrumentCache(aMainInstrument.getName(), aMainInstrument);
			KiteCache.putTradingSymbolMainInstrumentCache(aMainInstrument.getShortName(), aMainInstrument);
			if (aMainInstrument.getNoOfFutureExpiryData()>0) {
				Map<Long, String> futureExpiryDateMap = getNextNFUTUREExpiryDate(aMainInstrument.getId(), 
						aMainInstrument.getExchange(), 
						aMainInstrument.getNoOfFutureExpiryData());
				Iterator<Long> iter = futureExpiryDateMap.keySet().iterator();
				while(iter.hasNext()) {
					Long zerodhaToken = iter.next();
					String tradingSymbol= futureExpiryDateMap.get(zerodhaToken);
					log.info("Adding future zerodhaToken="+zerodhaToken+" tradingSymbol="+tradingSymbol);
					retList.add(zerodhaToken);
					KiteCache.putInstrumentTokenToTradingSymbolCache(zerodhaToken, tradingSymbol);
					KiteCache.putTradingSymbolMainInstrumentCache(tradingSymbol, aMainInstrument);
				}
			}
			
			if (aMainInstrument.getNoOfOptionsExpiryData()>0 && aMainInstrument.getNoOfOptionsStrikePoints()>0) { // Options will be added only if expiry and strike points are available
				Map<Long, String> optionExpiryDateMap = getNextNOptionExpiryDate(aMainInstrument, 
						aMainInstrument.getExchange(), 
						aMainInstrument.getNoOfOptionsExpiryData(),
						aMainInstrument.getNoOfOptionsStrikePoints());
				
				Iterator<Long> iter = optionExpiryDateMap.keySet().iterator();
				while(iter.hasNext()) {
					Long zerodhaToken = iter.next();
					String tradingSymbol= optionExpiryDateMap.get(zerodhaToken);
					log.info("Adding option zerodhaToken="+zerodhaToken+" tradingSymbol="+tradingSymbol);
					retList.add(zerodhaToken);
					KiteCache.putInstrumentTokenToTradingSymbolCache(zerodhaToken, tradingSymbol);
					KiteCache.putTradingSymbolMainInstrumentCache(tradingSymbol, aMainInstrument);
				}
			}
			
		}
		
		return retList;
	}
	
	private Map<Long, String> getNextNOptionExpiryDate(MainInstruments mainInstrument, String exchange, int noOfOptionsExpiryData, int noOfOptionsStrikePoints) {
		Map<Long, String> retMap = new HashMap<Long, String>();
		Connection conn = null;
		Statement stmt = null;
		try {
			conn = HDataSource.getConnection();
			stmt = conn.createStatement();
			
			String fnoExchange = "NFO-OPT";
			if (exchange.equalsIgnoreCase("BSE")) fnoExchange = "BFO-OPT";
			
			String fetchSql = "SELECT fno_prefix from nexcorio_fno_expiry_dates WHERE f_main_instrument="+mainInstrument.getId()+ ""
					+ " and fno_segment='" + fnoExchange + "' "
					+ " and expiry_date >= (now() - '1 day'::interval) "
					+ " ORDER BY expiry_date ASC LIMIT "+noOfOptionsExpiryData;
			
			ResultSet rs = stmt.executeQuery(fetchSql);
			List<String> fnoPrefixes = new ArrayList<String>();			
			while(rs.next()) {
				fnoPrefixes.add(rs.getString("fno_prefix"));
			}
			rs.close();
			
			String scripnameForQuote = mainInstrument.getExchange() + ":" + mainInstrument.getShortName();
			if (mainInstrument.getInstrumentType().equalsIgnoreCase("INDEX")) {
				scripnameForQuote = mainInstrument.getExchange() + ":" + mainInstrument.getName();
			}
			//log.info("scripnameForQuote="+scripnameForQuote);
			
			String[] instruments = {scripnameForQuote}; 
			Map<String, LTPQuote> scripLtp = kiteConnect.getLTP(instruments);
			
			int scripSpotPrice  = (int) scripLtp.get(instruments[0]).lastPrice;
			//log.info("scripSpotPrice="+scripSpotPrice);
			
			// make last decimal zero
			scripSpotPrice = scripSpotPrice - (scripSpotPrice%10);
			
			int basePrice = scripSpotPrice;
			
			for(int i=0;i<10;i++) {
				String checkCEUpStr = fnoPrefixes.get(0) + (scripSpotPrice + i*10) + "CE";
				String checkCEDownStr = fnoPrefixes.get(0) + (scripSpotPrice - i*10) + "CE";
				
				fetchSql = "select trading_symbol, zerodha_instrument_token from nexcorio_fno_instruments"
						+ " where trading_symbol in ('" + checkCEUpStr+ "','"+checkCEDownStr+"')";
				//log.info("fetchSql="+fetchSql);
				
				rs = stmt.executeQuery(fetchSql);
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
			//log.info("basePrice="+basePrice);
			
			for(int spotPointDiff=0;
					spotPointDiff<noOfOptionsStrikePoints;
					spotPointDiff=spotPointDiff+mainInstrument.getGapBetweenStrikes()) {
				//log.info("spotPointDiff="+spotPointDiff);
				for(int i=0;i<fnoPrefixes.size();i++) {
					
					fetchSql = "select trading_symbol, zerodha_instrument_token, exchange from nexcorio_fno_instruments"
							+ " where trading_symbol in ("
							+ "  '" + fnoPrefixes.get(i) + (basePrice + spotPointDiff) + "CE'"
							+ ", '" + fnoPrefixes.get(i) + (basePrice + spotPointDiff) + "PE'"
							+ ", '" + fnoPrefixes.get(i) + (basePrice - spotPointDiff) + "CE'"
							+ ", '" + fnoPrefixes.get(i) + (basePrice - spotPointDiff) + "PE')";
					
					//log.info("fetchSql="+fetchSql);
					
					rs = stmt.executeQuery(fetchSql);
					while(rs.next()) {
						String tradingSymbol = rs.getString("trading_symbol");
						retMap.put(rs.getLong("zerodha_instrument_token"), tradingSymbol);
						KiteCache.putTradingSymbolExchangeCache( tradingSymbol, rs.getString("exchange") );
					}
					rs.close();
				}
			}
			stmt.close();
		} catch (Exception | KiteException ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (conn!=null) conn.close();
			} catch (SQLException e) {
				log.error(e);
			}
		}
		return retMap;
	}
	
	private Map<Long, String> getNextNFUTUREExpiryDate(Long mainInstrumentId, String exchange, int noOfFutureExpiryData) {
		Map<Long, String> retMap = new HashMap<Long, String>();
		Connection conn = null;
		Statement stmt = null;
		try {
			conn = HDataSource.getConnection();
			stmt = conn.createStatement();
			
			String fnoExchange = "NFO-FUT";
			if (exchange.equalsIgnoreCase("BSE")) fnoExchange = "BFO-FUT";
			
			String fetchSql = "SELECT fno_prefix from nexcorio_fno_expiry_dates WHERE f_main_instrument="+mainInstrumentId+ ""
					+ " and fno_segment='" + fnoExchange + "' "
					+ " and expiry_date >= (now() - '1 day'::interval) "
					+ " ORDER BY expiry_date ASC LIMIT "+noOfFutureExpiryData;
			
			ResultSet rs = stmt.executeQuery(fetchSql);
			List<String> fnoPrefixes = new ArrayList<String>();			
			while(rs.next()) {
				fnoPrefixes.add(rs.getString("fno_prefix") + "FUT");
			}
			rs.close();
			
			fetchSql = "select trading_symbol, zerodha_instrument_token, exchange from nexcorio_fno_instruments where trading_symbol in (" + getQuotedString(fnoPrefixes) + ")";
			rs = stmt.executeQuery(fetchSql);
			while(rs.next()) {
				retMap.put(rs.getLong("zerodha_instrument_token"), rs.getString("trading_symbol"));
				KiteCache.putTradingSymbolExchangeCache( rs.getString("trading_symbol"), rs.getString("exchange") );
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
		return retMap;
	}
	
	
	
	public void tickerUsage(ArrayList<Long> zerodhaTokens) throws IOException, WebSocketException, KiteException {
        /** To get live price use com.rainmatter.ticker websocket connection. It is recommended to use only one websocket connection at any point of time and make sure you stop connection, once user goes out of app.*/
        
        log.info("tokens size="+zerodhaTokens.size());
        KiteTicker tickerProvider = new KiteTicker(kiteConnect.getAccessToken(), kiteConnect.getApiKey());
        
        tickerProvider.setOnConnectedListener(new OnConnect() {
			
			@Override
			public void onConnected() {								
				tickerProvider.subscribe(zerodhaTokens);
				tickerProvider.setMode(zerodhaTokens, KiteTicker.modeFull);
			}
		});
        
        tickerProvider.setOnDisconnectedListener(new OnDisconnect() {
			
			@Override
			public void onDisconnected() {				
			}
		});
        
        tickerProvider.setOnTickerArrivalListener(new OnTicks() {
			
			@Override
			public void onTicks(ArrayList<Tick> ticks) {
				if (ticks.size()>0) {
            		log.info("Ticks recieved");
            		new ZerodhaIntradayStreamingThread(ticks);
            	}
				if ((new Date()).after(KiteUtil.getDailyCustomTime(15, 29, 45))) {
					log.info("End of the daym disconnect and logout");
					try {
						tickerProvider.disconnect();
						//kiteConnect.logout();
					} catch (JSONException e) {
						e.printStackTrace();
					}
				}
			}
		});
                
        /** for reconnection of ticker when there is abrupt network disconnection, use the following code
            by default tryReconnection is set to false */
        tickerProvider.setTryReconnection(true);
        //minimum value must be 5 for time interval for reconnection
        tickerProvider.setMaximumRetryInterval(10);
        //set number to times ticker can try reconnection, for infinite retries use -1
        tickerProvider.setMaximumRetries(5000);

        /** connects to com.rainmatter.ticker server for getting live quotes*/
        tickerProvider.connect();

        /** You can check, if websocket connection is open or not using the following method.*/
        boolean isConnected = tickerProvider.isConnectionOpen();
        System.out.println("isConnected"+isConnected);

        /** set mode is used to set mode in which you need tick for list of tokens.
         * Ticker allows three modes, modeFull, modeQuote, modeLTP.
         * For getting only last traded price, use modeLTP
         * For getting last traded price, last traded quantity, average price, volume traded today, total sell quantity and total buy quantity, open, high, low, close, change, use modeQuote
         * For getting all data with depth, use modeFull*/
        
        tickerProvider.setMode(zerodhaTokens, KiteTicker.modeFull);

        // After using com.rainmatter.ticker, close websocket connection.
        //tickerProvider.disconnect();
	}
	
	public String getQuotedString(List<String> strList) {
		String retStr = "";
		for(int i=0;i<strList.size();i++) {
			if (retStr.length()>0) retStr = retStr + ",";
			retStr = retStr + "'" + strList.get(i) + "'";
		}
		return retStr;
	}
	
	public static void main(String[] args) {
		String inpStr = "\"Keshav\"";
		KiteHelper kiteHelper = new KiteHelper();
		System.out.println(inpStr + ","+  kiteHelper.getClearAlphaNumericText(inpStr));
	}

}
