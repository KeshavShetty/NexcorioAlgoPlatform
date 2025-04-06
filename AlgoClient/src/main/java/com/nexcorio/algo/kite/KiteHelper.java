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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.neovisionaries.ws.client.WebSocketException;
import com.nexcorio.algo.dto.MainInstruments;
import com.nexcorio.algo.util.ApplicationConfig;
import com.nexcorio.algo.util.db.HDataSource;
import com.zerodhatech.kiteconnect.KiteConnect;
import com.zerodhatech.kiteconnect.kitehttp.SessionExpiryHook;
import com.zerodhatech.kiteconnect.kitehttp.exceptions.KiteException;
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

	protected SimpleDateFormat postgresLongDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	protected SimpleDateFormat postgresShortDateFormat = new SimpleDateFormat("yyyy-MM-dd");
	
	public KiteConnect login() {
		KiteConnect kiteconnect = null;	
		try {
			String USER_ID = ApplicationConfig.getProperty("zerodha.user.id");
			
			ZerodhaAccountKeys zerodhaAccountKeys = getZerodhaAccountKeys(USER_ID);
			kiteconnect = new KiteConnect(zerodhaAccountKeys.getApiKey());				
            kiteconnect.setUserId(USER_ID);
            String kiteLoginURL = kiteconnect.getLoginURL();
            
            String requestToken = getRequestToken(USER_ID, kiteLoginURL);
            User userModel =  kiteconnect.generateSession(requestToken, zerodhaAccountKeys.getApiSecretKey());

            kiteconnect.setAccessToken(userModel.accessToken);
            kiteconnect.setPublicToken(userModel.publicToken);
            
            log.info("kiteconnect kiteLoginURL={"+kiteLoginURL+"}");
            log.info("kiteconnect AccessToken={"+kiteconnect.getAccessToken()+"}");
            log.info("kiteconnect ApiKey={"+kiteconnect.getApiKey()+"}");
            log.info("kiteconnect PublicToken={"+kiteconnect.getPublicToken()+"}");
            log.info("kiteconnect UserId={"+kiteconnect.getUserId()+"}");
            
            saveKiteAccessCodes(USER_ID, requestToken, kiteconnect.getAccessToken(), kiteconnect.getPublicToken()); // For future when decide multi user system
            // Set session expiry callback.
            kiteconnect.setSessionExpiryHook((new SessionExpiryHook() {
				@Override
				public void sessionExpired() {}
			}));
		} catch(Exception ex) {
			ex.printStackTrace();
		} catch (KiteException e) {
			e.printStackTrace();
		}
		return kiteconnect;
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
							
							saveFnOInstruments(mainInstrumentId, tradingsymbol, instrument_token, exchange);
							
							
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
	
	private List<MainInstruments> getMainInstrumentsDto() {
		List<MainInstruments> mainInstruments = new ArrayList<MainInstruments>();
		Connection conn = null;
		Statement stmt = null;
		try {
			conn = HDataSource.getConnection();
			stmt = conn.createStatement();
			
			ResultSet rs = stmt.executeQuery("SELECT id, short_name, exchange, zerodha_instrument_token, expiry_day,"
					+ " no_of_future_expiry_data, no_of_options_expiry_data, no_of_options_strike_points"
					+ " FROM nexcorio_main_instruments WHERE IS_ACTIVE=TRUE");
			while(rs.next()) {
				MainInstruments mainInstrument = new MainInstruments();
				mainInstrument.setId(rs.getLong("id"));
				mainInstrument.setShortName(rs.getString("short_name"));
				mainInstrument.setExchange(rs.getString("exchange"));
				mainInstrument.setZerodhaInstrumentToken(rs.getLong("zerodha_instrument_token"));
				mainInstrument.setExpiryDay(rs.getInt("expiry_day"));
				mainInstrument.setNoOfFutureExpiryData(rs.getInt("no_of_future_expiry_data"));
				mainInstrument.setNoOfOptionsExpiryData(rs.getInt("no_of_options_expiry_data")); 
				mainInstrument.setNoOfOptionsStrikePoints(rs.getInt("no_of_options_strike_points"));
				
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
	
	private void saveExpiryDate(Long mainInstrumentId, String expiry, String segment, String fnoPrefix) {
		log.info("In saveExpiryDate mainInstrumentId="+mainInstrumentId+ " expiry="+expiry+" segment="+segment+" fnoPrefix="+fnoPrefix);
		
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
				log.info("Record already exist");
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
	
	private void saveFnOInstruments(Long mainInstrumentId, String tradingsymbol, String instrument_token, String exchange) {
		log.info("In saveFnOInstruments " + mainInstrumentId + "," + tradingsymbol + "," + instrument_token + "," + exchange);
		
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
				stmt.executeUpdate("INSERT INTO nexcorio_fno_instruments (id, f_main_instrument, trading_symbol, zerodha_instrument_token, exchange) "
						+ "VALUES (nextval('nexcorio_fno_instruments_id_seq'),"+mainInstrumentId+ ",'" + tradingsymbol + "',"+instrument_token+",'"+exchange+"')");
			} else {
				log.info("Record already exist");
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
			if (aMainInstrument.getNoOfFutureExpiryData()>0) {
				List<Long> futureInstrumentTokens = getNextNFUTUREInstrumentTokens(aMainInstrument.getNoOfFutureExpiryData());
				retList.addAll(futureInstrumentTokens);
			}
			
		}
		
		return retList;
	}
	
	public void tickerUsage(KiteConnect kiteConnect, ArrayList<Long> zerodhaTokens) throws IOException, WebSocketException, KiteException {
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
            		// Todo: Do something with ticks
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
	
	public static void main(String[] args) {
		String inpStr = "\"Keshav\"";
		KiteHelper kiteHelper = new KiteHelper();
		System.out.println(inpStr + ","+  kiteHelper.getClearAlphaNumericText(inpStr));
	}

}
