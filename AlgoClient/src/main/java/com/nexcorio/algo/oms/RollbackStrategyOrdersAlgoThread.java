package com.nexcorio.algo.oms;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.util.ApplicationConfig;
import com.nexcorio.algo.util.FileLogTelegramWriter;
import com.nexcorio.algo.util.TelegramUtil;
import com.nexcorio.algo.util.db.HDataSource;
import com.zerodhatech.kiteconnect.KiteConnect;
import com.zerodhatech.kiteconnect.kitehttp.exceptions.InputException;
import com.zerodhatech.kiteconnect.kitehttp.exceptions.KiteException;
import com.zerodhatech.kiteconnect.utils.Constants;
import com.zerodhatech.models.Order;
import com.zerodhatech.models.OrderParams;
import com.zerodhatech.models.OrderResponse;

public class RollbackStrategyOrdersAlgoThread implements Runnable {

	private static final Logger log = LogManager.getLogger(RollbackStrategyOrdersAlgoThread.class);
	
	FileLogTelegramWriter fileLogTelegramWriter;
	
	Long userId = null;
	String algoTag;
	KiteConnect kiteConnect;
	
	SimpleDateFormat postgresLongDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public RollbackStrategyOrdersAlgoThread(Long userId, String algoTag) {
		super();
		
		this.userId = userId;
		
		this.algoTag=algoTag;
		
		Thread t = new Thread(this, this.algoTag);
		t.setPriority(Thread.MAX_PRIORITY);
		t.start();
	}
	
	@Override
	public void run() {
		try {			
			fileLogTelegramWriter = new FileLogTelegramWriter("Generic", "Rollback-User"+userId + "algoTag-"+this.algoTag, null);
			
			initialize();
			
			List<Order> orders = kiteConnect.getOrders();
			
			Map<String, Integer> executedOrders = new HashMap<String, Integer>();
			System.out.println("algoTag="+algoTag);
		    for (Order order : orders) {
		        System.out.println("Order Tag:" + order.tag+":");
		        System.out.println("Status: " + order.status); //COMPLETE
		        
		        if (order.status.equalsIgnoreCase("COMPLETE") && order.tag!=null) { 
		        	if (order.tag.contains(algoTag)) {
		        		System.out.println(order.tradingSymbol+"~~~~~~~~~~~" + order.quantity+" " + order.transactionType);
		        		
		        		int qty = Integer.parseInt(order.quantity);
		        		if (order.transactionType.equals("SELL")) qty = -qty;
		        		
		        		int existingQty = 0;
		        		if (executedOrders.get(order.tradingSymbol)!=null) {
		        			existingQty = executedOrders.get(order.tradingSymbol);
		        		}
		        		existingQty = existingQty + qty;
		        		executedOrders.put(order.tradingSymbol, existingQty);		
		        	}		        	
		        }
		    }
			
		    Iterator<String> iter = executedOrders.keySet().iterator();
		    while(iter.hasNext()) {
		    	String aKey = iter.next();
		    	System.out.println("For " + aKey + " open qty="+executedOrders.get(aKey));
		    	if (executedOrders.get(aKey)<0) {
		    		placeKiteOrder(aKey, executedOrders.get(aKey), "BUY");
		    	}
		    }
		    
		    iter = executedOrders.keySet().iterator();
		    while(iter.hasNext()) {
		    	String aKey = iter.next();
		    	System.out.println("For " + aKey + " open qty="+executedOrders.get(aKey));
		    	if (executedOrders.get(aKey)>0) {
		    		placeKiteOrder(aKey, executedOrders.get(aKey), "SELL");
		    	}
		    }
		    
		    
			fileLogTelegramWriter.close();
		} catch (Exception | KiteException e) {			
			log.error("Error"+e.getMessage(), e);
			sendKiteExceptionAlerts("KiteException in Rollback ");
		}
	}
	
	private void sendKiteExceptionAlerts(String msg) {
		try {
			TelegramUtil.postTelegramMessage("@NseFnOAutoPicks", ApplicationConfig.getProperty("zerodha.user.id") + "-Algo " + msg);
		} catch (Exception e) {			
			e.printStackTrace();
			log.error("Error in sendAlerts"+e.getMessage(), e);
		}
	}
	
	private void initialize() {
			
		Connection conn = null;
		Statement stmt = null;
		
		try {
			conn = HDataSource.getReadOnlyConnection();
			stmt = conn.createStatement();
			
			String fetchSql = "select id, zerodha_api_key, zerodha_api_secret_key, zerodha_service_token, zerodha_access_token, zerodha_public_token, zerodha_user_id FROM nexcorio_users WHERE id='" + this.userId + "'";
				
			ResultSet rs = stmt.executeQuery(fetchSql);
			while(rs.next()) {
				this.kiteConnect = new KiteConnect(rs.getString("zerodha_api_key"));
				this.kiteConnect.setUserId(rs.getString("zerodha_user_id"));
				this.kiteConnect.setAccessToken(rs.getString("zerodha_access_token"));
				this.kiteConnect.setPublicToken(rs.getString("zerodha_public_token"));
			}
			rs.close();
			stmt.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void placeKiteOrder(String tradingSymbol, int qty, String transactionType) {
		System.out.println(tradingSymbol+" "+transactionType+" "+qty);
		
		fileLogTelegramWriter.write("In rollback placeKiteOrder(optionname:"+tradingSymbol+" quantity=" + qty+" transactionType="+transactionType);
		
		try {
			String exchange = "NFO";
			if (tradingSymbol.startsWith("SENSEX")) {
				exchange = "BFO";
			}
			
			OrderParams orderParameters = new OrderParams();
			
	        orderParameters.orderType=Constants.ORDER_TYPE_MARKET;
	        orderParameters.exchange=exchange;
	        orderParameters.validity=Constants.VALIDITY_DAY;
	        orderParameters.tradingsymbol=tradingSymbol;
			orderParameters.transactionType=transactionType;
			orderParameters.product= Constants.PRODUCT_MIS;
			orderParameters.marketProtection = 10; // April 1st 2026, SEBI changes to add market protection percentage for MARKET AND SL-M orders 
	        
			orderParameters.quantity = Math.abs(qty);
			sendAlerts(algoTag, tradingSymbol+" "+transactionType+" "+qty);
			
			OrderResponse aOrderResponse = this.kiteConnect.placeOrder(orderParameters, Constants.VARIETY_REGULAR);
		} catch (InputException e) {
			log.error("InputException "+e.getMessage(), e);
			fileLogTelegramWriter.write("Exception: " + e.message);
			System.out.println(e.getCause());
			System.out.println(e.getLocalizedMessage());
			e.printStackTrace();
			
		} catch (KiteException e) {
			log.error("KiteException "+e.getMessage(), e);
			fileLogTelegramWriter.write("Exception: " + e.message);
			
		} catch (Exception e) {
			log.error("Error"+e.getMessage(), e);
			fileLogTelegramWriter.write("Exception: " + e.getMessage());
			
		}
	}
	
	private void sendAlerts(String algoTag, String message) {
		try {
			TelegramUtil.postTelegramMessage("@NseFnOAutoPicks", ApplicationConfig.getProperty("zerodha.user.id") + "-Algo " + algoTag + ": Rollback " + message);
		} catch (Exception e) {			
			e.printStackTrace();
			log.error("Error in sendAlerts"+e.getMessage(), e);
		}
	}
	
	public static void main(String[] args) {
		new RollbackStrategyOrdersAlgoThread(1L, "X316");
		
		
	}
}
