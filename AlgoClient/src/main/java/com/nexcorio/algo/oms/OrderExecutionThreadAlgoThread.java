package com.nexcorio.algo.oms;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.kite.KiteCache;
import com.nexcorio.algo.kite.KiteHelper;
import com.nexcorio.algo.util.FileLogTelegramWriter;
import com.nexcorio.algo.util.KiteUtil;
import com.nexcorio.algo.util.db.HDataSource;
import com.zerodhatech.kiteconnect.KiteConnect;
import com.zerodhatech.kiteconnect.kitehttp.exceptions.InputException;
import com.zerodhatech.kiteconnect.kitehttp.exceptions.KiteException;
import com.zerodhatech.kiteconnect.utils.Constants;
import com.zerodhatech.models.Margin;
import com.zerodhatech.models.Order;
import com.zerodhatech.models.OrderParams;
import com.zerodhatech.models.Position;
import com.zerodhatech.models.Trade;

class KiteOrderDetails {
	
	Long id;
	
	Long algoOrderId;
	String option_name;
	int quantity;
	String transactionType;
	boolean waitforpositionfill;
	
	String placedKiteOrderId;
	
	public KiteOrderDetails(Long id, Long algoOrderId, String option_name, int quantity, String transactionType,
			boolean waitforpositionfill) {
		super();
		this.id = id;
		this.algoOrderId = algoOrderId;
		this.option_name = option_name;
		this.quantity = quantity;
		this.transactionType = transactionType;
		this.waitforpositionfill = waitforpositionfill;
	}
	
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	public Long getAlgoOrderId() {
		return algoOrderId;
	}
	public void setAlgoOrderId(Long algoOrderId) {
		this.algoOrderId = algoOrderId;
	}
	public String getOption_name() {
		return option_name;
	}
	public void setOption_name(String option_name) {
		this.option_name = option_name;
	}
	public int getQuantity() {
		return quantity;
	}
	public void setQuantity(int quantity) {
		this.quantity = quantity;
	}
	public String getTransactionType() {
		return transactionType;
	}
	public void setTransactionType(String transactionType) {
		this.transactionType = transactionType;
	}
	public boolean isWaitforpositionfill() {
		return waitforpositionfill;
	}
	public void setWaitforpositionfill(boolean waitforpositionfill) {
		this.waitforpositionfill = waitforpositionfill;
	}

	public String getPlacedKiteOrderId() {
		return placedKiteOrderId;
	}

	public void setPlacedKiteOrderId(String placedKiteOrderId) {
		this.placedKiteOrderId = placedKiteOrderId;
	}
}

public class OrderExecutionThreadAlgoThread implements Runnable{

	private static final Logger log = LogManager.getLogger(OrderExecutionThreadAlgoThread.class);
	
	FileLogTelegramWriter fileLogTelegramWriter;
	
	Long userId = null;
	String algoname;
	int noOfOrdersExecuted = 0;
	boolean exitThread = false;
	KiteConnect kiteConnect;
	
	SimpleDateFormat postgresLongDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public OrderExecutionThreadAlgoThread(Long userId) {
		super();
		
		this.userId = userId;
		
		this.algoname="OrderExecutionThread-User"+userId;
		
		Thread t = new Thread(this, this.algoname);
		t.setPriority(Thread.MAX_PRIORITY);
		t.start();
	}
	
	private void initialize() {
			
		Connection conn = null;
		Statement stmt = null;
		
		try {
			conn = HDataSource.getConnection();
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
	
	private void updateOrderStatus(Long orderId, String placedKiteOrderId) {
		Connection conn = null;
		try {
			conn = HDataSource.getConnection();
			Statement stmt = conn.createStatement();
			String status = "SUCCESS";
			if (placedKiteOrderId==null)  status = "FAILED";
			stmt.executeUpdate("UPDATE option_kite_orders set status='" + status +"', executed_time=NOW() WHERE id="+orderId);
			stmt.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	private List<KiteOrderDetails> getPendingOrderIds() {
		List<KiteOrderDetails> retList =  new ArrayList<KiteOrderDetails>();
		
		Connection conn = null;
		try {
			conn = HDataSource.getConnection();
			Statement stmt = conn.createStatement();
			
			String opOIFetch = "select id, algo_order_id, option_name, quantity, transaction_type, waitforpositionfill from nexcorio_real_orders where f_user = " + this.userId + " and status='PENDING'";
			//fileLogTelegramWriter.write("opOIFetch="+opOIFetch);
			
			ResultSet rs = stmt.executeQuery(opOIFetch);
			while (rs.next()) {
				KiteOrderDetails aOrder = new KiteOrderDetails(rs.getLong("id"), 
						rs.getLong("algo_order_id"), 
						rs.getString("option_name"), 
						rs.getInt("quantity"), 
						rs.getString("transaction_type"),
						rs.getBoolean("waitforpositionfill"));
				retList.add(aOrder);
			}
			rs.close();
			stmt.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (conn!=null) conn.close();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		return retList;
	}
	
	private String placeKiteOrder(String optionname, int quantity, String transactionType, boolean waitForPositionFill, boolean useNormal) {
		log.info("In placeKiteOrder(optionname:"+optionname+" quantity=" + quantity+" transactionType="+transactionType+" useNormal="+useNormal);
		fileLogTelegramWriter.write("In placeKiteOrder(optionname:"+optionname+" quantity=" + quantity+" transactionType="+transactionType+" useNormal="+useNormal);
		String orderId = null;
		try {
			int freezeLimitPerOrder = KiteCache.getTradingSymbolMainInstrumentCache(optionname).getOrderFreezingQuantity();//KiteUtil.getOptionIndexInstrumentMetaData(KiteUtil.getIndexnameFromOptionName(optionname)).getOrderFreezingQuantity(); 
			fileLogTelegramWriter.write("freezeLimitPerOrder="+freezeLimitPerOrder);
			OrderParams orderParameters = new OrderParams();
			
	        orderParameters.orderType=Constants.ORDER_TYPE_MARKET;
	        orderParameters.exchange="NFO";
	        orderParameters.validity=Constants.VALIDITY_DAY;
	        orderParameters.tradingsymbol=optionname;
			orderParameters.transactionType=transactionType;
			
			orderParameters.product= Constants.PRODUCT_MIS;
	        if (useNormal==true) orderParameters.product= Constants.PRODUCT_NRML;
	        
	        if (optionname!=null) {
	        	int remainingQty = quantity;
	        	int openPositionsBeforeOrder = waitForPositionFill?getOpenPosition(optionname):0;
	        	do {
	        		orderParameters.quantity = remainingQty>freezeLimitPerOrder?freezeLimitPerOrder:remainingQty;
	        		if (this.noOfOrdersExecuted>=10) {
	        			fileLogTelegramWriter.write("Zerodha order limit per second exceeded, going to sleep");
	        			Thread.sleep(2000); // 2sec sleep
	        			this.noOfOrdersExecuted = 0;
	        		}
	        		Order aOrder = this.kiteConnect.placeOrder(orderParameters, Constants.VARIETY_REGULAR);
	        		orderId = aOrder.orderId;
					log.info("Order placed orderId" + aOrder.orderId);
					remainingQty = remainingQty>freezeLimitPerOrder?(remainingQty-freezeLimitPerOrder):0;
					log.info("Remaining qty=" +remainingQty);
					noOfOrdersExecuted++;
	        	} while(remainingQty>0);
	        	if (waitForPositionFill) {
	        		waitTillAllPositionFilled(optionname, orderParameters.transactionType.equals("BUY")? orderParameters.quantity:-orderParameters.quantity, openPositionsBeforeOrder);
	        	}
	        }
		} catch (InputException e) {
			log.error("InputException "+e.getMessage(), e);
			fileLogTelegramWriter.write("Exception: " + e.message);
			orderId = null;
		} catch (KiteException e) {
			log.error("KiteException "+e.getMessage(), e);
			fileLogTelegramWriter.write("Exception: " + e.message);
			orderId = null;
		} catch (Exception e) {
			log.error("Error"+e.getMessage(), e);
			fileLogTelegramWriter.write("Exception: " + e.getMessage());
			orderId = null;
		}
		return orderId;
	}
	
	private void waitTillAllPositionFilled(String optionName, int orderQuantity, int openPositionsBeforeOrder) {
		log.info("In waitTillAllPositionFilled for optionName="+optionName+" orderQuantity="+orderQuantity+" openPositionsBeforeOrder="+openPositionsBeforeOrder);
		try {
			int newOpenPositionAfterOrder = 0;
	    	int numberOfSecondPassed = 0;
	    	do {
	    		Thread.sleep(500); // Sleep till order executed
	    		newOpenPositionAfterOrder = getOpenPosition(optionName);
	    		log.info(optionName + " waitTillAllPositionFilled loop qtyFromBuyOrder="+newOpenPositionAfterOrder+" ordered quantity="+orderQuantity);
	    		numberOfSecondPassed++;
	    	} while(newOpenPositionAfterOrder!=openPositionsBeforeOrder+orderQuantity && numberOfSecondPassed<10); // Either all quantity filled or 10 seconds over 20*500ms
		} catch (Exception ex) {
			log.error("Error waitTillAllPositionFilled", ex);
		}
	}
	
	private int getOpenPosition(String optionName) {
		int retVal = 0;
		try {
			Map<String, List<Position>> positions;
			positions = kiteConnect.getPositions();
			
	    	List<Position> netPositions = positions.get("net");
    		for(int i=0;i<netPositions.size();i++) {	
    			Position aPosition = netPositions.get(i);
    			
    			if (aPosition.netQuantity!=0 && aPosition.tradingSymbol.contains(optionName)) {
    				retVal = aPosition.netQuantity;
    				break;
    			}
    		}
		} catch (Exception | KiteException e) {			
			log.error("Error",e);
		}
		log.info("In getOpenPosition "+ optionName + " qty:"+retVal);
		return retVal;
	}
	
	private boolean isUserLevelRealtimeOrderEnabled() {
		boolean retVal = false;
		Connection conn = null;
		try {
			conn = HDataSource.getConnection();
			Statement stmt = conn.createStatement();
			
			String opOIFetch = "select enableRealtimeOrder from nexcorio_users where id = " + this.userId ;
			fileLogTelegramWriter.write("opOIFetch="+opOIFetch);
			
			ResultSet rs = stmt.executeQuery(opOIFetch);
			while (rs.next()) {
				retVal = rs.getBoolean("enableRealtimeOrder");
			}
			rs.close();
			stmt.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		//System.out.println("isUserLevelRealtimeOrderEnabled="+retVal);
		return retVal;
	}
	
	private void updateStaleOrderStatus() {
		Connection conn = null;
		try {
			conn = HDataSource.getConnection();
			Statement stmt = conn.createStatement();			
			stmt.executeUpdate("UPDATE nexcorio_real_orders set status='EXPIRED' WHERE f_user="+this.userId+" and status='PENDING'");
			stmt.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}	
	}
	
	private float printAndUpdateOrderDetails(String kitePlacedOrderId) {
		fileLogTelegramWriter.write("In printOrderDetails for kitePlacedOrderId="+kitePlacedOrderId);
		float retVal = 0f;
		try {
			List<Trade> kiteOrders = kiteConnect.getOrderTrades(kitePlacedOrderId);
			 
			for(Trade aTrade: kiteOrders) {
				fileLogTelegramWriter.write("For trading symbol " + aTrade.tradingSymbol +" avereage price =" + aTrade.averagePrice);
				retVal = Float.parseFloat(aTrade.averagePrice.trim());
			}
			
			List<Order> kiteOrderHistory = kiteConnect.getOrderHistory(kitePlacedOrderId);
			for(Order aOrder: kiteOrderHistory) {
				fileLogTelegramWriter.write("Order history for trading symbol " + aOrder.tradingSymbol +" filled Qty="+ aOrder.filledQuantity + " avereage price =" + aOrder.averagePrice+" status="+aOrder.status+" statusMessage=" + aOrder.statusMessage);
			}
		} catch(Exception e) {
			log.error("Error"+e.getMessage(), e);
		} catch (KiteException e) {
			e.printStackTrace();
		}
		return retVal;
	}
	
	@Override
	public void run() {
		try {			
			fileLogTelegramWriter = new FileLogTelegramWriter("Generic", this.algoname, null);
			
			initialize();
			int i=0;
			double lowestPnL = 0d;
			double maxPnL = 0d;
			
			float maxUtilizedMargin = 0f;
			float minUtilizedMargin = 1000000000f;
			
			do {
				Thread.sleep(100);
				i++;
				this.noOfOrdersExecuted = 0;
				
				List<KiteOrderDetails> optionKiteOrders = getPendingOrderIds();
				
				if (optionKiteOrders.size()>0) {
					if (isUserLevelRealtimeOrderEnabled()) {
						for(KiteOrderDetails aOrder: optionKiteOrders) {
							String placedKiteOrderId = placeKiteOrder(aOrder.getOption_name(), aOrder.getQuantity(), aOrder.getTransactionType(), aOrder.isWaitforpositionfill(), KiteUtil.USE_NORMAL_ORDER_FALSE);
							updateOrderStatus(aOrder.getId(), placedKiteOrderId);
							aOrder.setPlacedKiteOrderId(placedKiteOrderId);
						}
						Thread.sleep(1000);
						for(KiteOrderDetails aOrder: optionKiteOrders) {
							if (aOrder.getPlacedKiteOrderId()!=null && aOrder.getAlgoOrderId()>0L) {
								float averageOrderPrice = printAndUpdateOrderDetails(aOrder.getPlacedKiteOrderId());
								if (averageOrderPrice>0f) updateAlgoOrderPrice(aOrder.getAlgoOrderId(), aOrder.getOption_name(), aOrder.getTransactionType(), averageOrderPrice); 
							}
						}
					}
				} else {
					//Todo: Fecth PnL
					if (i>600) {
						i=0;
						fileLogTelegramWriter.write("Wakeup & fetching Pnl and Margin utilization orders");
						float currentUtilizedMrgin = checkMarginUtilization();
						double currentPnL = getPositionPnL();
						if (currentPnL>maxPnL) maxPnL = currentPnL;
						else if (currentPnL<lowestPnL) lowestPnL = currentPnL;
						
						if (currentUtilizedMrgin > maxUtilizedMargin) maxUtilizedMargin = currentUtilizedMrgin;
						else if (currentUtilizedMrgin!=0f && currentUtilizedMrgin < minUtilizedMargin) minUtilizedMargin = currentUtilizedMrgin;
						
					}
				}
				
				if ((new Date()).after(KiteUtil.getDailyCustomTime(15, 30, 0))) {
					this.exitThread = true;
				}
			} while(!this.exitThread);
			fileLogTelegramWriter.write("lowestPnL="+lowestPnL+" maxPnL="+maxPnL+" maxUtilizedMargin="+maxUtilizedMargin+" minUtilizedMargin="+minUtilizedMargin);
			updateStaleOrderStatus(); 
			
			fileLogTelegramWriter.close();
		} catch (Exception e) {			
			log.error("Error"+e.getMessage(), e);
		}
	}
	
	private float checkMarginUtilization() {
		float utilizedMargin = 0f;
		try {
			Map<String, Margin> availableMargins = kiteConnect.getMargins();
			Iterator<String> iter = availableMargins.keySet().iterator();
			while(iter.hasNext()) {
				String mapKey = (String) iter.next();
				if (mapKey.equals(KiteUtil.SEGMENT_EQUITY)) {
					Margin aMargin = availableMargins.get(mapKey);					
					fileLogTelegramWriter.write("Margin of Segment="+mapKey+", Net="+aMargin.net+", Available=" + aMargin.available.liveBalance + ", Utilised debits="+aMargin.utilised.debits);
					utilizedMargin = Float.parseFloat(aMargin.utilised.debits);
				}
			}
		} catch (Exception | KiteException e) {			
			e.printStackTrace();
			log.info("Error in checkDailyMarginUsed"+e.getMessage(), e);
		}
		return utilizedMargin;
	}
	
	public double getPositionPnL() {
		double totalPnl = 0f;
		try {
			
			Map<String, List<Position>> positions = kiteConnect.getPositions();
			Iterator<String> iter = positions.keySet().iterator();
			while(iter.hasNext()) {
				String mapKey = (String) iter.next();
				if (mapKey.equals("day")) {
					List<Position> positionList = positions.get(mapKey);
					for(Position aPosition: positionList) { 
						// pnl = (sellValue - buyValue) + (netQuantity * lastPrice * multiplier);					
						double aPositionPNL = (aPosition.sellValue - aPosition.buyValue) + (aPosition.netQuantity*aPosition.lastPrice*aPosition.multiplier);
								
						//fileLogTelegramWriter.write(" For "+ aPosition.tradingSymbol + " PnL="+aPositionPNL);
						totalPnl = totalPnl + aPositionPNL;
					}
				}
			}
			fileLogTelegramWriter.write("TotalPnl="+totalPnl);
		} catch (Exception | KiteException e) {			
			e.printStackTrace();
			log.info("Error in checkDailyMarginUsed"+e.getMessage(), e);
		}
		return totalPnl;
	}
	
	private void updateAlgoOrderPrice(Long algoOrderId, String optionName, String transactionType, float avgPrice) {
		Connection conn = null;
		try {
			conn = HDataSource.getConnection();
			Statement stmt = conn.createStatement();	
			String updateSql = "UPDATE option_algo_orders set " + (transactionType.equals("BUY")?"buy_price=":"sell_price=") + avgPrice + " WHERE id="+algoOrderId +" and option_name = '" + optionName + "'";
			fileLogTelegramWriter.write(updateSql);
			stmt.executeUpdate(updateSql);
			stmt.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) {
		new OrderExecutionThreadAlgoThread(1L);
		
	}
}
