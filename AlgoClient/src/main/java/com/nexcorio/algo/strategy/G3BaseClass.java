package com.nexcorio.algo.strategy;

import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.core.BaseClass;
import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.util.ApplicationConfig;
import com.nexcorio.algo.util.FileLogTelegramWriter;
import com.nexcorio.algo.util.KiteUtil;
import com.nexcorio.algo.util.TelegramUtil;
import com.nexcorio.algo.util.db.HDataSource;
import com.zerodhatech.kiteconnect.KiteConnect;
import com.zerodhatech.kiteconnect.kitehttp.exceptions.KiteException;
import com.zerodhatech.kiteconnect.utils.Constants;
import com.zerodhatech.models.CombinedMarginData;
import com.zerodhatech.models.MarginCalculationParams;

public abstract class G3BaseClass extends BaseClass {
	
	private static final Logger log = LogManager.getLogger(G3BaseClass.class);
	
	protected Long napAlgoId = -1L;
	
	protected int target = -1;
	protected int stoploss = -1;
	protected int trailingStoploss = -1;
	protected int exitHour = 15;
	protected int exitMinute = 15;
	public float quickGainNotifTarget =-1f;
	
	protected boolean placeActualOrder = false;
	protected int noOfLots = 0;
	protected float maxFundAllocated = 0f;
	protected int hedgeDistance = 0;
	protected int optimalHedgeDistance = 0;
	protected float maxHedgeCostPerLeg = 1.5f;
	protected int maxAllowedNoOfOrders = 0;
	protected int lotSize = 0;
	protected int noOfOrders = 0;
	protected boolean nonDirectional = true;
	
	protected int noOfBatches = 1;
	protected float currentProfitPerUnit = 0f;
	protected float trailingProfit = 0f;
	
	protected float requiredMargin = 0f;
	
	String ceHedgeOptionName = "";  
	String peHedgeOptionName = "";
	
	String ceStraddleOptionName = "";
	String peStraddleOptionName = "";
	
	int ignoredOrders = 0;
	
	int daysToExpiry = -1;
	void initializeAlgorithmParameters() {
		
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
						
			String opOIFetch = "select name, data_type, value from nexcorio_options_algo_strategy_parameters where f_strategy = " + this.napAlgoId + " order by name";			  
			  
			System.out.println("opOIFetch="+opOIFetch);
			
			ResultSet rs = stmt.executeQuery(opOIFetch);
			while (rs.next()) {
				String name = rs.getString("name");
				String dataType = rs.getString("data_type");
				String value = rs.getString("value");
				System.out.println("name="+name+" dataType="+dataType+" value="+value);
				//this.algoname = this.algoname +"-" + name+value;
				Field field = this.getClass().getField(name);
				field.set(this, getFieldValue(dataType, value));
			}
			rs.close();
			stmt.close();
					
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error"+e.getMessage(),e);
		} finally {
			try {
				if (conn!=null) conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	private Object getFieldValue(String dataType, String fieldValue) {
		Object retObj = null;
		if (dataType.equals("boolean")) retObj = Boolean.parseBoolean(fieldValue);
		else if (dataType.equals("float")) retObj = Float.parseFloat(fieldValue);
		else if (dataType.equals("int")) retObj = Integer.parseInt(fieldValue);
		else if (dataType.equals("String")) retObj = fieldValue;
		else if (dataType.equals("long")) retObj = Long.parseLong(fieldValue);
		return retObj; 
	}
	
	public G3BaseClass(Long napAlgoId) {
		super();
		this.napAlgoId = napAlgoId;
	}
	
	protected void initializeParameters(String backTestDateStr) {
		
		if (backTestDateStr!=null) {
			try {
				this.backtestDate = Calendar.getInstance();
				this.backtestDate.setTime(postgresLongDateFormat.parse(backTestDateStr));
			} catch (ParseException e) {
				log.error("Error"+e.getMessage(), e);
			}
		}		
		initializeGenericParameters();
		initializeAlgorithmParameters();
		
		if (backTestDateStr!=null) {
			this.algoname = this.algoname + "-Test";
			this.placeActualOrder = false;
		}
		fileLogTelegramWriter = new FileLogTelegramWriter(this.mainInstrument.getShortName(), this.algoname, this.backtestDate);
		
		if (this.placeActualOrder) {
			this.optimalHedgeDistance = getOptimalHedgeDistance(this.hedgeDistance, this.maxHedgeCostPerLeg);
			
			this.requiredMargin = getStraddleMargin(this.nonDirectional);
			setLotBasedonAvailableMargin();
			
		} else {
			this.requiredMargin = this.mainInstrument.getStraddleMargin();
		}
		
	}

	protected void initializeGenericParameters() {
		
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
						
			String opOIFetch = "select f_user, f_main_instrument, algoname, exit_time, no_of_lots, max_fund_allocated, target, stoploss, trailing_stoploss, max_allowed_nooforders, hedge_distance, max_hedge_cost_per_leg, non_directional, "
					+ " order_enabled_monday, order_enabled_tuesday, order_enabled_wednesday, order_enabled_thursday, order_enabled_friday"
					+ " from nexcorio_options_algo_strategy where id = " + this.napAlgoId;			  
			  
			System.out.println("opOIFetch="+opOIFetch);
			
			ResultSet rs = stmt.executeQuery(opOIFetch);
			while (rs.next()) {
				this.userId =  rs.getLong("f_user");
				this.mainInstrument = getMainInstrumentDtoById(rs.getLong("f_main_instrument"));
				this.algoname =  "X"+this.napAlgoId + "-" + this.mainInstrument.getShortName() + "-" + rs.getString("algoname");
				this.noOfLots = rs.getInt("no_of_lots");
				this.maxFundAllocated = rs.getFloat("max_fund_allocated");
				
				this.maxHedgeCostPerLeg = rs.getFloat("max_hedge_cost_per_leg");
				
				this.hedgeDistance = rs.getInt("hedge_distance");
				
				this.optimalHedgeDistance = this.hedgeDistance;
				
				this.nonDirectional = rs.getBoolean("non_directional");
				this.target = rs.getInt("target");
				this.stoploss = rs.getInt("stoploss");
				this.trailingStoploss = rs.getInt("trailing_stoploss");
				this.maxAllowedNoOfOrders = rs.getInt("max_allowed_nooforders");
				if (this.maxAllowedNoOfOrders<=0) this.maxAllowedNoOfOrders = 1000;
				this.placeActualOrder = rs.getBoolean(getDayOfWeekField(this.backtestDate));
				//this.placeActualOrder = false;
				setExitTime(rs.getString("exit_time")); 
			}
			rs.close();
			stmt.close();
			
			this.lotSize = this.mainInstrument.getLotSize();  
			
		} catch (Exception e) {
			log.error("Error"+e.getMessage(),e);
			e.printStackTrace();
		} finally {
			try {
				if (conn!=null) conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	protected void setLotBasedonAvailableMargin() {
		
		float availableMargin = getAvailableMargin(getKiteConnect(this.userId), KiteUtil.SEGMENT_EQUITY);
		
		float maxFundCanUse = this.maxFundAllocated>availableMargin?availableMargin:this.maxFundAllocated;
		
		int maxPossibleLots = (requiredMargin>0f)?((int) (maxFundCanUse/requiredMargin)):0;
		
		if (this.noOfLots > maxPossibleLots) {
			this.noOfLots = maxPossibleLots;
		}
		if (this.noOfLots==0) {
			this.placeActualOrder=false;
			this.noOfLots=1;
		}
		
		if (fileLogTelegramWriter!=null) {
			fileLogTelegramWriter.write(" requiredMargin per lot="+requiredMargin +" availableMargin="+availableMargin+" maxPossibleLots="+maxPossibleLots+" maxFundAllocated="+maxFundAllocated+" finally lot set="+this.noOfLots);
		} else {
			log.debug(" requiredMargin per lot="+requiredMargin +" availableMargin="+availableMargin+" maxPossibleLots="+maxPossibleLots+" maxFundAllocated="+maxFundAllocated);
		}
	}
	
	protected float setLotBasedonAvailableMarginHalfStraddle() {
		this.requiredMargin = this.mainInstrument.getHalfStraddleMargin();
		float availableMargin = getAvailableMargin(getKiteConnect(this.userId), KiteUtil.SEGMENT_EQUITY);
		
		if (availableMargin == 0) availableMargin = maxFundAllocated; // In case zeordha api fails to fetch
		
		float maxFundCanUse = this.maxFundAllocated>availableMargin?availableMargin:this.maxFundAllocated;
		
		int maxPossibleLots = (requiredMargin>0f)?((int) (maxFundCanUse/requiredMargin)):0;
		
		if (fileLogTelegramWriter!=null) {
			fileLogTelegramWriter.write("Half straddle requiredMargin per lot="+requiredMargin +" availableMargin="+availableMargin+" maxPossibleLots="+maxPossibleLots+" maxFundAllocated="+maxFundAllocated);
		} else {
			log.debug("Half straddle requiredMargin per lot="+requiredMargin +" availableMargin="+availableMargin+" maxPossibleLots="+maxPossibleLots+" maxFundAllocated="+maxFundAllocated);
		}
		
		if (this.noOfLots > maxPossibleLots) this.noOfLots = maxPossibleLots;
		
		if (this.noOfLots==0) {
			this.placeActualOrder=false;
			this.noOfLots=1;
		}
		return requiredMargin;
	}
	
	protected String getDayOfWeekField(Calendar curTestTime) {
		String retStr = "order_enabled_monday";
		Calendar calInst = Calendar.getInstance();
		if (curTestTime!=null) calInst = curTestTime;
		if (calInst.get(Calendar.DAY_OF_WEEK)==Calendar.MONDAY) retStr = "order_enabled_monday";
		else if (calInst.get(Calendar.DAY_OF_WEEK)==Calendar.TUESDAY) retStr = "order_enabled_tuesday";
		else if (calInst.get(Calendar.DAY_OF_WEEK)==Calendar.WEDNESDAY) retStr = "order_enabled_wednesday";
		else if (calInst.get(Calendar.DAY_OF_WEEK)==Calendar.THURSDAY) retStr = "order_enabled_thursday";
		else if (calInst.get(Calendar.DAY_OF_WEEK)==Calendar.FRIDAY) retStr = "order_enabled_friday"; 
		System.out.println("In getDayOfWeekField retStr="+retStr);
		return retStr;
	}
	
	protected void setExitTime(String exitTimeFromDB) {
		if (exitTimeFromDB!=null) {
			String[] exitTimeParts = exitTimeFromDB.split(":");
			this.exitHour = Integer.parseInt(exitTimeParts[0]);
			this.exitMinute = Integer.parseInt(exitTimeParts[1]);
		}
	}
	
	protected void updateAlgoStatus(String status) {
		if (backtestDate == null) { // Only for real time 			
			Connection conn = null;
			try {
				conn = HDataSource.getConnection();
				Statement stmt = conn.createStatement();
				// Update running status
				String updateStatusSql = "update nexcorio_options_algo_strategy set status = '" + status + "', manual_exit_enabled=FALSE where id = " + this.napAlgoId;
				System.out.println(updateStatusSql);
				stmt.execute(updateStatusSql);
				stmt.close();
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
		}
	}
	
	protected void updateCurrentOrderStatus(String optionName, long orderDbId, String status) {
		if (!optionName.equals("") ) {
			Connection conn = null;
			try {			
				conn = HDataSource.getConnection();
				Statement stmt = conn.createStatement();
											
				String updateSql = "UPDATE nexcorio_option_algo_orders set status='" + status+"', exit_time='" + postgresLongDateFormat.format(getCurrentTime()) +"' WHERE id=" + orderDbId ;
				//log.debug(updateSql);
				stmt.execute(updateSql);
				
				stmt.close();
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
		}
	}
	
	protected boolean manualExitEnabled() {
		boolean retVal = false;
		Connection conn = null;
		try {
			conn = HDataSource.getConnection();
			Statement stmt = conn.createStatement();
			
			String fetchSql = "select manual_exit_enabled from nexcorio_options_algo_strategy where id =" + this.napAlgoId;
			ResultSet rs = stmt.executeQuery(fetchSql);
			while (rs.next()) {
				retVal = rs.getBoolean("manual_exit_enabled");
			}
			rs.close();
			if (retVal==true) { // reset back the flag
				stmt.executeUpdate("update nexcorio_options_algo_strategy set manual_exit_enabled=false where id =" + this.napAlgoId);
			}
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
	
	protected void checkExitSignals() {
		if (timeout(this.exitHour, this.exitMinute, 0)) {
			prepareExit("Timeout");
		}
		if (this.target != 0 && this.currentProfitPerUnit > this.target) { 
			prepareExit("Target acheived");
		}
		if (this.stoploss != 0 && this.currentProfitPerUnit < this.stoploss) { 
			prepareExit("SL Hit");
		}
		if (this.trailingStoploss != 0 && this.trailingProfit < this.trailingStoploss) { 
			prepareExit("Traling SL Hit");
		}
		if(manualExitEnabled()==true) {
			prepareExit(" Manual exit triggered");
		}
		if (this.placeActualOrder==true && quickGainNotifTarget > 0f && this.currentProfitPerUnit > quickGainNotifTarget) {
			TelegramUtil.postTelegramMessage("@NseFnOAutoPicks", ApplicationConfig.getProperty("zerodha.user.id") + "-X" + this.napAlgoId + ": " 
					+ " Quick and early target "+CURRENCY_FORMAT.format(quickGainNotifTarget) +" reached.");
			quickGainNotifTarget = -1f; // Don't send notification again
		}
	}
	
	protected long createAlgoSellOrder(String optionName, float optionPrice, int quantity) {
		fileLogTelegramWriter.write( "Create Algo order SELL " + optionName + "@" +optionPrice + " qty=" + quantity);
		long retId = -1;
		Connection conn = null;
		try {
			Date expiryDate = getOptionCurrentWeekExpiryDate();
			
			if (this.daysToExpiry < 0) {
				this.daysToExpiry = getDaysBetween(getCurrentTime(), expiryDate);
			}
			conn = HDataSource.getConnection();
			Statement stmt = conn.createStatement();
			
			String fetchNextSeq = "select nextval('nexcorio_option_algo_orders_id_seq') as nextId";
	    	
	    	ResultSet rs = stmt.executeQuery(fetchNextSeq);
			while (rs.next()) {
				retId = rs.getLong("nextId");
			}
			rs.close();
			
			String insertSql = "INSERT INTO nexcorio_option_algo_orders (id, f_strategy, option_name, sell_price, buy_price, place_actual_order, quantity, days_to_expiry, short_date, entry_time, exit_time)"
					+ " VALUES (" + retId +"," + this.napAlgoId + ",'" + optionName +"'," + optionPrice + "," + optionPrice +"," + this.placeActualOrder+"," + quantity +"," +  this.daysToExpiry 
					+ ",'" + postgresShortDateFormat.format(getCurrentTime())+ "'"
					+ ",'" + postgresLongDateFormat.format(getCurrentTime())+ "'"
					+ ",'" + postgresLongDateFormat.format(getCurrentTime())+ "'"
					+ ")";
			fileLogTelegramWriter.write(insertSql);
			stmt.execute(insertSql);
			
			stmt.close();
			this.noOfOrders++;
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
		return retId;
	}
	
	protected long createAlgoBuyOrder(String optionName, float optionPrice, int quantity) {
		fileLogTelegramWriter.write( "Create Algo order BUY " + optionName + "@" +optionPrice + " qty=" + quantity);
		long retId = -1;
		Connection conn = null;
		try {
			Date expiryDate = getOptionCurrentWeekExpiryDate();
			if (this.daysToExpiry < 0) {
				this.daysToExpiry = getDaysBetween(getCurrentTime(), expiryDate);
			}
			conn = HDataSource.getConnection();
			Statement stmt = conn.createStatement();
			
			String fetchNextSeq = "select nextval('nexcorio_option_algo_orders_id_seq') as nextId";
	    	
	    	ResultSet rs = stmt.executeQuery(fetchNextSeq);
			while (rs.next()) {
				retId = rs.getLong("nextId");
			}
			rs.close();
			
			String insertSql = "INSERT INTO nexcorio_option_algo_orders (id, f_strategy, option_name, sell_price, buy_price, place_actual_order, quantity, days_to_expiry, short_date, entry_time, exit_time)"
					+ " VALUES (" + retId +"," + this.napAlgoId + ",'" + optionName +"'," + optionPrice + "," + optionPrice +"," + this.placeActualOrder+"," + quantity +"," + this.daysToExpiry 
					+ ",'" + postgresShortDateFormat.format(getCurrentTime())+ "'"
					+ ",'" + postgresLongDateFormat.format(getCurrentTime())+ "'"
					+ ",'" + postgresLongDateFormat.format(getCurrentTime())+ "'"
					+ ")";
			fileLogTelegramWriter.write(insertSql);
			stmt.execute(insertSql);
			
			stmt.close();
			this.noOfOrders++;
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
		return retId;
	}
	
	protected void updateCurrentOrderBuyPrice(String optionName, long orderDbId, float optionPrice) {
		if (!optionName.equals("") && optionPrice>0f) {
			Connection conn = null;
			try {			
				conn = HDataSource.getConnection();
				Statement stmt = conn.createStatement();
							
				String updateSql = "UPDATE nexcorio_option_algo_orders set buy_price=" + optionPrice+", exit_time='" + postgresLongDateFormat.format(getCurrentTime()) +"' WHERE id=" + orderDbId ;
				//fileLogTelegramWriter.write(updateSql);
				stmt.execute(updateSql);
				
				stmt.close();
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
		}
	}
	
	protected void updateCurrentOrderSellPrice(String optionName, long orderDbId, float optionPrice) {
		if (!optionName.equals("") && optionPrice>0f) {
			Connection conn = null;
			try {			
				conn = HDataSource.getConnection();
				Statement stmt = conn.createStatement();
							
				String updateSql = "UPDATE nexcorio_option_algo_orders set sell_price=" + optionPrice+", exit_time='" + postgresLongDateFormat.format(getCurrentTime()) +"' WHERE id=" + orderDbId ;
				//fileLogTelegramWriter.write(updateSql);
				stmt.execute(updateSql);
				
				stmt.close();
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
		}
	}
	
	public float getProfitFromDB() {
		
		float retVal = 0f;
		Connection conn = null;
		try {
			
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			String fetchNextSeq = "select sum(sell_price-buy_price) as profitPerLot from nexcorio_option_algo_orders where short_date = '" + postgresShortDateFormat.format(getCurrentTime())+ "' and f_strategy="+this.napAlgoId;
			fileLogTelegramWriter.write("fetchNextSeq="+fetchNextSeq);
			ResultSet rs = stmt.executeQuery(fetchNextSeq);
	    	while (rs.next()) {
	    		retVal  = rs.getFloat("profitPerLot");
			}
			rs.close();
			stmt.close();
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error"+e.getMessage(),e);
		} finally {
			try {
				if (conn!=null) conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return retVal;
	}
	protected float getPercentDiff(float firstValue, float secondValue) {
		float retval = 0f;
		float first = Math.abs(firstValue);
		float second = Math.abs(secondValue);
		retval = first>second?(first-second)/first:(second-first)/second;
		retval = retval*100f;
		return retval;
	}
	
	protected void print(OptionGreek optionGreekDto) {
		if (optionGreekDto!=null) {
			fileLogTelegramWriter.write( "[" + optionGreekDto.getTradingSymbol()+"@" + optionGreekDto.getLtp() + "] IV=" + optionGreekDto.getIv()+" Delta="+optionGreekDto.getDelta()+" Gamma="+optionGreekDto.getGamma()+" Vega="+optionGreekDto.getVega()+" Theta="+optionGreekDto.getTheta());
		}
	}
	
	protected void print(OptionGreek firstOptionGreekDto, OptionGreek secondOptionGreekDto) {
		
		if (firstOptionGreekDto!=null) print(firstOptionGreekDto);
		if (secondOptionGreekDto!=null) print(secondOptionGreekDto);

		if (firstOptionGreekDto!=null && secondOptionGreekDto!=null) {
			fileLogTelegramWriter.write( " Percent diff:"
					+ " Delta->" + getPercentDiff(firstOptionGreekDto.getDelta(), secondOptionGreekDto.getDelta())
					+ " Gamma->" + getPercentDiff(firstOptionGreekDto.getGamma(), secondOptionGreekDto.getGamma())
					+ " Theta->" + getPercentDiff(firstOptionGreekDto.getTheta(), secondOptionGreekDto.getTheta())
					+ " IV->" + getPercentDiff(firstOptionGreekDto.getIv(), secondOptionGreekDto.getIv())
					+ " Vega->" + getPercentDiff(firstOptionGreekDto.getVega(), secondOptionGreekDto.getVega())
					+ " Price->" + getPercentDiff(firstOptionGreekDto.getLtp(), secondOptionGreekDto.getLtp())
					+ " DbG->" + getPercentDiff(firstOptionGreekDto.getDelta()/firstOptionGreekDto.getGamma(), secondOptionGreekDto.getDelta()/secondOptionGreekDto.getGamma())
					);
		}
	}
	
	public void placeRealOrder(Long dbOrderId, String optionname, int quantity, String transactionType, boolean waitForPositionFill, boolean useNormal) {
		fileLogTelegramWriter.write( "In Base class transactOption(optionname:"+optionname+" quantity=" + quantity+" transactionType="+transactionType+" useNormal="+useNormal);
		
		Connection conn = null;
		try {
			conn = HDataSource.getConnection();
			Statement stmt = conn.createStatement();
			
			String algoTag = "X" + this.napAlgoId;
			
			String derivativeExchange = "NFO";
			if (this.mainInstrument.getExchange().equals("BSE")) derivativeExchange = "BFO";
			
			String sql2Execute = "INSERT INTO nexcorio_real_orders (id, algo_order_id, f_user, option_name, quantity, transaction_type, waitforpositionfill, algo_tag, exchange) VALUES "
					+ " (nextval('nexcorio_real_orders_id_seq')," +dbOrderId + ", " + this.userId + ",'" + optionname + "', " + quantity+ ",'" +transactionType+ "', " + waitForPositionFill +",'"+algoTag + "','" + derivativeExchange+"')"; 
			  log.debug(sql2Execute);
			stmt.executeUpdate(sql2Execute);			
			stmt.close();
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error"+e.getMessage(),e);
		} finally {
			try {
				if (conn!=null) conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public void placeRealOrder(String optionname, int quantity, String transactionType, boolean waitForPositionFill, boolean useNormal) {
		fileLogTelegramWriter.write( "In Base class transactOption(optionname:"+optionname+" quantity=" + quantity+" transactionType="+transactionType+" useNormal="+useNormal);
		
		Connection conn = null;
		try {
			conn = HDataSource.getConnection();
			Statement stmt = conn.createStatement();
			
			String algoTag = "X" + this.napAlgoId;
			
			String derivativeExchange = "NFO";
			if (this.mainInstrument.getExchange().equals("BSE")) derivativeExchange = "BFO";
			
			String sql2Execute = "INSERT INTO nexcorio_real_orders (id, f_user, option_name, quantity, transaction_type, waitforpositionfill, algo_tag, exchange) VALUES "
					+ " (nextval('nexcorio_real_orders_id_seq')," + this.userId + ",'" + optionname + "', " + quantity+ ",'" +transactionType+ "', " + waitForPositionFill +",'" + algoTag+"','" + derivativeExchange+ "' )"; 
			  log.debug(sql2Execute);
			stmt.executeUpdate(sql2Execute);			
			stmt.close();
		} catch (Exception e) {
			log.error("Error"+e.getMessage(),e);
			e.printStackTrace();
		} finally {
			try {
				if (conn!=null) conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	protected void saveAlgoDailySummary(float profit, float maxProfit, Date maxProfitReachedAt, float worstProfit, Date maxLowestpointReachedAt, float maxTrailingProfit) {
		Connection conn = null;
		if (profit == 0 && maxProfit == 0 && worstProfit == 0) {
			fileLogTelegramWriter.write("Blank day, skipping save");
		}  else {
			try {			
				conn = HDataSource.getConnection();
				Statement stmt = conn.createStatement();
				
				Date shortDateToUse = getCurrentTime();
				
				// Update if exist, else create new
				String updateSql = " UPDATE nexcorio_option_algo_orders_daily_summary set "
						+ "exit_profit=" + (profit) + ", best_profit=" + (maxProfit) + ", worst_profit=" + (worstProfit) + ", max_profit_reached_at='" + postgresLongDateFormat.format(maxProfitReachedAt) + "',"
						+ "worst_profit_reached_at='" + postgresLongDateFormat.format(maxLowestpointReachedAt) + "', maxTrailingProfit=" + maxTrailingProfit + ", noOfOrders=" + this.noOfOrders +","
						+ " last_updated_at = '" + postgresLongDateFormat.format(getCurrentTime()) +"'"
						+ (this.exitThread==true?(", exit_reason='" + this.exitReason.trim()+ "'"):"")
						+ ", dte=" + this.daysToExpiry
						+ " WHERE f_strategy=" + this.napAlgoId + " and short_date='" + postgresShortDateFormat.format(shortDateToUse) + "'";
						
				int recUpdated = stmt.executeUpdate(updateSql);
				
				if (recUpdated==0) {
					String insertSql = "INSERT INTO nexcorio_option_algo_orders_daily_summary (id, f_strategy, exit_profit, best_profit, worst_profit, max_profit_reached_at, worst_profit_reached_at, maxTrailingProfit, noOfOrders, short_date, dte) "
							+ " VALUES (nextval('nexcorio_option_algo_orders_daily_summary_id_seq')," + this.napAlgoId + "," + profit + "," + maxProfit + "," + worstProfit + ",'" + postgresLongDateFormat.format(maxProfitReachedAt) + "','" + postgresLongDateFormat.format(maxLowestpointReachedAt) + "'," + maxTrailingProfit + "," + this.noOfOrders + ",'" + postgresShortDateFormat.format(shortDateToUse) + "'," + daysToExpiry + ")";
					log.debug(insertSql);
					stmt.execute(insertSql);
				}
				
				stmt.close();
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
			if (this.placeActualOrder==true && this.exitThread == true) {
				com.ibm.icu.text.NumberFormat format = com.ibm.icu.text.NumberFormat.getCurrencyInstance(new Locale("en", "in"));
			    
				TelegramUtil.postTelegramMessage("@NseFnOAutoPicks", ApplicationConfig.getProperty("zerodha.user.id") + "-X" + this.napAlgoId + ": " 
						+ " Exit with PnL = "+CURRENCY_FORMAT.format(profit) +" ( "+ format.format((noOfLots*lotSize*profit)) + "/- ), NoOfOrders=" + this.noOfOrders + ", Exit reason:-" + this.exitReason + ")");
			}
		}
	}
	
	protected void saveObject(Object objectToSave) {
		System.out.println("Writing to file");
		try {
			FileOutputStream file = new FileOutputStream(ApplicationConfig.getProperty("ObjectFileStoreLocation") + File.separator + this.napAlgoId + postgresShortDateFormat.format(getCurrentTime())+ ".obj");
	        ObjectOutput out = new ObjectOutputStream(file);
	            
	        // Method for serialization of object
	        out.writeObject(objectToSave);
	            
	        out.close();
	        file.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		System.out.println("Writing to file - Complete");
	}
	
	protected void exitStraddle(Long ceDbId, Long peDbId) {
		try {
			if (!ceStraddleOptionName.equals("")) placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
			if (!peStraddleOptionName.equals("")) placeRealOrder( peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
			if (!ceHedgeOptionName.equals("")) placeRealOrder( ceHedgeOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
			if (!peHedgeOptionName.equals("")) placeRealOrder( peHedgeOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
		} catch (Exception e) {			
			log.error("Error"+e.getMessage(), e);
		}
	}
	

	protected float getStraddleMargin(boolean isFullStraddle) {
		
		float retVal = 0f;
		
		KiteConnect kiteconnect = getKiteConnect(this.userId);
		String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised( 0.5f, this.optimalHedgeDistance);
		
		float requiredMargin = getMarginRequiredForIronCondorFly(kiteconnect, entryStraddleOptionNames[0], entryStraddleOptionNames[1], entryStraddleOptionNames[2], entryStraddleOptionNames[3], this.lotSize, 60000f);
		fileLogTelegramWriter.write("In getStraddleMargin fullrequiredMargin " + requiredMargin);
		
		float halfStraddleRequiredMargin = getMarginRequiredForIronCondorFly(kiteconnect, entryStraddleOptionNames[0], null, entryStraddleOptionNames[2], null, lotSize, 45000f);
		fileLogTelegramWriter.write("In getStraddleMargin halfStraddleRequiredMargin " + halfStraddleRequiredMargin);
		
		if (isFullStraddle) {
			retVal = requiredMargin;
		} else {
			retVal = halfStraddleRequiredMargin;
		}
		
		return retVal;
	}
	
	protected float getMarginRequiredForIronCondorFly(KiteConnect kiteconnect, String ceOptionName, String peOptionName, String ceHedgeOptionName, String peHedgeOptionName, int qty, float defaultReturnInCaseofError) {
		float retVal = defaultReturnInCaseofError;
		try {
	    	// Check margin required
    		List<MarginCalculationParams> params = new ArrayList<MarginCalculationParams>();
    		
    		String exchangeToUse = "NFO";
			if (this.mainInstrument.getExchange().equals("BSE")) exchangeToUse = "BFO";
			
    		if (ceHedgeOptionName!=null && !ceHedgeOptionName.equals("")) {
	    		MarginCalculationParams ceHedgeItemMarginParam = new MarginCalculationParams();
	    		ceHedgeItemMarginParam.exchange = exchangeToUse; 
	    		ceHedgeItemMarginParam.variety = Constants.VARIETY_REGULAR;
	    		ceHedgeItemMarginParam.orderType = Constants.ORDER_TYPE_MARKET;
	    		ceHedgeItemMarginParam.product = Constants.PRODUCT_MIS;
	    		ceHedgeItemMarginParam.quantity = qty;
	    		ceHedgeItemMarginParam.tradingSymbol = ceHedgeOptionName;			
	    		ceHedgeItemMarginParam.transactionType = "BUY";
				params.add(ceHedgeItemMarginParam);
    		}
    		if (peHedgeOptionName!=null && !peHedgeOptionName.equals("")) {
				MarginCalculationParams peHedgeItemMarginParam = new MarginCalculationParams();
	    		peHedgeItemMarginParam.exchange = exchangeToUse; 
	    		peHedgeItemMarginParam.variety = Constants.VARIETY_REGULAR;
	    		peHedgeItemMarginParam.orderType = Constants.ORDER_TYPE_MARKET;
	    		peHedgeItemMarginParam.product = Constants.PRODUCT_MIS;
	    		peHedgeItemMarginParam.quantity = qty;
	    		peHedgeItemMarginParam.tradingSymbol = peHedgeOptionName;			
	    		peHedgeItemMarginParam.transactionType = "BUY";
				params.add(peHedgeItemMarginParam);
    		}
    		if (ceOptionName!=null && !ceOptionName.equals("")) {    		
				MarginCalculationParams ceItemMarginParam = new MarginCalculationParams();
				ceItemMarginParam.exchange = exchangeToUse; 
				ceItemMarginParam.variety = Constants.VARIETY_REGULAR;
				ceItemMarginParam.orderType = Constants.ORDER_TYPE_MARKET;
				ceItemMarginParam.product = Constants.PRODUCT_MIS;
				ceItemMarginParam.quantity = qty;
				ceItemMarginParam.tradingSymbol = ceOptionName;			
				ceItemMarginParam.transactionType = "SELL";
				params.add(ceItemMarginParam);
    		}
    		if (peOptionName!=null && !peOptionName.equals("")) {
				MarginCalculationParams peItemMarginParam = new MarginCalculationParams();
				peItemMarginParam.exchange = exchangeToUse; 
				peItemMarginParam.variety = Constants.VARIETY_REGULAR;
				peItemMarginParam.orderType = Constants.ORDER_TYPE_MARKET;
				peItemMarginParam.product = Constants.PRODUCT_MIS;
				peItemMarginParam.quantity = qty;
				peItemMarginParam.tradingSymbol = peOptionName;			
				peItemMarginParam.transactionType = "SELL";
				params.add(peItemMarginParam);
    		}
    		
    		CombinedMarginData marginData = kiteconnect.getCombinedMarginCalculation(params, true, false);
    		fileLogTelegramWriter.write("In checkDailyMarginUsed -> initialMargin " + marginData.initialMargin.total+" finalMargin "+marginData.finalMargin.total);
			//retVal = ((float) marginData.initialMargin.total + (float) marginData.finalMargin.total)/2f;			
    		retVal = (float) marginData.finalMargin.total;
		} catch (Exception | KiteException e) {			
			e.printStackTrace();
			log.error("Error in checkDailyMarginUsed"+e.getMessage(), e);
		}
		return retVal;
	}
	
	protected float getStradlePremium() {
		float retVal =0f;
		
		Connection conn = null;
		try {
			conn = HDataSource.getConnection();
			Statement stmt = conn.createStatement();
			
			String fetchSql = "SELECT celtp+peltp as premium, adjustedCEATMLtp+adjustedPEATMLtp as adjustedPremium FROM nexcorio_option_atm_movement_data WHERE record_time <= '" + postgresLongDateFormat.format(getCurrentTime()) + "'"
					+ " AND f_main_instrument=" + this.mainInstrument.getId() 
					+ " ORDER BY record_time DESC LIMIT 1";
			
			ResultSet rs = stmt.executeQuery(fetchSql);
			while(rs.next()) {
				//retVal = rs.getFloat("adjustedPremium"); // We found adjusted atm ltp has some erretic numbers, so switch back to use premium
				if (retVal<5f) retVal = rs.getFloat("premium");
			}
			stmt.close();
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
	
	protected float getIdealPremiumBasedOnPreviousStradlePremium() {
		float retVal =0f;
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			
			Calendar cal = Calendar.getInstance();
			cal.setTime(getCurrentTime());
			
			
			int noOfDaysPassed = 0;
			float premium = 0f;
			do {
				cal.set(Calendar.HOUR_OF_DAY, 15);
				cal.set(Calendar.MINUTE, 20);
				cal.add(Calendar.DATE, -1);
				noOfDaysPassed++;
				
				String fetchSql = "SELECT celtp+peltp as premium, adjustedCEATMLtp+adjustedPEATMLtp as adjustedPremium FROM nexcorio_option_atm_movement_data"
						+ " WHERE record_time <= '" + postgresLongDateFormat.format(cal.getTime()) + "'"
						//+ " AND record_time > ('" + postgresLongDateFormat.format(getCurrentTime()) + "' - '1 miniute'::interval)"
						+ " AND record_time > (DATE_ADD('" + postgresLongDateFormat.format(cal.getTime()) + "',INTERVAL '-1 minute')) "
						+ " AND f_main_instrument=" + this.mainInstrument.getId() 
						+ " ORDER BY record_time DESC LIMIT 1";
				fileLogTelegramWriter.write(fetchSql);
				
				ResultSet rs = stmt.executeQuery(fetchSql);
				while (rs.next()) {
					//premium = rs.getFloat("adjustedPremium");
					if (premium<5f) premium = rs.getFloat("premium");
				}
				fileLogTelegramWriter.write(" premium found="+premium);
				
				rs.close();
			} while(premium==0f);
			
			retVal = premium - noOfDaysPassed*12f;
			
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
		return retVal;
	}
	
	protected int getTimeDiffMinute(Date date1, Date date2) { 
		int retVal = 0;
		long mili = date1.getTime() - date2.getTime();
		retVal =  (int) TimeUnit.MILLISECONDS.toMinutes(mili);
		fileLogTelegramWriter.write("date1="+date1+" date2="+date2+" retVal="+retVal);
		return retVal;
	}
	
	protected float setCoeveredMarginRequired(Map<String, Integer> orderPositions, float defaultReturnInCaseofError) {
		float retVal = defaultReturnInCaseofError;
		try {
	    	// Check margin required
    		List<MarginCalculationParams> params = new ArrayList<MarginCalculationParams>();
    		
    		String exchangeToUse = "NFO";
			if (this.mainInstrument.getExchange().equals("BSE")) exchangeToUse = "BFO";
			
			Iterator<String> iter = orderPositions.keySet().iterator();
			while(iter.hasNext()) {
				String nextKey = iter.next();
				int qty = orderPositions.get(nextKey);
				MarginCalculationParams aMarginParam = new MarginCalculationParams();
	    		aMarginParam.exchange = exchangeToUse; 
	    		aMarginParam.variety = Constants.VARIETY_REGULAR;
	    		aMarginParam.orderType = Constants.ORDER_TYPE_MARKET;
	    		aMarginParam.product = Constants.PRODUCT_MIS;
	    		aMarginParam.quantity = Math.abs(qty);
	    		aMarginParam.tradingSymbol = nextKey;			
	    		aMarginParam.transactionType = qty>0?"BUY":"SELL";
				params.add(aMarginParam);
				fileLogTelegramWriter.write(nextKey + " " + qty + " " + aMarginParam.transactionType);
			}
			
    		KiteConnect kiteconnect = getKiteConnect(this.userId);
    		CombinedMarginData marginData = kiteconnect.getCombinedMarginCalculation(params, true, false);
    		fileLogTelegramWriter.write("In MarginRequired for 1 batch -> initialMargin " + marginData.initialMargin.total+" finalMargin "+marginData.finalMargin.total);
			
			this.requiredMargin = (float) marginData.finalMargin.total; //(float) ((marginData.initialMargin.total + marginData.finalMargin.total)/2f);
			float availableMargin = getAvailableMargin(getKiteConnect(this.userId), KiteUtil.SEGMENT_EQUITY);
			
			fileLogTelegramWriter.write("requiredMargin based on Avg = "+requiredMargin+" actual availableMargin=" +availableMargin);
			availableMargin = availableMargin - 150000f; // Reserve 1.5lakh for adjustments
			fileLogTelegramWriter.write("availableMargin after reserve=" +availableMargin);
			
			if (availableMargin == 0) availableMargin = maxFundAllocated; // In case zeordha api fails to fetch
			
			float maxFundCanUse = this.maxFundAllocated>availableMargin?availableMargin:this.maxFundAllocated;
			
			int maxPossibleLots = (requiredMargin>0f)?((int) (maxFundCanUse/requiredMargin)):0;
			
			fileLogTelegramWriter.write("maxFundCanUse"+maxFundCanUse+" maxPossibleLots=" +maxPossibleLots+" maxFundAllocated="+maxFundAllocated+" initial this.noOfLots ="+this.noOfLots );
			
			if (this.noOfLots > maxPossibleLots) {
				this.noOfLots = maxPossibleLots;
			}
			
			if (this.noOfLots==0) {
				this.placeActualOrder=false;
				this.noOfLots=1;
			}
			this.noOfBatches = this.noOfLots;
			retVal = requiredMargin;
			fileLogTelegramWriter.write("Final this.noOfLots ="+this.noOfLots );
		} catch (Exception | KiteException e) {			
			e.printStackTrace();
			log.error("Error in checkDailyMarginUsed"+e.getMessage(), e);
		}
		return retVal;
	}
	
	public void updateOrderPrice(Long fromOrderId, Long toOrderId, String transactionType) {
		Connection conn = null;
		try {
			
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			float buyPrice = 0f;
			float sellPrice = 0f;
			String fetchNextSeq = "select buy_price, sell_price from nexcorio_option_algo_orders where id = " + fromOrderId;
			
			ResultSet rs = stmt.executeQuery(fetchNextSeq);
	    	while (rs.next()) {
	    		buyPrice = rs.getFloat("buy_price");
	    		sellPrice = rs.getFloat("sell_price");
	    	}
	    	rs.close();
			
	    	fileLogTelegramWriter.write("fetchNextSeq="+fetchNextSeq + " buyPrice="+buyPrice+" sellPrice="+sellPrice);
	    	
			String filedToUpdate = transactionType.equals("BUY")?"buy_price":"sell_price";
			float priceToUse = transactionType.equals("BUY")?buyPrice:sellPrice;
			
			if (priceToUse>0f) {
				String updateSql = "UPDATE nexcorio_option_algo_orders set " + filedToUpdate + "=" + priceToUse + " WHERE id="+toOrderId ;
				fileLogTelegramWriter.write("updateSql="+updateSql);
				stmt.executeUpdate(updateSql);
			}
			stmt.close();
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error"+e.getMessage(),e);
		} finally {
			try {
				if (conn!=null) conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	public int getTimeDiff(Date date1, Date date2) {
		int retVal = -1;
		
		 long diffInMillies = Math.abs(date2.getTime() - date1.getTime());
	        
	        // Approach A: Convert using TimeUnit utility (Cleaner)
		 retVal = (int) TimeUnit.MILLISECONDS.toMinutes(diffInMillies);
	        
	        
		return retVal;
	}
}

class SortbyOI implements Comparator<OptionGreek> {
    public int compare(OptionGreek a, OptionGreek b) { 
    	if (a.getOi() > b.getOi()) return -1;
    	else if (a.getOi() < b.getOi()) return 1;
    	else return 0;
    } 
}

class SortbyOiDesc implements Comparator<OptionGreek> { 
    // Comparator 
    public int compare(OptionGreek a, OptionGreek b) 
    { 
    	if (a.getOi() > b.getOi()) return -1;
    	else if (a.getOi() < b.getOi()) return 1;
    	else return 0;
    } 
}

class SortbyIV implements Comparator<OptionGreek> { 
    // Comparator 
    public int compare(OptionGreek a, OptionGreek b) 
    { 
    	if (a.getIv() < b.getIv()) return -1;
    	else if (a.getIv() > b.getIv()) return 1;
    	else return 0;
    } 
}