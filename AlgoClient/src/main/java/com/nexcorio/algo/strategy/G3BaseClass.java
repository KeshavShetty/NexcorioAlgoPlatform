package com.nexcorio.algo.strategy;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.core.BaseClass;
import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.util.FileLogTelegramWriter;
import com.nexcorio.algo.util.KiteUtil;
import com.nexcorio.algo.util.db.HDataSource;

public abstract class G3BaseClass extends BaseClass {
	
	private static final Logger log = LogManager.getLogger(G3BaseClass.class);
	
	protected Long napAlgoId = -1L;
	
	protected Long userId = -1L;
	
	protected int target = -1;
	protected int stoploss = -1;
	protected int trailingStoploss = -1;
	protected int exitHour = 15;
	protected int exitMinute = 15;
	
	protected boolean placeActualOrder = false;
	protected int noOfLots = 0;
	protected float maxFundAllocated = 0f;
	protected int hedgeDistance = 0;
	protected int maxAllowedNoOfOrders = 0;
	protected int lotSize = 0;
	protected int noOfOrders = 0;
	
	protected float currentProfitPerUnit = 0f;
	protected float trailingProfit = 0f;
	
	protected float requiredMargin = 0f;
	
	String ceHedgeOptionName = "";  
	String peHedgeOptionName = "";
	
	String ceStraddleOptionName = "";
	String peStraddleOptionName = "";
	
	void initializeAlgorithmParameters() {
		
		Connection conn = null;
		try {
			conn = HDataSource.getConnection();
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
		
		if (this.placeActualOrder) setLotBasedonAvailableMargin();
		else this.requiredMargin = this.mainInstrument.getStraddleMargin();
	}

	protected void initializeGenericParameters() {
		
		Connection conn = null;
		try {
			conn = HDataSource.getConnection();
			Statement stmt = conn.createStatement();
						
			String opOIFetch = "select f_user, f_main_instrument, algoname, exit_time, no_of_lots, max_fund_allocated, target, stoploss, trailing_stoploss, max_allowed_nooforders, hedge_distance,"
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
				this.hedgeDistance = rs.getInt("hedge_distance");
				
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
	
	protected float setLotBasedonAvailableMargin() {
		this.requiredMargin = this.mainInstrument.getStraddleMargin();
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
			log.info(" requiredMargin per lot="+requiredMargin +" availableMargin="+availableMargin+" maxPossibleLots="+maxPossibleLots+" maxFundAllocated="+maxFundAllocated);
		}
		
		return requiredMargin;
	}
	
	protected float setLotBasedonAvailableMarginHalfStraddle() {
		this.requiredMargin = this.mainInstrument.getHalfStraddleMargin();
		float availableMargin = getAvailableMargin(getKiteConnect(this.userId), KiteUtil.SEGMENT_EQUITY);
		
		float maxFundCanUse = this.maxFundAllocated>availableMargin?availableMargin:this.maxFundAllocated;
		
		int maxPossibleLots = (requiredMargin>0f)?((int) (maxFundCanUse/requiredMargin)):0;
		
		if (fileLogTelegramWriter!=null) {
			fileLogTelegramWriter.write("Half straddle requiredMargin per lot="+requiredMargin +" availableMargin="+availableMargin+" maxPossibleLots="+maxPossibleLots+" maxFundAllocated="+maxFundAllocated);
		} else {
			log.info("Half straddle requiredMargin per lot="+requiredMargin +" availableMargin="+availableMargin+" maxPossibleLots="+maxPossibleLots+" maxFundAllocated="+maxFundAllocated);
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
				//log.info(updateSql);
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
			prepareExit("Day target acheived");
		}
		if (this.stoploss != 0 && this.currentProfitPerUnit < this.stoploss) { 
			prepareExit("SL Hit");
		}
		if (this.trailingStoploss != 0 && this.trailingProfit < this.trailingStoploss) { 
			prepareExit("Traling Profit SL Hit");
		}
		if(manualExitEnabled()==true) {
			prepareExit(" Exiting: Manual exit triggered");
		}
	}
	
	protected long createAlgoSellOrder(String optionName, float optionPrice, int quantity) {
		
		long retId = -1;
		Connection conn = null;
		try {
			Date expiryDate = getOptionCurrentWeekExpiryDate();
			
			conn = HDataSource.getConnection();
			Statement stmt = conn.createStatement();
			
			String fetchNextSeq = "select nextval('nexcorio_option_algo_orders_id_seq') as nextId";
	    	
	    	ResultSet rs = stmt.executeQuery(fetchNextSeq);
			while (rs.next()) {
				retId = rs.getLong("nextId");
			}
			rs.close();
			
			String insertSql = "INSERT INTO nexcorio_option_algo_orders (id, f_strategy, option_name, sell_price, buy_price, place_actual_order, quantity, days_to_expiry, short_date, entry_time, exit_time)"
					+ " VALUES (" + retId +"," + this.napAlgoId + ",'" + optionName +"'," + optionPrice + "," + optionPrice +"," + this.placeActualOrder+"," + quantity +"," + getDaysBetween(getCurrentTime(), expiryDate) 
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
				fileLogTelegramWriter.write(updateSql);
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
			
			conn = HDataSource.getConnection();
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
		log.info("In Base class transactOption(optionname:"+optionname+" quantity=" + quantity+" transactionType="+transactionType+" useNormal="+useNormal);
		
		Connection conn = null;
		try {
			conn = HDataSource.getConnection();
			Statement stmt = conn.createStatement();
			
			String algoTag = "X" + this.napAlgoId;
			
			String sql2Execute = "INSERT INTO nexcorio_real_orders (id, algo_order_id, f_user, option_name, quantity, transaction_type, waitforpositionfill, algo_ag) VALUES "
					+ " (nextval('nexcorio_real_orders_id_seq')," +dbOrderId + ", " + this.userId + ",'" + optionname + "', " + quantity+ ",'" +transactionType+ "', " + waitForPositionFill +",'"+algoTag+"')"; 
			  log.info(sql2Execute);
			stmt.executeUpdate(sql2Execute);			
			stmt.close();
		} catch (Exception e) {
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
	
	public void placeRealOrder(String optionname, int quantity, String transactionType, boolean waitForPositionFill, boolean useNormal) {
		log.info("In Base class transactOption(optionname:"+optionname+" quantity=" + quantity+" transactionType="+transactionType+" useNormal="+useNormal);
		
		Connection conn = null;
		try {
			conn = HDataSource.getConnection();
			Statement stmt = conn.createStatement();
			
			String algoTag = "X" + this.napAlgoId;
			
			String sql2Execute = "INSERT INTO nexcorio_real_orders (id, f_user, option_name, quantity, transaction_type, waitforpositionfill) VALUES "
					+ " (nextval('nexcorio_real_orders_id_seq')," + this.userId + ",'" + optionname + "', " + quantity+ ",'" +transactionType+ "', " + waitForPositionFill + " )"; 
			  log.info(sql2Execute);
			stmt.executeUpdate(sql2Execute);			
			stmt.close();
		} catch (Exception e) {
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
		try {			
			conn = HDataSource.getConnection();
			Statement stmt = conn.createStatement();
			
			Date shortDateToUse = getCurrentTime();
			
			// Update if exist, else create new
			String updateSql = " UPDATE nexcorio_option_algo_orders_daily_summary set "
					+ "exit_profit=" + (profit) + ", best_profit=" + (maxProfit) + ", worst_profit=" + (worstProfit) + ", max_profit_reached_at='" + postgresLongDateFormat.format(maxProfitReachedAt) + "',"
					+ "worst_profit_reached_at='" + postgresLongDateFormat.format(maxLowestpointReachedAt) + "', maxTrailingProfit=" + maxTrailingProfit + ", noOfOrders=" + this.noOfOrders +","
					+ " last_updated_at = '" + postgresLongDateFormat.format(getCurrentTime()) +"'"
					+ " WHERE f_strategy=" + this.napAlgoId + " and short_date='" + postgresShortDateFormat.format(shortDateToUse) + "'";
					
			int recUpdated = stmt.executeUpdate(updateSql);
			
			if (recUpdated==0) {
				String insertSql = "INSERT INTO nexcorio_option_algo_orders_daily_summary (id, f_strategy, exit_profit, best_profit, worst_profit, max_profit_reached_at, worst_profit_reached_at, maxTrailingProfit, noOfOrders, short_date) "
						+ " VALUES (nextval('nexcorio_option_algo_orders_daily_summary_id_seq')," + this.napAlgoId + "," + profit + "," + maxProfit + "," + worstProfit + ",'" + postgresLongDateFormat.format(maxProfitReachedAt) + "','" + postgresLongDateFormat.format(maxLowestpointReachedAt) + "'," + maxTrailingProfit + "," + this.noOfOrders + ",'" + postgresShortDateFormat.format(shortDateToUse) + "')";
				log.info(insertSql);
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
}
