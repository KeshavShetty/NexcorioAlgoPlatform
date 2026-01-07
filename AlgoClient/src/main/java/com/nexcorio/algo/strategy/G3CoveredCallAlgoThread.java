package com.nexcorio.algo.strategy;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ibm.icu.util.Calendar;
import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.util.KiteUtil;
import com.nexcorio.algo.util.db.HDataSource;
import com.zerodhatech.kiteconnect.KiteConnect;
import com.zerodhatech.kiteconnect.kitehttp.exceptions.KiteException;
import com.zerodhatech.kiteconnect.utils.Constants;
import com.zerodhatech.models.CombinedMarginData;
import com.zerodhatech.models.MarginCalculationParams;

public class G3CoveredCallAlgoThread extends G3BaseClass implements Runnable{

	private static final Logger log = LogManager.getLogger(G3CoveredCallAlgoThread.class);
	
	private int lotsPerBatch = 5;
	
	public float baseDelta = 0.5f;
	
	public float quickGainExitProfit = 1000f;
	public String quickGainExitTime = "15:15";
	
	public float decayPoints = 0f;
	
	private float openingStraddlePremium = -1f;
	
	private SimpleDateFormat postgresLongDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public G3CoveredCallAlgoThread(Long napAlgoId, String backTestDateStr) {
		super(napAlgoId);
		initializeParameters(backTestDateStr);
		
		fileLogTelegramWriter.write(this.algoname);
		Thread t = new Thread(this, this.mainInstrument.getShortName()+this.algoname);
		t.setPriority(Thread.MAX_PRIORITY);
		t.start();
		printFields(this);
	}
	
	@Override
	public void run() {
		try {
			long ceDbId = -1;
			
			this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
			fileLogTelegramWriter.write( " this.instrumentLtp="+this.instrumentLtp+" isExpiryToday() "+isExpiryToday());
	
			
			String[] entryStraddleOptionNames = null;
			
			float deltaDiff = 1f;
			do { // Wait till get delta near exact baseDelta
				entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(baseDelta, this.optimalHedgeDistance);
				OptionGreek ceOptionGreek = getOptionGreeks(entryStraddleOptionNames[0]);
				deltaDiff = Math.abs(ceOptionGreek.getDelta() - baseDelta);
				if (deltaDiff>0.02f) {
					sleep(5);
				}
			} while(deltaDiff>0.02f);
			
			String syntheticFutureLongCE = entryStraddleOptionNames[0]; // CALL Buy
			OptionGreek syntheticFutureLongCEGreek = getOptionGreeks(syntheticFutureLongCE);
			
			String syntheticFutureShortPE  = entryStraddleOptionNames[0].replace("CE", "PE"); // PUT sell
			OptionGreek syntheticFutureShortPEGreek = getOptionGreeks(syntheticFutureShortPE);
			
			fileLogTelegramWriter.write( "Synthetix future greeks Begn-----"); 
			print(syntheticFutureLongCEGreek, syntheticFutureShortPEGreek);
			fileLogTelegramWriter.write( "Synthetix future greeks Ends-----");
			
			String hedge4FutureShortPE = entryStraddleOptionNames[3];	
			int qtyOfHedge4FutureShortPE = 0;
			
			// Coevered options
			String soldCEOptionname = entryStraddleOptionNames[0];	
			String hedge4SoldCEOptionname = entryStraddleOptionNames[2];
			
			if (this.placeActualOrder) {
				Map<String, Integer> orderPositions = new LinkedHashMap<String, Integer>();
				orderPositions.put(syntheticFutureLongCE, lotsPerBatch*lotSize); // Buy
				orderPositions.put(hedge4FutureShortPE, lotsPerBatch*lotSize); // Buy
				orderPositions.put(hedge4SoldCEOptionname, lotsPerBatch*lotSize*2); // Buy
				orderPositions.put(syntheticFutureShortPE, -lotsPerBatch*lotSize); // Short
				orderPositions.put(soldCEOptionname, -lotsPerBatch*lotSize*2); // Sell
				setCoeveredMarginRequired(orderPositions, 800000f);
			}
			noOfBatches = this.noOfLots; // Override
			fileLogTelegramWriter.write( "noOfBatches set to "+noOfBatches);
			
			fileLogTelegramWriter.write( "Taking syntheticFuture  BUY="+syntheticFutureLongCE+"@"+syntheticFutureLongCEGreek.getLtp()+" SELL="+syntheticFutureShortPE+"@"+syntheticFutureShortPEGreek.getLtp());
			
			List<Long> syntheticFutureShortOrderIds = new ArrayList<Long>();
			List<Long> syntheticFutureLongOrderIds = new ArrayList<Long>();
			for(int i=0;i<lotsPerBatch;i++) {
				Long longOrderDbId = createAlgoBuyOrder(syntheticFutureLongCE, syntheticFutureLongCEGreek.getLtp(),  noOfBatches*lotSize);
				syntheticFutureLongOrderIds.add(longOrderDbId);
				
				Long shortOrderDbId = createAlgoSellOrder(syntheticFutureShortPE, syntheticFutureShortPEGreek.getLtp(), noOfBatches*lotSize);
				syntheticFutureShortOrderIds.add(shortOrderDbId);
			}
			if (this.placeActualOrder) {
				placeRealOrder( syntheticFutureLongOrderIds.get(0),  syntheticFutureLongCE,  noOfBatches*lotsPerBatch*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
				placeRealOrder( hedge4FutureShortPE, noOfBatches*lotsPerBatch*lotSize, "BUY",  true, KiteUtil.USE_NORMAL_ORDER_FALSE); // Hedge for short leg of Synthetic future
				qtyOfHedge4FutureShortPE = lotsPerBatch;
				placeRealOrder( syntheticFutureShortOrderIds.get(0), syntheticFutureShortPE, noOfBatches*lotsPerBatch*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
			}
			this.noOfOrders = this.noOfOrders-8;
				
			float cePrice = getPriceFromTicks(soldCEOptionname);
			fileLogTelegramWriter.write( "Sold CE " + soldCEOptionname + "@" +cePrice+" Qty=" + (noOfBatches*lotsPerBatch*lotSize*2));
			ceDbId = createAlgoSellOrder(soldCEOptionname, cePrice, noOfBatches*lotsPerBatch*lotSize*2); // 0.5 delta two sets = 1delta to match net delta with Synthetic future delta
			if (this.placeActualOrder) {
				placeRealOrder( hedge4SoldCEOptionname, noOfBatches*lotsPerBatch*lotSize*2, "BUY",  true, KiteUtil.USE_NORMAL_ORDER_FALSE); // Additional hedge for option sold
				placeRealOrder( ceDbId, soldCEOptionname, noOfBatches*lotsPerBatch*lotSize*2, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
			}
			
			if (this.placeActualOrder) { // Update all order price
				sleep(5);
				for(int i=1;i<syntheticFutureLongOrderIds.size();i++) {
					updateOrderPrice(syntheticFutureLongOrderIds.get(0), syntheticFutureLongOrderIds.get(i), "BUY");
					updateOrderPrice(syntheticFutureShortOrderIds.get(0), syntheticFutureShortOrderIds.get(i), "SELL");
				}
			}
			
			float currentDeltaPtr = 0.5f;
			float lowerDelta = currentDeltaPtr - 0.1f;
			float upperDelta = currentDeltaPtr + 0.1f;
			
			
			float maxProfitReached = 0f;
			Date maxProfitReachedAt = getCurrentTime();
			float maxLowestpointReached = 0f;
			Date maxLowestpointReachedAt = getCurrentTime();
			float maxTrailingProfit = 0f;
			
			updateAlgoStatus("Running");
			
			do {
				sleep(5); 
				
				this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
				
				// Update open position prices
				OptionGreek ceOptionGreeks = getOptionGreeks(soldCEOptionname);
				print(ceOptionGreeks);
				float runningCePrice = ceOptionGreeks==null?0: ceOptionGreeks.getLtp();
				updateCurrentOrderBuyPrice(soldCEOptionname, ceDbId, runningCePrice);
				
				OptionGreek aOptionGreeks = getOptionGreeks(syntheticFutureLongCE);
				for(int i=0;i<syntheticFutureLongOrderIds.size();i++) {
					updateCurrentOrderSellPrice(syntheticFutureLongCE, syntheticFutureLongOrderIds.get(i), aOptionGreeks.getLtp());
				}
				aOptionGreeks = getOptionGreeks(syntheticFutureShortPE);
				for(int i=0;i<syntheticFutureShortOrderIds.size();i++) {
					updateCurrentOrderBuyPrice(syntheticFutureShortPE, syntheticFutureShortOrderIds.get(i), aOptionGreeks.getLtp());
				}
				
				currentProfitPerUnit = getProfitFromDB();
				if (currentProfitPerUnit>maxProfitReached) {
					maxProfitReached=currentProfitPerUnit;
					maxProfitReachedAt = getCurrentTime();
				}
				if (currentProfitPerUnit<maxLowestpointReached) {
					maxLowestpointReached=currentProfitPerUnit;
					maxLowestpointReachedAt = getCurrentTime();
				}
				trailingProfit = (currentProfitPerUnit-maxProfitReached);
				if (trailingProfit<maxTrailingProfit) {
					maxTrailingProfit = trailingProfit;
				}
				fileLogTelegramWriter.write( " instrumentLtp=" + this.instrumentLtp +" currentProfit="+currentProfitPerUnit+" maxLowestpointReachedPerUnit="+(maxLowestpointReached)+" maxTrailingProfit="+maxTrailingProfit);
				fileLogTelegramWriter.write( " currentDeltaPtr="+currentDeltaPtr+" lowerDelta="+lowerDelta+" upperDelta="+upperDelta+" soldCEDelta="+ceOptionGreeks.getDelta()+" syntheticFutureLongOrderIds.size="+syntheticFutureLongOrderIds.size() + " syntheticFutureShortOrderIds.size="+syntheticFutureShortOrderIds.size());
				if (ceOptionGreeks.getDelta() < lowerDelta) {
					fileLogTelegramWriter.write( "Chande In Delta, Sold Call delta reduced, reducing synthetic future size by 1");
					if (syntheticFutureLongOrderIds.size()>0) {
						if (this.placeActualOrder) {
							placeRealOrder( syntheticFutureShortOrderIds.get(0), syntheticFutureShortPE, noOfBatches*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
							placeRealOrder( syntheticFutureLongOrderIds.get(0),  syntheticFutureLongCE,  noOfBatches*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						syntheticFutureLongOrderIds.remove(0);
						syntheticFutureShortOrderIds.remove(0);
						currentDeltaPtr = lowerDelta;
						lowerDelta = currentDeltaPtr - 0.1f;
						upperDelta = currentDeltaPtr + 0.1f; 
					} else {
						prepareExit("Underflow future contracts");
					}
				} else if (ceOptionGreeks.getDelta() > upperDelta) {
					fileLogTelegramWriter.write( "Chande In Delta, Sold Call delta increased, increase synthetic future size by 1");
					if (syntheticFutureLongOrderIds.size()<lotsPerBatch*2) {
						if (this.noOfOrders<maxAllowedNoOfOrders) {
							Long longOrderDbId = createAlgoBuyOrder(syntheticFutureLongCE, getPriceFromTicks(syntheticFutureLongCE),  noOfBatches*lotSize);
							syntheticFutureLongOrderIds.add(longOrderDbId);
							
							Long shortOrderDbId = createAlgoSellOrder(syntheticFutureShortPE, getPriceFromTicks(syntheticFutureShortPE), noOfBatches*lotSize);
							syntheticFutureShortOrderIds.add(shortOrderDbId);
							if (this.placeActualOrder) {
								placeRealOrder( longOrderDbId,  syntheticFutureLongCE,  noOfBatches*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
								if (qtyOfHedge4FutureShortPE <= syntheticFutureLongOrderIds.size()) {
									qtyOfHedge4FutureShortPE++;
									placeRealOrder( hedge4FutureShortPE, noOfBatches*lotSize, "BUY",  true, KiteUtil.USE_NORMAL_ORDER_FALSE); // Hedge for short leg of Synthetic future
								}
								placeRealOrder( shortOrderDbId, syntheticFutureShortPE, noOfBatches*lotSize, "SELL", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
							currentDeltaPtr = upperDelta;
							lowerDelta = currentDeltaPtr - 0.1f;
							upperDelta = currentDeltaPtr + 0.1f;
						} else {
							prepareExit("Overflow future contracts");
						}
					} else {
						prepareExit("Too many future position exist");
					}
				}
				checkExitSignals();
				saveAlgoDailySummary(currentProfitPerUnit, maxProfitReached, maxProfitReachedAt, maxLowestpointReached, maxLowestpointReachedAt, maxTrailingProfit);
			} while(!exitThread);
			updateAlgoStatus("Terminated");
			String logString = "Exiting all open positions"; 
			log.info(logString);
			fileLogTelegramWriter.write( " " + logString);
			
			// exit all positions
			if (this.placeActualOrder) {
				placeRealOrder(ceDbId, soldCEOptionname, noOfBatches*lotsPerBatch*lotSize*2, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE); // Covered call CE short positions 
				placeRealOrder(hedge4SoldCEOptionname, noOfBatches*lotsPerBatch*lotSize*2, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);  // Hedge of above
				
				if (syntheticFutureShortOrderIds.size()>0) {
					placeRealOrder(syntheticFutureShortPE, syntheticFutureShortOrderIds.size()*noOfBatches*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
					placeRealOrder(syntheticFutureLongCE, syntheticFutureLongOrderIds.size()*noOfBatches*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
				}
				placeRealOrder(hedge4FutureShortPE, qtyOfHedge4FutureShortPE*noOfBatches*lotSize, "SELL", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
			}
			
			fileLogTelegramWriter.write( " noOfOrders="+noOfOrders + " ROI=" + (currentProfitPerUnit*this.lotSize*100f)/requiredMargin + "% (Max profit/lot reached to "+ (maxProfitReached) +"@" + maxProfitReachedAt+ "\n and Lowest reached to " + (maxLowestpointReached) + "@" + maxLowestpointReachedAt + ")");
		} catch (Exception e) {			
			updateAlgoStatus("Error");
			log.error("Error"+e.getMessage(), e);
			fileLogTelegramWriter.write("Error " + ExceptionUtils.getStackTrace(e));
		} finally {
			fileLogTelegramWriter.close();
		}
	}
	
	protected void checkExitSignals() {
		super.checkExitSignals();
		if (this.quickGainExitProfit != 0 && this.currentProfitPerUnit > this.quickGainExitProfit) {
			if (!isExpiryToday()) {
				String[] exitTimeParts = quickGainExitTime.split(":");
				int quickGainExitHour = Integer.parseInt(exitTimeParts[0]);
				int quickGainExitMinute = Integer.parseInt(exitTimeParts[1]);
				
				fileLogTelegramWriter.write( "quickGainExitHour="+quickGainExitHour+" quickGainExitMinute="+quickGainExitMinute+" timeout "+timeout(quickGainExitHour, quickGainExitMinute, 0));
				if (timein(quickGainExitHour, quickGainExitMinute, 0)) {
					prepareExit("Quick Target acheived");
				}
			}
		}
		if (this.decayPoints > 0f ) {
			if (this.openingStraddlePremium < 0f) { // First time, set the morning straddle premium 
				this.openingStraddlePremium =  getStraddlePremium(9,20);
			}
			float currentStraddlePremium =  getStraddlePremium(0,0);
			if (this.openingStraddlePremium - currentStraddlePremium > this.decayPoints) {
				prepareExit("Premium Decay over");
			}
		}
	}
	
	private float getStraddlePremium(int hourOfTheDay, int minuteOfTheDay) {
		float retVal = 0f;
		Connection conn = null;
		try {			
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			Calendar cal = Calendar.getInstance();
			cal.setTime(getCurrentTime());
			if (hourOfTheDay > 0) {
				cal.set(Calendar.HOUR_OF_DAY, hourOfTheDay);
			}
			if (minuteOfTheDay > 0) {
				cal.set(Calendar.MINUTE, minuteOfTheDay);
			}
			
			String fetchSql = "SELECT celtp+peltp as stradlePremium FROM nexcorio_option_atm_movement_data WHERE f_main_instrument=" + this.mainInstrument.getId() + " AND record_time <= '" + postgresLongDateFormat.format(cal.getTime())+ "' ORDER BY record_time desc LIMIT 1";
			fileLogTelegramWriter.write( fetchSql);
			
			ResultSet rs = stmt.executeQuery(fetchSql);
			while (rs.next()) {
	    		retVal  = rs.getFloat("stradlePremium");
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
	
	
	public float getProfitFromDB() {
		
		float retVal = 0f;
		Connection conn = null;
		try {
			
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			String fetchNextSeq = "select sum((sell_price-buy_price)*quantity) as profitPerLot from nexcorio_option_algo_orders where short_date = '" + postgresShortDateFormat.format(getCurrentTime())+ "' and f_strategy="+this.napAlgoId;
			//fileLogTelegramWriter.write("fetchNextSeq="+fetchNextSeq);
			ResultSet rs = stmt.executeQuery(fetchNextSeq);
	    	while (rs.next()) {
	    		retVal  = rs.getFloat("profitPerLot");
			}
			rs.close();
			stmt.close();
			retVal =retVal / (noOfBatches*6*this.lotSize); 
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
	
	public static void main(String[] args) {
		new G3CoveredCallAlgoThread(23L, null);
	}	

}
