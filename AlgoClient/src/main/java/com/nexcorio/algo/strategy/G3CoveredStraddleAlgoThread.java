package com.nexcorio.algo.strategy;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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

import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.util.KiteUtil;
import com.nexcorio.algo.util.db.HDataSource;
import com.zerodhatech.kiteconnect.KiteConnect;
import com.zerodhatech.kiteconnect.kitehttp.exceptions.KiteException;
import com.zerodhatech.kiteconnect.utils.Constants;
import com.zerodhatech.models.CombinedMarginData;
import com.zerodhatech.models.MarginCalculationParams;

public class G3CoveredStraddleAlgoThread extends G3BaseClass implements Runnable{

	private static final Logger log = LogManager.getLogger(G3CoveredStraddleAlgoThread.class);
	
	private int lotsPerBatch = 5;
	
	public float baseDelta = 0.5f;
	
	public float quickGainExitProfit = 1000f;
	public String quickGainExitTime = "15:15";
	
	public G3CoveredStraddleAlgoThread(Long napAlgoId, String backTestDateStr) {
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
			//this.placeActualOrder = true;
			this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
			fileLogTelegramWriter.write( " this.instrumentLtp="+this.instrumentLtp+" isExpiryToday() "+isExpiryToday());
	
			String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(baseDelta-0.1f, this.optimalHedgeDistance);
			
			String syntheticFutureCE = entryStraddleOptionNames[0]; 
			OptionGreek syntheticFutureLongCEGreek = getOptionGreeks(syntheticFutureCE);
			
			String syntheticFuturePE  = entryStraddleOptionNames[0].replace("CE", "PE"); 
			OptionGreek syntheticFutureShortPEGreek = getOptionGreeks(syntheticFuturePE);
			
			fileLogTelegramWriter.write( "Synthetix future greeks Begn-----"); 
			print(syntheticFutureLongCEGreek, syntheticFutureShortPEGreek);
			fileLogTelegramWriter.write( "Synthetix future greeks Ends-----");
			
			String hedge4FutureCE = entryStraddleOptionNames[2];
			String hedge4FuturePE = entryStraddleOptionNames[3];
			
			// Coevered options
			entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(baseDelta, this.optimalHedgeDistance);
			
			String soldCEOptionname = entryStraddleOptionNames[0];	
			String hedge4SoldCEOptionname = entryStraddleOptionNames[2];
			
			String soldPEOptionname = entryStraddleOptionNames[1];	
			String hedge4SoldPEOptionname = entryStraddleOptionNames[3];
			
			OptionGreek shortCEGreek = getOptionGreeks(soldCEOptionname);
			OptionGreek shortPEGreek = getOptionGreeks(soldPEOptionname);
			
			fileLogTelegramWriter.write( "Straddle greeks Begn-----"); 
			print(shortCEGreek, shortPEGreek);
			fileLogTelegramWriter.write( "Straddle greeks Ends-----");
			
			if (this.placeActualOrder) {
				Map<String, Integer> orderPositions = new LinkedHashMap<String, Integer>();
				orderPositions.put(hedge4SoldCEOptionname, lotsPerBatch*lotSize); // Buy
				orderPositions.put(hedge4SoldPEOptionname, lotsPerBatch*lotSize); // Buy
				
				orderPositions.put(soldCEOptionname, -lotsPerBatch*lotSize); // Sell
				orderPositions.put(soldPEOptionname, -lotsPerBatch*lotSize); // Sell				
				setCoeveredMarginRequired(orderPositions, 800000f);
			}
			noOfBatches = this.noOfLots; // Override
			fileLogTelegramWriter.write( "noOfBatches set to "+noOfBatches);
			
			fileLogTelegramWriter.write( "Taking Straddle SELL="+soldCEOptionname+"@"+shortCEGreek.getLtp()+" SELL="+soldPEOptionname+"@"+shortPEGreek.getLtp());
			
			Long ceDbId = createAlgoSellOrder(soldCEOptionname, shortCEGreek.getLtp(), noOfBatches*lotsPerBatch*lotSize*2);
			Long peDbId = createAlgoSellOrder(soldPEOptionname, shortPEGreek.getLtp(), noOfBatches*lotsPerBatch*lotSize*2);
			
			if (this.placeActualOrder) {
				placeRealOrder(hedge4SoldCEOptionname, noOfBatches*lotsPerBatch*lotSize*2, "BUY",  true, KiteUtil.USE_NORMAL_ORDER_FALSE); 
				placeRealOrder(hedge4SoldPEOptionname, noOfBatches*lotsPerBatch*lotSize*2, "BUY",  true, KiteUtil.USE_NORMAL_ORDER_FALSE); 
				
				placeRealOrder( ceDbId, soldCEOptionname, noOfBatches*lotsPerBatch*lotSize*2, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
				placeRealOrder( peDbId, soldPEOptionname, noOfBatches*lotsPerBatch*lotSize*2, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
			}
			
			float currentCEDeltaPtr = 0.5f;
			float currentPEDeltaPtr = 0.5f;
			
			List<Long> syntheticFutureCeShortOrderIds = new ArrayList<Long>();
			List<Long> syntheticFutureCeLongOrderIds = new ArrayList<Long>();
			
			List<Long> syntheticFuturePeShortOrderIds = new ArrayList<Long>();
			List<Long> syntheticFuturePeLongOrderIds = new ArrayList<Long>();
			
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
				OptionGreek peOptionGreeks = getOptionGreeks(soldPEOptionname);
				print(ceOptionGreeks, peOptionGreeks);
				
				updateCurrentOrderBuyPrice(soldCEOptionname, ceDbId, ceOptionGreeks.getLtp());
				updateCurrentOrderBuyPrice(soldPEOptionname, peDbId, peOptionGreeks.getLtp());
				
				for(Long ceShortId:syntheticFutureCeShortOrderIds) {
					updateCurrentOrderBuyPrice(syntheticFutureCE, ceShortId, getPriceFromTicks(syntheticFutureCE));
				}
				for(Long peShortId:syntheticFuturePeShortOrderIds) {
					updateCurrentOrderBuyPrice(syntheticFuturePE, peShortId, getPriceFromTicks(syntheticFuturePE));
				}
				
				for(Long ceLongId:syntheticFutureCeLongOrderIds) {
					updateCurrentOrderSellPrice(syntheticFutureCE, ceLongId, getPriceFromTicks(syntheticFutureCE));
				}
				for(Long peLongId:syntheticFuturePeLongOrderIds) {
					updateCurrentOrderSellPrice(syntheticFuturePE, peLongId, getPriceFromTicks(syntheticFuturePE));
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
				fileLogTelegramWriter.write( " currentCeDeltaPtr="+currentCEDeltaPtr+" currentPeDeltaPtr="+currentPEDeltaPtr);
				
				if (Math.abs(ceOptionGreeks.getDelta()) > 0.9f || Math.abs(ceOptionGreeks.getDelta()) < 0.1f
						|| Math.abs(peOptionGreeks.getDelta()) > 0.9f || Math.abs(peOptionGreeks.getDelta()) < 0.1f
						) { // Delta overflow
					fileLogTelegramWriter.write( "Delta overflow, Exit all");
					prepareExit("Delta overflow");
				} else {
					if (Math.abs(ceOptionGreeks.getDelta()) > currentCEDeltaPtr+0.1f) { // CE Crossed 0.6
						fileLogTelegramWriter.write("CE DeltaPtr moved UP");
						// +CE -PE
						if (syntheticFutureCeShortOrderIds.size()>0) {
							if (this.placeActualOrder) {
								placeRealOrder( syntheticFutureCeShortOrderIds.get(0), syntheticFutureCE, noOfBatches*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
								placeRealOrder( syntheticFuturePeLongOrderIds.get(0),  syntheticFuturePE, noOfBatches*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
								placeRealOrder( hedge4FutureCE, noOfBatches*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE); // Hedge of CE 
							}
							syntheticFutureCeShortOrderIds.remove(0);
							syntheticFuturePeLongOrderIds.remove(0);
						} else { // buy fresh
							Long longOrderDbId = createAlgoBuyOrder(syntheticFutureCE, getOptionGreeks(syntheticFutureCE).getLtp(),  noOfBatches*lotSize);
							syntheticFutureCeLongOrderIds.add(longOrderDbId);
							
							Long shortOrderDbId = createAlgoSellOrder(syntheticFuturePE, getOptionGreeks(syntheticFuturePE).getLtp(), noOfBatches*lotSize);
							syntheticFuturePeShortOrderIds.add(shortOrderDbId);
							if (this.placeActualOrder) {
								placeRealOrder( longOrderDbId, syntheticFutureCE, noOfBatches*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
								placeRealOrder( hedge4FuturePE, noOfBatches*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE); // Hedge for PE
								placeRealOrder( shortOrderDbId,  syntheticFuturePE, noOfBatches*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
						}
						currentCEDeltaPtr = currentCEDeltaPtr+0.1f;
					} else if (Math.abs(ceOptionGreeks.getDelta()) < currentCEDeltaPtr-0.1f) { // CE Below 0.4
						fileLogTelegramWriter.write("CE DeltaPtr moved DOWN");
						// -CE +PE
						
						if (syntheticFutureCeLongOrderIds.size()>0) {
							if (this.placeActualOrder) {
								placeRealOrder( syntheticFuturePeShortOrderIds.get(0),  syntheticFuturePE, noOfBatches*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
								placeRealOrder( syntheticFutureCeLongOrderIds.get(0), syntheticFutureCE, noOfBatches*lotSize, "SELL", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
								placeRealOrder( hedge4FuturePE, noOfBatches*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE); // Hedge of CE 
							}
							syntheticFutureCeLongOrderIds.remove(0);
							syntheticFuturePeShortOrderIds.remove(0);
						} else { // buy fresh
							Long shortOrderDbId = createAlgoSellOrder(syntheticFutureCE, getOptionGreeks(syntheticFutureCE).getLtp(), noOfBatches*lotSize);
							syntheticFutureCeShortOrderIds.add(shortOrderDbId);
							
							Long longOrderDbId = createAlgoBuyOrder(syntheticFuturePE, getOptionGreeks(syntheticFuturePE).getLtp(),  noOfBatches*lotSize);
							syntheticFuturePeLongOrderIds.add(longOrderDbId);
							
							if (this.placeActualOrder) {
								placeRealOrder( longOrderDbId, syntheticFuturePE, noOfBatches*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
								placeRealOrder( hedge4FutureCE, noOfBatches*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE); // Hedge for PE
								placeRealOrder( shortOrderDbId,  syntheticFutureCE, noOfBatches*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
						}
						currentCEDeltaPtr = currentCEDeltaPtr-0.1f;
					}
					
					if (Math.abs(peOptionGreeks.getDelta()) > currentPEDeltaPtr+0.1f) { // PE Crossed 0.6
						// -CE +PE
						fileLogTelegramWriter.write("PE DeltaPtr moved UP");
						if (syntheticFutureCeLongOrderIds.size()>0) {
							if (this.placeActualOrder) {
								placeRealOrder( syntheticFuturePeShortOrderIds.get(0),  syntheticFuturePE, noOfBatches*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
								placeRealOrder( syntheticFutureCeLongOrderIds.get(0), syntheticFutureCE, noOfBatches*lotSize, "SELL", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
								placeRealOrder( hedge4FuturePE, noOfBatches*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE); // Hedge of CE 
							}
							syntheticFutureCeLongOrderIds.remove(0);
							syntheticFuturePeShortOrderIds.remove(0);
						} else { // buy fresh
							Long shortOrderDbId = createAlgoSellOrder(syntheticFutureCE, getOptionGreeks(syntheticFutureCE).getLtp(), noOfBatches*lotSize);
							syntheticFutureCeShortOrderIds.add(shortOrderDbId);
							
							Long longOrderDbId = createAlgoBuyOrder(syntheticFuturePE, getOptionGreeks(syntheticFuturePE).getLtp(),  noOfBatches*lotSize);
							syntheticFuturePeLongOrderIds.add(longOrderDbId);
							
							if (this.placeActualOrder) {
								placeRealOrder( longOrderDbId, syntheticFuturePE, noOfBatches*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
								placeRealOrder( hedge4FutureCE, noOfBatches*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE); // Hedge for PE
								placeRealOrder( shortOrderDbId,  syntheticFutureCE, noOfBatches*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
						}
						currentPEDeltaPtr = currentPEDeltaPtr+0.1f;
					} else if (Math.abs(peOptionGreeks.getDelta()) < currentPEDeltaPtr-0.1f) { // PE Below 0.4
						// +CE -PE
						fileLogTelegramWriter.write("PE DeltaPtr moved DOWN");
						if (syntheticFutureCeShortOrderIds.size()>0) {
							if (this.placeActualOrder) {
								placeRealOrder( syntheticFutureCeShortOrderIds.get(0), syntheticFutureCE, noOfBatches*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
								placeRealOrder( syntheticFuturePeLongOrderIds.get(0),  syntheticFuturePE, noOfBatches*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
								placeRealOrder( hedge4FutureCE, noOfBatches*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE); // Hedge of CE 
							}
							syntheticFutureCeShortOrderIds.remove(0);
							syntheticFuturePeLongOrderIds.remove(0);
						} else { // buy fresh
							Long longOrderDbId = createAlgoBuyOrder(syntheticFutureCE, getOptionGreeks(syntheticFutureCE).getLtp(),  noOfBatches*lotSize);
							syntheticFutureCeLongOrderIds.add(longOrderDbId);
							
							Long shortOrderDbId = createAlgoSellOrder(syntheticFuturePE, getOptionGreeks(syntheticFuturePE).getLtp(), noOfBatches*lotSize);
							syntheticFuturePeShortOrderIds.add(shortOrderDbId);
							if (this.placeActualOrder) {
								placeRealOrder( longOrderDbId, syntheticFutureCE, noOfBatches*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
								placeRealOrder( hedge4FuturePE, noOfBatches*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE); // Hedge for PE
								placeRealOrder( shortOrderDbId,  syntheticFuturePE, noOfBatches*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
						}
						currentPEDeltaPtr = currentPEDeltaPtr-0.1f;
					}
				}
				checkExitSignals();
				saveAlgoDailySummary(currentProfitPerUnit, maxProfitReached, maxProfitReachedAt, maxLowestpointReached, maxLowestpointReachedAt, maxTrailingProfit);
			} while(!exitThread);
			updateAlgoStatus("Terminated");
			String logString = "Exiting all open positions"; 
			log.info(logString);
			fileLogTelegramWriter.write( " " + logString);
			
			
			fileLogTelegramWriter.write("syntheticFutureCeShortOrderIds.size()="+syntheticFutureCeShortOrderIds.size());
			fileLogTelegramWriter.write("syntheticFuturePeShortOrderIds.size()="+syntheticFuturePeShortOrderIds.size());
			fileLogTelegramWriter.write("syntheticFutureCeLongOrderIds.size()="+syntheticFutureCeLongOrderIds.size());
			fileLogTelegramWriter.write("syntheticFuturePeLongOrderIds.size()="+syntheticFuturePeLongOrderIds.size());
			
			// exit all positions
			if (this.placeActualOrder) {
				placeRealOrder(ceDbId, soldCEOptionname, noOfBatches*lotsPerBatch*lotSize*2, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE); 
				placeRealOrder(peDbId, soldPEOptionname, noOfBatches*lotsPerBatch*lotSize*2, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
				
				
				placeRealOrder(hedge4SoldCEOptionname, noOfBatches*lotsPerBatch*lotSize*2, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE); // CE Hedges
				placeRealOrder(hedge4SoldPEOptionname, noOfBatches*lotsPerBatch*lotSize*2, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE); // PE Hedges
				
				
				if (syntheticFutureCeShortOrderIds.size()>0) placeRealOrder(syntheticFutureCeShortOrderIds.get(0), syntheticFutureCE, syntheticFutureCeShortOrderIds.size()*noOfBatches*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
				if (syntheticFuturePeShortOrderIds.size()>0) placeRealOrder(syntheticFuturePeShortOrderIds.get(0), syntheticFuturePE, syntheticFuturePeShortOrderIds.size()*noOfBatches*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
				
				if (syntheticFutureCeLongOrderIds.size()>0) placeRealOrder(syntheticFutureCeLongOrderIds.get(0), syntheticFutureCE, syntheticFutureCeLongOrderIds.size()*noOfBatches*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
				if (syntheticFuturePeLongOrderIds.size()>0) placeRealOrder(syntheticFuturePeLongOrderIds.get(0), syntheticFuturePE, syntheticFuturePeLongOrderIds.size()*noOfBatches*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
				
				if (syntheticFutureCeShortOrderIds.size()>0) placeRealOrder(hedge4FutureCE, syntheticFutureCeShortOrderIds.size()*noOfBatches*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE); // Hadges of synthetic futures
				if (syntheticFuturePeShortOrderIds.size()>0) placeRealOrder(hedge4FuturePE, syntheticFuturePeShortOrderIds.size()*noOfBatches*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
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
			retVal =retVal / (noOfBatches*lotsPerBatch*2*this.lotSize); 
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
		new G3CoveredStraddleAlgoThread(23L, null);
	}	

}
