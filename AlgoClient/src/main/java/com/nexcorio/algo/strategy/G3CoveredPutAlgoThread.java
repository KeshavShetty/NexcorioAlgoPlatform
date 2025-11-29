package com.nexcorio.algo.strategy;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.util.KiteUtil;
import com.nexcorio.algo.util.db.HDataSource;

public class G3CoveredPutAlgoThread extends G3BaseClass implements Runnable{

	private static final Logger log = LogManager.getLogger(G3CoveredPutAlgoThread.class);
	
	private int lotsPerBatch = 5;
	
	public int noOfBatches = 1;
	public float baseDelta = 0.5f;
	
	
	public G3CoveredPutAlgoThread(Long napAlgoId, String backTestDateStr) {
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
			if (this.placeActualOrder) setLotBasedonAvailableMarginHalfStraddle();
			
			long peDbId = -1;
			
			this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
			fileLogTelegramWriter.write( " this.instrumentLtp="+this.instrumentLtp);
	
			String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(baseDelta-0.1f, this.optimalHedgeDistance);
			
			String syntheticFutureLongPE = entryStraddleOptionNames[1]; // PUT Buy
			OptionGreek syntheticFutureLongPEGreek = getOptionGreeks(syntheticFutureLongPE);
			
			String syntheticFutureShortCE  = entryStraddleOptionNames[1].replace("PE", "CE"); // Call sell
			OptionGreek syntheticFutureShortCEGreek = getOptionGreeks(syntheticFutureShortCE);
			
			fileLogTelegramWriter.write( "Synthetix future greeks Begn-----"); 
			print(syntheticFutureLongPEGreek, syntheticFutureShortCEGreek);
			fileLogTelegramWriter.write( "Synthetix future greeks Ends-----");
			
			String hedge4FutureShortCE = entryStraddleOptionNames[2];	
			int qtyOfHedge4FutureShortCE = 0;
			
			fileLogTelegramWriter.write( "Taking syntheticFuture  BUY="+syntheticFutureLongPE+"@"+syntheticFutureLongPEGreek.getLtp()+" SELL="+syntheticFutureShortCE+"@"+syntheticFutureShortCEGreek.getLtp());
			
			List<Long> syntheticFutureShortOrderIds = new ArrayList<Long>();
			List<Long> syntheticFutureLongOrderIds = new ArrayList<Long>();
			for(int i=0;i<lotsPerBatch;i++) {
				Long longOrderDbId = createAlgoBuyOrder(syntheticFutureLongPE, syntheticFutureLongPEGreek.getLtp(),  noOfBatches*lotSize);
				syntheticFutureLongOrderIds.add(longOrderDbId);
				
				Long shortOrderDbId = createAlgoSellOrder(syntheticFutureShortCE, syntheticFutureShortCEGreek.getLtp(), noOfBatches*lotSize);
				syntheticFutureShortOrderIds.add(shortOrderDbId);
			}
			if (this.placeActualOrder) {
				placeRealOrder( syntheticFutureLongOrderIds.get(0),  syntheticFutureLongPE,  noOfBatches*lotsPerBatch*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
				placeRealOrder( hedge4FutureShortCE, noOfBatches*lotsPerBatch*lotSize, "BUY",  true, KiteUtil.USE_NORMAL_ORDER_FALSE); // Hedge for short leg of Synthetic future
				qtyOfHedge4FutureShortCE = lotsPerBatch;
				placeRealOrder( syntheticFutureShortOrderIds.get(0), syntheticFutureShortCE, noOfBatches*lotsPerBatch*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
			}
			this.noOfOrders = this.noOfOrders-8;
			// Coevered options
			entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(baseDelta, this.optimalHedgeDistance);
			
			String soldPEOptionname = entryStraddleOptionNames[1];	
			String hedge4SoldPEOptionname = entryStraddleOptionNames[3];	
			float pePrice = getPriceFromTicks(soldPEOptionname);
			fileLogTelegramWriter.write( "Sold PE " + soldPEOptionname + "@" +pePrice+" Qty=" + (noOfBatches*lotsPerBatch*lotSize*2));
			peDbId = createAlgoSellOrder(soldPEOptionname, pePrice, noOfBatches*lotsPerBatch*lotSize*2); // 0.5 delta two sets = 1delta to match net delta with Synthetic future delta
			if (this.placeActualOrder) {
				placeRealOrder( hedge4SoldPEOptionname, noOfBatches*lotsPerBatch*lotSize*2, "BUY",  true, KiteUtil.USE_NORMAL_ORDER_FALSE); // Additional hedge for option sold
				placeRealOrder( peDbId, soldPEOptionname, noOfBatches*lotsPerBatch*lotSize*2, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
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
				OptionGreek peOptionGreeks = getOptionGreeks(soldPEOptionname);
				print(peOptionGreeks);
				float runningPePrice = peOptionGreeks==null?0: peOptionGreeks.getLtp();
				updateCurrentOrderBuyPrice(soldPEOptionname, peDbId, runningPePrice);
				
				OptionGreek aOptionGreeks = getOptionGreeks(syntheticFutureLongPE);
				for(int i=0;i<syntheticFutureLongOrderIds.size();i++) {
					updateCurrentOrderSellPrice(syntheticFutureLongPE, syntheticFutureLongOrderIds.get(i), aOptionGreeks.getLtp());
				}
				aOptionGreeks = getOptionGreeks(syntheticFutureShortCE);
				for(int i=0;i<syntheticFutureShortOrderIds.size();i++) {
					updateCurrentOrderBuyPrice(syntheticFutureShortCE, syntheticFutureShortOrderIds.get(i), aOptionGreeks.getLtp());
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
				fileLogTelegramWriter.write( " currentDeltaPtr="+currentDeltaPtr+" lowerDelta="+lowerDelta+" upperDelta="+upperDelta+" soldPEDelta="+peOptionGreeks.getDelta()+" syntheticFutureLongOrderIds.size="+syntheticFutureLongOrderIds.size() + " syntheticFutureShortOrderIds.size="+syntheticFutureShortOrderIds.size());
				if (Math.abs(peOptionGreeks.getDelta()) < lowerDelta) {
					fileLogTelegramWriter.write( "Chande In Delta, Sold Put delta reduced, reducing synthetic future size by 1");
					if (syntheticFutureLongOrderIds.size()>0) {
						if (this.placeActualOrder) {
							placeRealOrder( syntheticFutureShortOrderIds.get(0), syntheticFutureShortCE, noOfBatches*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
							placeRealOrder( syntheticFutureLongOrderIds.get(0),  syntheticFutureLongPE,  noOfBatches*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						syntheticFutureLongOrderIds.remove(0);
						syntheticFutureShortOrderIds.remove(0);
						currentDeltaPtr = lowerDelta;
						lowerDelta = currentDeltaPtr - 0.1f;
						upperDelta = currentDeltaPtr + 0.1f; 
					} else {
						prepareExit("Underflow future contracts");
					}
				} else if (Math.abs(peOptionGreeks.getDelta()) > upperDelta) {
					fileLogTelegramWriter.write( "Chande In Delta, Sold Call delta increased, increase synthetic future size by 1");
					if (syntheticFutureLongOrderIds.size()<lotsPerBatch*2) {
						if (this.noOfOrders<maxAllowedNoOfOrders) {
							Long longOrderDbId = createAlgoBuyOrder(syntheticFutureLongPE, getPriceFromTicks(syntheticFutureLongPE),  noOfBatches*lotSize);
							syntheticFutureLongOrderIds.add(longOrderDbId);
							
							Long shortOrderDbId = createAlgoSellOrder(syntheticFutureShortCE, getPriceFromTicks(syntheticFutureShortCE), noOfBatches*lotSize);
							syntheticFutureShortOrderIds.add(shortOrderDbId);
							if (this.placeActualOrder) {
								placeRealOrder( longOrderDbId,  syntheticFutureLongPE,  noOfBatches*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
								if (qtyOfHedge4FutureShortCE <= syntheticFutureLongOrderIds.size()) {
									qtyOfHedge4FutureShortCE++;
									placeRealOrder( hedge4FutureShortCE, noOfBatches*lotSize, "BUY",  true, KiteUtil.USE_NORMAL_ORDER_FALSE); // Hedge for short leg of Synthetic future
								}
								placeRealOrder( shortOrderDbId, syntheticFutureShortCE, noOfBatches*lotSize, "SELL", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
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
				placeRealOrder(peDbId, soldPEOptionname, noOfBatches*lotsPerBatch*lotSize*2, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE); // Covered call CE short positions 
				placeRealOrder(hedge4SoldPEOptionname, noOfBatches*lotsPerBatch*lotSize*2, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);  // Hedge of above
				
				if (syntheticFutureShortOrderIds.size()>0) {
					placeRealOrder(syntheticFutureShortCE, syntheticFutureShortOrderIds.size()*noOfBatches*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
					placeRealOrder(syntheticFutureLongPE, syntheticFutureLongOrderIds.size()*noOfBatches*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
				}
				placeRealOrder(hedge4FutureShortCE, qtyOfHedge4FutureShortCE*noOfBatches*lotSize, "SELL", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
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
		new G3CoveredPutAlgoThread(23L, null);
	}

	

}
