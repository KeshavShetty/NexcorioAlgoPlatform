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

public class G3DynamicBatchStraddleAlgoThread extends G3BaseClass implements Runnable{

	private static final Logger log = LogManager.getLogger(G3DynamicBatchStraddleAlgoThread.class);
	
	private int lotsPerBatch = 5;
	
	public int noOfBatches = 1;
	public float baseDelta = 0.5f;
	
	
	public G3DynamicBatchStraddleAlgoThread(Long napAlgoId, String backTestDateStr) {
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
			
			this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
			fileLogTelegramWriter.write( " this.instrumentLtp="+this.instrumentLtp);
	
			String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(baseDelta, this.optimalHedgeDistance);
			
			ceStraddleOptionName =  entryStraddleOptionNames[0];
			ceHedgeOptionName =  entryStraddleOptionNames[2];
			
			peStraddleOptionName =  entryStraddleOptionNames[1];
			peHedgeOptionName =  entryStraddleOptionNames[3];
			
			OptionGreek ceOptionGreeks = getOptionGreeks(ceStraddleOptionName);
			OptionGreek peOptionGreeks = getOptionGreeks(peStraddleOptionName);
			print(ceOptionGreeks, peOptionGreeks);
			
			List<Long> ceOrderIds = new ArrayList<Long>();
			for(int i=0;i<lotsPerBatch;i++) {
				Long ceDbId = createAlgoSellOrder(ceStraddleOptionName, ceOptionGreeks.getLtp(), noOfBatches*lotSize);
				ceOrderIds.add(ceDbId);
			}
			if (this.placeActualOrder) { 
				placeRealOrder( ceHedgeOptionName, lotsPerBatch*noOfBatches*lotSize, "BUY",  true, KiteUtil.USE_NORMAL_ORDER_FALSE);
				placeRealOrder( ceOrderIds.get(0), ceStraddleOptionName, lotsPerBatch*noOfBatches*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
			}
			
			List<Long> peOrderIds = new ArrayList<Long>();
			for(int i=0;i<lotsPerBatch;i++) {
				Long peDbId = createAlgoSellOrder(peStraddleOptionName, peOptionGreeks.getLtp(), noOfBatches*lotSize);
				peOrderIds.add(peDbId);
			}
			if (this.placeActualOrder) { 
				placeRealOrder( peHedgeOptionName, lotsPerBatch*noOfBatches*lotSize, "BUY",  true, KiteUtil.USE_NORMAL_ORDER_FALSE);
				placeRealOrder( peOrderIds.get(0), peStraddleOptionName, lotsPerBatch*noOfBatches*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
			}
			
			float ceDeltaPtr = Math.abs(ceOptionGreeks.getDelta());
			float peDeltaPtr = Math.abs(peOptionGreeks.getDelta());
			this.noOfOrders = this.noOfOrders-(lotsPerBatch*2-2);
			
			float maxProfitReached = 0f;
			Date maxProfitReachedAt = getCurrentTime();
			float maxLowestpointReached = 0f;
			Date maxLowestpointReachedAt = getCurrentTime();
			float maxTrailingProfit = 0f;
			
			updateAlgoStatus("Running");
			
			do {
				sleep(5); 
				
				this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
				
				ceOptionGreeks = getOptionGreeks(ceStraddleOptionName);
				peOptionGreeks = getOptionGreeks(peStraddleOptionName);
				print(ceOptionGreeks, peOptionGreeks);
				
				for(int i=0;i<ceOrderIds.size();i++) {
					updateCurrentOrderBuyPrice(ceStraddleOptionName, ceOrderIds.get(i), ceOptionGreeks.getLtp());
				}
				for(int i=0;i<peOrderIds.size();i++) {
					updateCurrentOrderBuyPrice(peStraddleOptionName, peOrderIds.get(i), peOptionGreeks.getLtp());
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
				fileLogTelegramWriter.write( " ceDeltaPtr="+ceDeltaPtr+" peDeltaPtr="+peDeltaPtr+" ceOrderIds.size="+ceOrderIds.size()+" peOrderIds.size="+peOrderIds.size());
				
				if (Math.abs(ceOptionGreeks.getDelta()) > ceDeltaPtr+0.05f) {
					if (ceOrderIds.size()>0) {
						if (this.placeActualOrder) { // Exit one set
							placeRealOrder( ceOrderIds.get(0),  ceStraddleOptionName,  noOfBatches*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						ceOrderIds.remove(0);
						ceDeltaPtr = Math.abs(ceOptionGreeks.getDelta());
					}
				} else if (Math.abs(ceOptionGreeks.getDelta()) < ceDeltaPtr-0.05f) {
					if (ceOrderIds.size()<lotsPerBatch) {
						Long ceDbId = createAlgoSellOrder(ceStraddleOptionName, ceOptionGreeks.getLtp(), noOfBatches*lotSize);
						ceOrderIds.add(ceDbId);
						if (this.placeActualOrder) { // Add one set
							placeRealOrder( ceOrderIds.get(0),  ceStraddleOptionName,  noOfBatches*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						ceDeltaPtr = Math.abs(ceOptionGreeks.getDelta());
					}
				}
				
				if (Math.abs(peOptionGreeks.getDelta()) > peDeltaPtr+0.05f) {
					if (peOrderIds.size()>0) {
						if (this.placeActualOrder) { // Exit one set
							placeRealOrder( peOrderIds.get(0),  peStraddleOptionName,  noOfBatches*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						peOrderIds.remove(0);
						peDeltaPtr = Math.abs(peOptionGreeks.getDelta());
					}
				} else if (Math.abs(peOptionGreeks.getDelta()) < peDeltaPtr-0.05f) {
					if (peOrderIds.size()<lotsPerBatch) {
						Long peDbId = createAlgoSellOrder(peStraddleOptionName, peOptionGreeks.getLtp(), noOfBatches*lotSize);
						peOrderIds.add(peDbId);
						if (this.placeActualOrder) { // Add one set
							placeRealOrder( peOrderIds.get(0),  peStraddleOptionName,  noOfBatches*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						peDeltaPtr = Math.abs(peOptionGreeks.getDelta());
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
				// Todo
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
			retVal =retVal / (noOfBatches*lotsPerBatch*this.lotSize); 
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
		new G3DynamicBatchStraddleAlgoThread(23L, null);
	}	

}
