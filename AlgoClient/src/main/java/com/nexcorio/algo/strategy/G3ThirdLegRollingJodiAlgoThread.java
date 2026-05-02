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

class OptionPosition {
	Long orderId = null;
	String optionname = "";
	
	public OptionPosition(Long orderId, String optionname) {
		super();
		this.orderId = orderId;
		this.optionname = optionname;
	}
}

public class G3ThirdLegRollingJodiAlgoThread extends G3BaseClass implements Runnable {

	private static final Logger log = LogManager.getLogger(G3PriceParityIVBasedAlgoThread.class);
	
	public float baseDelta = 0.5f;	
	public float rollingIndexPts = 50f;

	
		
	public G3ThirdLegRollingJodiAlgoThread(Long napAlgoId, String backTestDateStr) {
		super(napAlgoId);
		initializeParameters(backTestDateStr);
		
		fileLogTelegramWriter.write(this.algoname);
		Thread t = new Thread(this, this.mainInstrument.getShortName()+this.algoname);
		t.setPriority(Thread.MAX_PRIORITY);
		t.start();
	}
	
	@Override
	public void run() {
		
		try {
			List<OptionPosition> ceOptions = new ArrayList<OptionPosition>();
			List<OptionPosition> peOptions = new ArrayList<OptionPosition>();
						
			this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
			
			fileLogTelegramWriter.write( " this.instrumentLtp="+this.instrumentLtp);
			
			printFields(this);
			
			this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
			fileLogTelegramWriter.write( " this.instrumentLtp="+this.instrumentLtp);
			
			String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(this.baseDelta, this.optimalHedgeDistance);

			ceStraddleOptionName =  entryStraddleOptionNames[0];
			float cePrice = getPriceFromTicks(ceStraddleOptionName);
			fileLogTelegramWriter.write( " Entering ="+ceStraddleOptionName +"(@"+cePrice+")");
			// Place order
			long orderId = createAlgoSellOrder(ceStraddleOptionName, cePrice, noOfLots*lotSize);
			if (this.placeActualOrder) {
				placeRealOrder(orderId, ceStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
			}
			ceOptions.add(new OptionPosition(orderId, ceStraddleOptionName));
			
			peStraddleOptionName =  entryStraddleOptionNames[1];
			float pePrice = getPriceFromTicks(peStraddleOptionName);
			fileLogTelegramWriter.write( "Entering ="+peStraddleOptionName +"(@"+pePrice+")");
			// Place order
			orderId = createAlgoSellOrder(peStraddleOptionName, pePrice, noOfLots*lotSize);
			if (this.placeActualOrder) {				
				placeRealOrder(orderId, peStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
			}
			peOptions.add(new OptionPosition(orderId, peStraddleOptionName));
			
			float ceLegExitAt = this.instrumentLtp + rollingIndexPts;
			float peLegExitAt = this.instrumentLtp - rollingIndexPts;
			
			float upperAdjustLevel =  this.instrumentLtp + rollingIndexPts;
			float lowerAdjustLevel =  this.instrumentLtp - rollingIndexPts;
			
			float maxProfitReached = 0f;
			Date maxProfitReachedAt = getCurrentTime();
			float maxLowestpointReached = 0f;
			Date maxLowestpointReachedAt = getCurrentTime();
			float maxTrailingProfit = 0f;
			
			updateAlgoStatus("Running");

			do {
				sleep(5); // Quick to react
				
				this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
				
				for(OptionPosition optionPosition: ceOptions) {
					OptionGreek optionGreeks = getOptionGreeks(optionPosition.optionname);
					updateCurrentOrderBuyPrice(optionPosition.optionname, optionPosition.orderId, optionGreeks.getLtp());
				}
				for(OptionPosition optionPosition: peOptions) {
					OptionGreek optionGreeks = getOptionGreeks(optionPosition.optionname);
					updateCurrentOrderBuyPrice(optionPosition.optionname, optionPosition.orderId, optionGreeks.getLtp());
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
				fileLogTelegramWriter.write( " instrumentLtp=" + this.instrumentLtp +" currentProfit="+currentProfitPerUnit+" upperAdjustLevel="+upperAdjustLevel+" lowerAdjustLevel="+lowerAdjustLevel+" ceOptions="+ceOptions.size()+" peOptions="+peOptions.size());
				
				if (this.instrumentLtp > upperAdjustLevel || this.instrumentLtp < lowerAdjustLevel) {
					fileLogTelegramWriter.write( "Level breached, Need adjustment");
				}
				
				if (this.instrumentLtp > upperAdjustLevel) { // Exit CE
					OptionPosition position2Exit = ceOptions.get(ceOptions.size()-1);
					fileLogTelegramWriter.write( " Exiting ="+position2Exit.optionname );
					if (this.placeActualOrder) {
						placeRealOrder(position2Exit.orderId, position2Exit.optionname, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
					}
					updateCurrentOrderStatus(position2Exit.optionname, position2Exit.orderId, "LegClosed");
					ceOptions.remove(ceOptions.size()-1);
					
					if (peOptions.size() >=3) {
						position2Exit = peOptions.get(0);
						fileLogTelegramWriter.write( " Exiting ="+position2Exit.optionname );
						if (this.placeActualOrder) {
							placeRealOrder(position2Exit.orderId, position2Exit.optionname, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						updateCurrentOrderStatus(position2Exit.optionname, position2Exit.orderId, "LegClosed");
						peOptions.remove(0);
					}
					if (this.noOfOrders<maxAllowedNoOfOrders) {
						entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(this.baseDelta, this.optimalHedgeDistance);
	
						ceStraddleOptionName =  entryStraddleOptionNames[0];
						cePrice = getPriceFromTicks(ceStraddleOptionName);
						fileLogTelegramWriter.write( " Entering ="+ceStraddleOptionName +"(@"+cePrice+")");
						// Place order
						orderId = createAlgoSellOrder(ceStraddleOptionName, cePrice, noOfLots*lotSize);
						if (this.placeActualOrder) {
							placeRealOrder(orderId, ceStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						ceOptions.add(new OptionPosition(orderId, ceStraddleOptionName));
						
						peStraddleOptionName =  entryStraddleOptionNames[1];
						pePrice = getPriceFromTicks(peStraddleOptionName);
						fileLogTelegramWriter.write( "Entering ="+peStraddleOptionName +"(@"+pePrice+")");
						// Place order
						orderId = createAlgoSellOrder(peStraddleOptionName, pePrice, noOfLots*lotSize);
						if (this.placeActualOrder) {				
							placeRealOrder(orderId, peStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						peOptions.add(new OptionPosition(orderId, peStraddleOptionName));
					} else {
						prepareExit("Too many orders");
					}
					
					upperAdjustLevel =  this.instrumentLtp + rollingIndexPts;
					lowerAdjustLevel =  this.instrumentLtp - rollingIndexPts;
				} else if (this.instrumentLtp < lowerAdjustLevel) {
					OptionPosition position2Exit = peOptions.get(peOptions.size()-1);
					fileLogTelegramWriter.write( " Exiting ="+position2Exit.optionname );
					if (this.placeActualOrder) {
						placeRealOrder(position2Exit.orderId, position2Exit.optionname, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
					}
					updateCurrentOrderStatus(position2Exit.optionname, position2Exit.orderId, "LegClosed");
					peOptions.remove(peOptions.size()-1);
					
					if (ceOptions.size() >=3) {
						position2Exit = ceOptions.get(0);
						fileLogTelegramWriter.write( " Exiting ="+position2Exit.optionname );
						if (this.placeActualOrder) {
							placeRealOrder(position2Exit.orderId, position2Exit.optionname, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						updateCurrentOrderStatus(position2Exit.optionname, position2Exit.orderId, "LegClosed");
						ceOptions.remove(0);
					}
					if (this.noOfOrders<maxAllowedNoOfOrders) {
						entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(this.baseDelta, this.optimalHedgeDistance);
	
						ceStraddleOptionName =  entryStraddleOptionNames[0];
						cePrice = getPriceFromTicks(ceStraddleOptionName);
						fileLogTelegramWriter.write( " Entering ="+ceStraddleOptionName +"(@"+cePrice+")");
						// Place order
						orderId = createAlgoSellOrder(ceStraddleOptionName, cePrice, noOfLots*lotSize);
						if (this.placeActualOrder) {
							placeRealOrder(orderId, ceStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						ceOptions.add(new OptionPosition(orderId, ceStraddleOptionName));
						
						peStraddleOptionName =  entryStraddleOptionNames[1];
						pePrice = getPriceFromTicks(peStraddleOptionName);
						fileLogTelegramWriter.write( "Entering ="+peStraddleOptionName +"(@"+pePrice+")");
						// Place order
						orderId = createAlgoSellOrder(peStraddleOptionName, pePrice, noOfLots*lotSize);
						if (this.placeActualOrder) {				
							placeRealOrder(orderId, peStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
						}
						peOptions.add(new OptionPosition(orderId, peStraddleOptionName));
					} else {
						prepareExit("Too many orders");
					}
						
					upperAdjustLevel =  this.instrumentLtp + rollingIndexPts;
					lowerAdjustLevel =  this.instrumentLtp - rollingIndexPts;
				}
				
				checkExitSignals();
					
				saveAlgoDailySummary(currentProfitPerUnit, maxProfitReached, maxProfitReachedAt, maxLowestpointReached, maxLowestpointReachedAt, maxTrailingProfit);
			} while(!exitThread);
			updateAlgoStatus("Terminated");
			String logString = "Exiting Strddle ceStraddleOptionName="+ceStraddleOptionName + " peStraddleOptionName="+peStraddleOptionName; 
			log.info(logString);
			fileLogTelegramWriter.write( " " + logString);
			// exit all positions
			//if (this.placeActualOrder) exitStraddle(ceDbId, peDbId);
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
		return retVal/3f;
	}
	
}
