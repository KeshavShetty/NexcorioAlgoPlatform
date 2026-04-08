package com.nexcorio.algo.strategy;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.util.KiteUtil;
import com.nexcorio.algo.util.db.HDataSource;

public class G3OutlierBiasedRollingStrangleAlgoThread extends G3BaseClass implements Runnable {

	private static final Logger log = LogManager.getLogger(G3PriceParityIVBasedAlgoThread.class);
	
	public float callDelta = 0.5f;
	public float  putDelta = 0.4f;
	
	public float  indexRollingPts = 50f;
	
	public boolean maintainBias = false;
	public float oppositeDeltaDiff= 0.2f;
		
	public G3OutlierBiasedRollingStrangleAlgoThread(Long napAlgoId, String backTestDateStr) {
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
			long ceDbId = -1;
			long peDbId = -1;
						
			this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
			
			fileLogTelegramWriter.write( " this.instrumentLtp="+this.instrumentLtp);
			
			printFields(this);
			
			float maxProfitReached = 0f;
			Date maxProfitReachedAt = getCurrentTime();
			float maxLowestpointReached = 0f;
			Date maxLowestpointReachedAt = getCurrentTime();
			float maxTrailingProfit = 0f;
			
			updateAlgoStatus("Running");
			String trend = "Neutral";
			float indexWhenStrangleFormed = this.instrumentLtp;
			String lastknowntrend = "CE";
			do {
				sleep(5); // Quick to react
				
				this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
				
				OptionGreek ceOptionGreeks = !ceStraddleOptionName.equals("")?getOptionGreeks(ceStraddleOptionName):null;
				OptionGreek peOptionGreeks = !peStraddleOptionName.equals("")?getOptionGreeks(peStraddleOptionName):null;
				print(ceOptionGreeks, peOptionGreeks);
				
				float runningCePrice = ceOptionGreeks==null?0: ceOptionGreeks.getLtp();
				float runningPePrice = peOptionGreeks==null?0: peOptionGreeks.getLtp();
				
				if (!ceStraddleOptionName.equals("")) updateCurrentOrderBuyPrice(ceStraddleOptionName, ceDbId, runningCePrice);
				if (!peStraddleOptionName.equals("")) updateCurrentOrderBuyPrice(peStraddleOptionName, peDbId, runningPePrice);
				
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
				
				
				String sellerDirection = getSellerDirectionByATMGreekGap(lastknowntrend);
				if (sellerDirection.equals("CE")) trend = "Bearish";
				else if (sellerDirection.equals("PE")) trend = "Bullish";
				else trend = "Neutral";
				
				boolean needRepositioning = false;
				if (ceStraddleOptionName.equals("")) {
					needRepositioning = true; // Just starting, no open positions
				} else if (this.instrumentLtp > indexWhenStrangleFormed + indexRollingPts || this.instrumentLtp < indexWhenStrangleFormed - indexRollingPts) {
					fileLogTelegramWriter.write("Realigning " + indexRollingPts + " pt range broken. indexWhenStrangleFormed="+indexWhenStrangleFormed+" index now at "+this.instrumentLtp);
					needRepositioning = true;
				} 
				if (!sellerDirection.equals(lastknowntrend)) {
					needRepositioning = true;
				}
				if (needRepositioning==false && this.maintainBias == true) { // && Math.abs(ceOptionGreeks.getDelta()) < Math.abs(peOptionGreeks.getDelta())
					if (trend.equals("Bullish") && Math.abs(ceOptionGreeks.getDelta()) > Math.abs(peOptionGreeks.getDelta()) ) {
						fileLogTelegramWriter.write("Realigning maintainBias breached when bullis");
						needRepositioning = true;
					} else if (trend.equals("Bearish") && Math.abs(ceOptionGreeks.getDelta()) < Math.abs(peOptionGreeks.getDelta())) {
						fileLogTelegramWriter.write("Realigning maintainBias breached when bearish");
						needRepositioning = true;
					}
					
				} 
				if(needRepositioning==false && this.oppositeDeltaDiff > 0f) {
					float originalDeltaGap = Math.abs(callDelta-putDelta);
					float currentGap = ceOptionGreeks.getDelta() - Math.abs(peOptionGreeks.getDelta());
					if(trend.equals("Bullish")) {
						currentGap = Math.abs(peOptionGreeks.getDelta()) - ceOptionGreeks.getDelta();
					}
					if (currentGap > originalDeltaGap+oppositeDeltaDiff) { // 0.15f
						fileLogTelegramWriter.write("Realigning oppositeDeltaDiff breached. currentGap="+currentGap);
						needRepositioning = true;
//						if(trend.equals("Bullish")) trend = "Bearish";
//						else trend = "Bullish";
					}
				}
				
				if (needRepositioning) {
					if (trend.equals("Bullish")) {
						this.callDelta = 0.4f;
						this.putDelta  = 0.6f;
					} else if (trend.equals("Bearish")) {
						this.callDelta = 0.6f;
						this.putDelta  = 0.4f;
					} else {
						this.callDelta = 0.5f;
						this.putDelta  = 0.5f;
					}
					
					String[] entryStraddleOptionNames1 = getStraddleOptionNamesByDeltaOptimised(this.callDelta, this.optimalHedgeDistance); // getStraddleOptionNamesByGreekOptimised("ltp", this.baseDelta, this.optimalHedgeDistance);
					String[] entryStraddleOptionNames2 = getStraddleOptionNamesByDeltaOptimised(this.putDelta , 0);
					
					String ceOptionname = entryStraddleOptionNames1[0];
					
					if (!ceStraddleOptionName.equals(ceOptionname)) {
						if (!ceStraddleOptionName.equals("")) { // Exit and re enter
							fileLogTelegramWriter.write( " Exiting ="+ceStraddleOptionName );
							// Exit CE
							if (this.placeActualOrder) {
								placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
							ceStraddleOptionName = "";
						}
						if (this.noOfOrders<maxAllowedNoOfOrders) {
							ceStraddleOptionName =  ceOptionname;
							float cePrice = getPriceFromTicks(ceStraddleOptionName);
							fileLogTelegramWriter.write( " Entering ="+ceStraddleOptionName +"(@"+cePrice+")");
							// Place order
							ceDbId = createAlgoSellOrder(ceStraddleOptionName, cePrice, noOfLots*lotSize);
							if (this.placeActualOrder) {
								if (ceHedgeOptionName.equals("")) {								
									ceHedgeOptionName =  entryStraddleOptionNames1[2];
									placeRealOrder(ceHedgeOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
								}
								placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
						} else {
							prepareExit("Too many orders");
						}
					} else {
						fileLogTelegramWriter.write( " Retaining ="+ceStraddleOptionName);
					}
					
					String peOptionname = entryStraddleOptionNames2[1];
					if (!peStraddleOptionName.equals(peOptionname)) {
						if (!peStraddleOptionName.equals("")) { // Exit and re enter
							fileLogTelegramWriter.write( " Exiting ="+peStraddleOptionName );
							if (this.placeActualOrder) {
								placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
							peStraddleOptionName = "";
						}
						if (this.noOfOrders<maxAllowedNoOfOrders) {
							peStraddleOptionName =  peOptionname;
							float pePrice = getPriceFromTicks(peStraddleOptionName);
							fileLogTelegramWriter.write( "Entering ="+peStraddleOptionName +"(@"+pePrice+")");
							// Place order
							peDbId = createAlgoSellOrder(peStraddleOptionName, pePrice, noOfLots*lotSize);
							if (this.placeActualOrder) {
								if (peHedgeOptionName.equals("")) {
									peHedgeOptionName =  entryStraddleOptionNames1[3];
									placeRealOrder(peHedgeOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
								}
								placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
						} else {
							prepareExit("Too many orders");
						}
					} else {
						fileLogTelegramWriter.write( " Retaining ="+peStraddleOptionName);
					}
					indexWhenStrangleFormed = this.instrumentLtp;
				}
				lastknowntrend = sellerDirection;
				
				if ( (runningCePrice+runningPePrice)>0 && (runningCePrice+runningPePrice)<10f ) {
					prepareExit( "Nothing much left in premium");
				}
				
				checkExitSignals();
				
				if (exitThread==true) {
					if (!ceStraddleOptionName.equals("")) {
						updateCurrentOrderStatus(ceStraddleOptionName, ceDbId, "LegClosed");
					} 
					if (!peStraddleOptionName.equals("")) {
						updateCurrentOrderStatus(peStraddleOptionName, peDbId, "LegClosed");
					}
				}	
				saveAlgoDailySummary(currentProfitPerUnit, maxProfitReached, maxProfitReachedAt, maxLowestpointReached, maxLowestpointReachedAt, maxTrailingProfit);
			} while(!exitThread);
			updateAlgoStatus("Terminated");
			String logString = "Exiting Strddle ceStraddleOptionName="+ceStraddleOptionName + " peStraddleOptionName="+peStraddleOptionName; 
			log.info(logString);
			fileLogTelegramWriter.write( " " + logString);
			// exit all positions
			if (this.placeActualOrder) exitStraddle(ceDbId, peDbId);
			fileLogTelegramWriter.write( " noOfOrders="+noOfOrders + " ROI=" + (currentProfitPerUnit*this.lotSize*100f)/requiredMargin + "% (Max profit/lot reached to "+ (maxProfitReached) +"@" + maxProfitReachedAt+ "\n and Lowest reached to " + (maxLowestpointReached) + "@" + maxLowestpointReachedAt + ")");
			
		} catch (Exception e) {			
			updateAlgoStatus("Error");
			log.error("Error"+e.getMessage(), e);
			fileLogTelegramWriter.write("Error " + ExceptionUtils.getStackTrace(e));
		} finally {
			fileLogTelegramWriter.close();
		}
	}
	
	private String getSellerDirectionByATMGreekGap(String lastknowntrend) {
		String retVal = lastknowntrend;
		
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			String fieldname = "countceoutlier as ceGreek, countpeoutlier as peGreek";
			
			String fetchSql = "select " + fieldname + " from nexcorio_option_atm_movement_data where f_main_instrument = " + this.mainInstrument.getId() + ""
					+ " and record_time <= '" + postgresLongDateFormat.format(getCurrentTime()) + "'"
					+ " order by record_time desc limit 5";
			fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
			ResultSet rs = stmt.executeQuery(fetchSql);
			
			int ceGapCount = 0;
			int peGapCount = 0;
			
			while (rs.next()) {
				float ceGreek = rs.getFloat("ceGreek");
				float peGreek = rs.getFloat("peGreek");
				ceGapCount = (int) (ceGapCount + ceGreek);
				peGapCount = (int) (peGapCount + peGreek);
			}
			rs.close();			
			stmt.close();
			
			//fileLogTelegramWriter.write("ceGapCount="+ceGapCount+" peGapCount="+peGapCount);
			
			if (ceGapCount > 12 && ceGapCount >= peGapCount*3) {
				retVal = "CE";
			} else if (peGapCount > 12 && peGapCount >= ceGapCount*3) {
				retVal = "PE";
			} else {
				retVal = "Neutral";
			}
			fileLogTelegramWriter.write("ceGapCount="+ceGapCount+" peGapCount="+peGapCount+" retVal="+retVal);
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
	
}
