package com.nexcorio.algo.strategy;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.util.KiteUtil;
import com.nexcorio.algo.util.db.HDataSource;

public class G3DualIVParityAndOIWorthStraddleAlgoThread extends G3BaseClass implements Runnable{

	private static final Logger log = LogManager.getLogger(G3DualIVParityAndOIWorthStraddleAlgoThread.class);
	
	public float baseDelta = 0.6f;
	
	public float ivDiffCutoffPercent = 5f;
	public boolean filterOptionWorth = false;
	public int topOis = 7;
	
	public G3DualIVParityAndOIWorthStraddleAlgoThread(Long napAlgoId, String backTestDateStr) {
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
			
			if (this.placeActualOrder) setLotBasedonAvailableMarginHalfStraddle();
			
			printFields(this);
			
			long ceDbId = -1;
			long peDbId = -1;
			
			this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
			
			fileLogTelegramWriter.write( " this.instrumentLtp="+this.instrumentLtp);
			
			String lastKnownTrendbyIV = "Unknown";
			String lastKnownTrendbyOIWorth = "Unknown";
			String lastKnownMergedTrend = "Unknown";
			
			float maxProfitReached = 0f;
			Date maxProfitReachedAt = getCurrentTime();
			float maxLowestpointReached = 0f;
			Date maxLowestpointReachedAt = getCurrentTime();
			float maxTrailingProfit = 0f;
			
			updateAlgoStatus("Running");
			
			do {
				sleep(15); // Every 10sec
				
				this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
				
				OptionGreek ceOptionGreeks = getOptionGreeks(ceStraddleOptionName);
				OptionGreek peOptionGreeks = getOptionGreeks(peStraddleOptionName);
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
				fileLogTelegramWriter.write( " instrumentLtp=" + this.instrumentLtp +" ******* currentProfit="+currentProfitPerUnit+" ******* ");
				
				String currentTrendByIV = getSellerDirectionByATMIVParity(lastKnownTrendbyIV);
				String currentTrendByOIWorth = getSellerDirectionByOIWorth(lastKnownTrendbyOIWorth);
				
				String mergedTrend = "Ambiguity";
				
				if (currentTrendByIV.equals("CE") && currentTrendByOIWorth.equals("CE")) mergedTrend = "CE";
				else if (currentTrendByIV.equals("PE") && currentTrendByOIWorth.equals("PE")) mergedTrend = "PE";
				
				fileLogTelegramWriter.write( " currentTrendByIV="+currentTrendByIV+" currentTrendByOIWorth=" + currentTrendByOIWorth + " mergedTrend="+mergedTrend);
				
				if (!mergedTrend.equals(lastKnownMergedTrend)) {
				
					String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(baseDelta, this.hedgeDistance);
					
					if (mergedTrend.equals("CE")) { // Exit PE, Enter CE
						if (!peStraddleOptionName.equals("")) { // Exit PE, taking Directional
							fileLogTelegramWriter.write( " Exiting ="+peStraddleOptionName );
							// Exit PE
							if (this.placeActualOrder) {
								placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
							peStraddleOptionName = "";
						}
						if (!ceStraddleOptionName.equals(entryStraddleOptionNames[0])) {
							if (!ceStraddleOptionName.equals("")) { // Exit and re enter
								fileLogTelegramWriter.write( " Exiting ="+ceStraddleOptionName );
								// Exit CE
								if (this.placeActualOrder) {
									placeRealOrder( ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
								}
								ceStraddleOptionName = "";
							}
							if (this.noOfOrders<maxAllowedNoOfOrders) {
								ceStraddleOptionName =  entryStraddleOptionNames[0];
								float cePrice = getPriceFromTicks(ceStraddleOptionName);
								fileLogTelegramWriter.write( " Entering ="+ceStraddleOptionName +"(@"+cePrice+")");
								// Place order
								ceDbId = createAlgoSellOrder(ceStraddleOptionName, cePrice, noOfLots*lotSize);
								if (this.placeActualOrder) {
									if (ceHedgeOptionName.equals("")) {								
										ceHedgeOptionName =  entryStraddleOptionNames[2];
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
					} else if (mergedTrend.equals("PE")) { // Exit CE, Enter PE
						if (!ceStraddleOptionName.equals("")) { // Exit CE, taking Directional
							fileLogTelegramWriter.write( " Exiting ="+ceStraddleOptionName );
							// Exit CE
							if (this.placeActualOrder) {
								placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
							ceStraddleOptionName = "";
						}
						if (!peStraddleOptionName.equals(entryStraddleOptionNames[1])) {
							if (!peStraddleOptionName.equals("")) { // Exit and re enter
								fileLogTelegramWriter.write( " Exiting ="+peStraddleOptionName );
								if (this.placeActualOrder) {
									placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
								}
								peStraddleOptionName = "";
							}
							if (this.noOfOrders<maxAllowedNoOfOrders) {
								peStraddleOptionName =  entryStraddleOptionNames[1];
								float pePrice = getPriceFromTicks(peStraddleOptionName);
								fileLogTelegramWriter.write( "Entering ="+peStraddleOptionName +"(@"+pePrice+")");
								// Place order
								peDbId = createAlgoSellOrder(peStraddleOptionName, pePrice, noOfLots*lotSize);
								if (this.placeActualOrder) {
									if (peHedgeOptionName.equals("")) {
										peHedgeOptionName =  entryStraddleOptionNames[3];
										placeRealOrder( peHedgeOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
									}
									placeRealOrder( peDbId, peStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
								}
							} else {
								prepareExit("Too many orders");
							}
						} else {
							fileLogTelegramWriter.write( " Retaining ="+peStraddleOptionName);
						}
					} else { // Ambiguity - Exit both CE & PE
						fileLogTelegramWriter.write( " Forming Straddle");
						
						entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(baseDelta, this.hedgeDistance);
						
						if (peStraddleOptionName.equals("")) {
							if (this.noOfOrders<maxAllowedNoOfOrders) {
								peStraddleOptionName =  entryStraddleOptionNames[1];
								float pePrice = getPriceFromTicks(peStraddleOptionName);
								fileLogTelegramWriter.write( "Entering ="+peStraddleOptionName +"(@"+pePrice+")");
								// Place order
								peDbId = createAlgoSellOrder(peStraddleOptionName, pePrice, noOfLots*lotSize);
								if (this.placeActualOrder) {
									if (peHedgeOptionName.equals("")) {
										peHedgeOptionName =  entryStraddleOptionNames[3];
										placeRealOrder( peHedgeOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
									}
									placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
								}
							} else {
								prepareExit("Too many orders");
							}
						}
						
						if (ceStraddleOptionName.equals("")) {
							if (this.noOfOrders<maxAllowedNoOfOrders) {
								ceStraddleOptionName =  entryStraddleOptionNames[0];
								float cePrice = getPriceFromTicks(ceStraddleOptionName);
								fileLogTelegramWriter.write( " Entering ="+ceStraddleOptionName +"(@"+cePrice+")");
								// Place order
								ceDbId = createAlgoSellOrder(ceStraddleOptionName, cePrice, noOfLots*lotSize);
								if (this.placeActualOrder) {
									if (ceHedgeOptionName.equals("")) {								
										ceHedgeOptionName =  entryStraddleOptionNames[2];
										placeRealOrder( ceHedgeOptionName, noOfLots*lotSize, "BUY",  true, KiteUtil.USE_NORMAL_ORDER_FALSE);
									}
									placeRealOrder( ceDbId, ceStraddleOptionName, noOfLots*lotSize, "SELL",  false, KiteUtil.USE_NORMAL_ORDER_FALSE);
								}
							} else {
								prepareExit("Too many orders");
							}
						}
					}
				}
				
				lastKnownMergedTrend = mergedTrend;
				lastKnownTrendbyIV = currentTrendByIV;
				lastKnownTrendbyOIWorth = currentTrendByOIWorth;
				
				checkExitSignals();
				
				if ( (runningCePrice+runningPePrice)>0 && (runningCePrice+runningPePrice)<10f ) {
					prepareExit( "Nothing much left in premium");
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
	
	private String getSellerDirectionByOIWorth(String lastKnownTrend) {
		String retVal = lastKnownTrend;
		
		Connection conn = null;
		String top4Options ="";
		try {
			conn = HDataSource.getConnection();
			Statement stmt = conn.createStatement();
			
			String optionnamePrefix = getCurrentWeekExpiryOptionnamePrefix();
			
			float ceOIWorth = 0f;
			float peOIWorth = 0f;
			
			if (backtestDate == null) {
				String opOIFetch = "select trading_symbol, oi as open_interest, oi*ltp/10000000 as worthInCr from nexcorio_option_snapshot where trading_symbol like '" + optionnamePrefix + "%' and record_date = '" + postgresShortDateFormat.format(getCurrentTime()) +"' "
						+ (filterOptionWorth==true?" and oi*ltp/10000000>10":"")  + " order by oi desc limit "+this.topOis;
				
				fileLogTelegramWriter.write("opOIFetch="+opOIFetch);
				ResultSet rs = stmt.executeQuery(opOIFetch);
				
				float ceOICount = 0;
				float peOICount = 0;
				
				while (rs.next()) {
					String tradingSymbol = rs.getString("trading_symbol");
					float worthInCr = rs.getFloat("worthInCr");
					float openInterest = rs.getFloat("open_interest");
					top4Options = top4Options + tradingSymbol +" ";
					
					if (tradingSymbol.endsWith("CE")) {
						ceOIWorth = ceOIWorth + worthInCr;
						ceOICount = ceOICount + openInterest;
					} else {
						peOIWorth = peOIWorth + worthInCr;
						peOICount = peOICount + openInterest;
					}
				}
				rs.close();
			} else {
				Calendar intCal = Calendar.getInstance();
				intCal.setTime(backtestDate.getTime());
				intCal.set(Calendar.SECOND, 0);
				//log.info("intCal="+intCal);
				String opOIFetch = "select trading_symbol, oi as open_interest, oi*ltp/10000000 as worthInCr "
						+ " from nexcorio_option_greeks where trading_symbol like '" + optionnamePrefix + "%'"
						
						+ " and quote_time <= '"+ postgresLongDateFormat.format(getCurrentTime()) + "'"	
						+ " and quote_time >  '"+ postgresLongDateFormat.format(getCurrentTime(-1)) + "'"
						
						+ " and oi*ltp/10000000>10" + " order by quote_time desc ";
				
				fileLogTelegramWriter.write(" opOIFetch="+opOIFetch);
				
				List<String> symbols = new ArrayList<String>();
				List<Float> ois = new ArrayList<Float>();
				ResultSet rs = stmt.executeQuery(opOIFetch);
				int recCount = 0;
				while (rs.next()) {
					symbols.add(rs.getString("trading_symbol") );
					ois.add(rs.getFloat("open_interest") );
					top4Options = top4Options + symbols.get(recCount) +" "; 
					//fileLogTelegramWriter.write( symbols.get(recCount) + ", oi=" + ois.get(recCount) );
					recCount++;
				}
				rs.close();
				// Remove the duplicates from the bottom
				
				for(int bottomPt = ois.size()-1;bottomPt>0;bottomPt--) {
					for(int topPt = 0;topPt<bottomPt;topPt++) {
						if (symbols.get(bottomPt).equals(symbols.get(topPt))) {
							//fileLogTelegramWriter.write("Removing duplicate " + symbols.get(bottomPt) + ", oi=" + ois.get(bottomPt) );
							ois.remove(bottomPt);
							symbols.remove(bottomPt);
							break;
						}
					}
				}
				// Sort by OI
				for(int i=0;i<ois.size()-1;i++) {
					for(int j=i+1;j<ois.size();j++) {
						if ( ois.get(j) > ois.get(i) ) {
							String swapObj = symbols.get(i);
							symbols.set(i, symbols.get(j));
							symbols.set(j, swapObj);
							
							Float swapNum = ois.get(i);
							ois.set(i, ois.get(j));
							ois.set(j, swapNum);
						}
					}
				}
				
				int tillLoop = this.topOis;
				if (ois.size() < this.topOis) {
					tillLoop = ois.size();
				}
				
				for(int i=0;i<tillLoop;i++) {
					//fileLogTelegramWriter.write(i + ". " + symbols.get(i) + ", oi=" + ois.get(i) );
					if (symbols.get(i).endsWith("CE")) {
						ceOIWorth = ceOIWorth + ois.get(i);
					} else {
						peOIWorth = peOIWorth + ois.get(i);
					}
				}				
			}
			stmt.close();
			
			if (ceOIWorth-peOIWorth>10) {
				retVal = "CE";
			} else if (peOIWorth-ceOIWorth>10) {
				retVal = "PE";
			} else {
				retVal = lastKnownTrend;
			}
			
			if (retVal.equals("Unknown") ) {
				if (ceOIWorth > peOIWorth) {
					retVal = "CE";
				} else {
					retVal = "PE";
				}
			}
			String logString = " ceOIWorth="+ceOIWorth+" peOIWorth="+peOIWorth +" retVal="+retVal+" topOptions="+top4Options;
			fileLogTelegramWriter.write( logString);
		} catch (Exception e) {
			log.error("Error"+e.getMessage(),e);
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return retVal;
	}
	
	private String getSellerDirectionByATMIVParity( String lastKnownTrend) {
		String retVal = lastKnownTrend;
		
		Connection conn = null;
		try {
			conn = HDataSource.getConnection();
			Statement stmt = conn.createStatement();
			
			String fetchSql = "select ceiv as ceGreek, peiv as peGreek from nexcorio_option_atm_movement_data where f_main_instrument = " + this.mainInstrument.getId() + ""
					+ " and record_time <= '" + postgresLongDateFormat.format(getCurrentTime()) + "'"
					+ " order by record_time desc limit 1";
			fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
			ResultSet rs = stmt.executeQuery(fetchSql);
			
			float ceGreek = 0f;
			float peGreek = 0f;
			while (rs.next()) {
				ceGreek = rs.getFloat("ceGreek");
				peGreek = rs.getFloat("peGreek");
			}
			rs.close();			
			stmt.close();
			
			float greekDiffPercent = getPercentDiff(ceGreek, peGreek);
			if (ceGreek < peGreek)
				greekDiffPercent = -greekDiffPercent;
			
			fileLogTelegramWriter.write("ceGreek="+ceGreek + " peGreek="+peGreek + " greekDiffPercent="+greekDiffPercent);
			
			if (greekDiffPercent >= ivDiffCutoffPercent || greekDiffPercent <= -ivDiffCutoffPercent ) {
				fileLogTelegramWriter.write("Cutoff breached");
				if (greekDiffPercent > 0f) {
					retVal = "PE";
				} else {
					retVal = "CE";
				}
			}
			
			if (retVal.equals("Unknown") ) {
				if (greekDiffPercent > 0f) {
					retVal = "PE";
				} else {
					retVal = "CE";
				}
			}
			
		} catch(Exception ex) {
			log.error("Error"+ex.getMessage(),ex);
			ex.printStackTrace();
		}finally {
			try {
				if (conn!=null) conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}	
		return retVal;
	}
	
	public static void main(String[] args) {
		new G3DualIVParityAndOIWorthStraddleAlgoThread(353L, null);
	}

	

}
