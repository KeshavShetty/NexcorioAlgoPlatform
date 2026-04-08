package com.nexcorio.algo.strategy;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.util.KiteUtil;
import com.nexcorio.algo.util.db.HDataSource;

public class G0PlaybookGapAlgoThread extends G3BaseClass implements Runnable{

	private static final Logger log = LogManager.getLogger(G0PlaybookGapAlgoThread.class);
	
	public float baseDelta = 0.5f;
	public float greekDiffPct = 30f;
	public float maxLossPerStraddle = -0f;
	
	public boolean wait4IdealPremium = Boolean.FALSE;
	
	private float idealPremium = 0f;
	
	public G0PlaybookGapAlgoThread(Long napAlgoId, String backTestDateStr) {
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
			
			long ceDbId = -1;
			long peDbId = -1;
			
			this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
			fileLogTelegramWriter.write( " this.instrumentLtp="+this.instrumentLtp);
			
			float maxProfitReached = 0f;
			Date maxProfitReachedAt = getCurrentTime();
			float maxLowestpointReached = 0f;
			Date maxLowestpointReachedAt = getCurrentTime();
			float maxTrailingProfit = 0f;
			
			updateAlgoStatus("Running");
			
			String lastKnownTrend = "Unknown";
			do {
				sleep(5); // Every 10sec
				
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
				fileLogTelegramWriter.write( " instrumentLtp=" + this.instrumentLtp +" currentProfit="+currentProfitPerUnit+" maxLowestpointReachedPerUnit="+(maxLowestpointReached)+" maxTrailingProfit="+maxTrailingProfit);
				
				String currentTrend = getSellerDirectionByATMGreekGap(lastKnownTrend);
				
				if (!currentTrend.equals(lastKnownTrend)) {
					
					String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(baseDelta, this.optimalHedgeDistance);
					
					if (currentTrend.equals("CE")) { // Exit PE, Enter CE
						if (!peStraddleOptionName.equals("")) { // Exit PE, taking Directional
							fileLogTelegramWriter.write( " Exiting ="+peStraddleOptionName );
							// Exit PE
							if (this.placeActualOrder) {
								placeRealOrder( peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
							peStraddleOptionName = "";
						}
						if (!ceStraddleOptionName.equals(entryStraddleOptionNames[0])) {
							if (!ceStraddleOptionName.equals("")) { // Exit and re enter
								fileLogTelegramWriter.write( " Exiting ="+ceStraddleOptionName );
								// Exit CE
								if (this.placeActualOrder) {
									placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
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
					} else if (currentTrend.equals("PE")) { // Exit CE, Enter PE
						if (!ceStraddleOptionName.equals("")) { // Exit CE, taking Directional
							fileLogTelegramWriter.write( " Exiting ="+ceStraddleOptionName );
							// Exit CE
							if (this.placeActualOrder) {
								placeRealOrder( ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
							}
							ceStraddleOptionName = "";
						}
						if (!peStraddleOptionName.equals(entryStraddleOptionNames[1])) {
							if (!peStraddleOptionName.equals("")) { // Exit and re enter
								fileLogTelegramWriter.write( " Exiting ="+peStraddleOptionName );
								if (this.placeActualOrder) {
									placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
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
					} 
					lastKnownTrend = currentTrend;
				}
				
//				boolean needAlignment = false;
//				
//				if (ceStraddleOptionName.equals("")) { // no existing position
//					needAlignment = true;
//				} else {
//					float profitFromCurrentStradle = premiumWhenSold - (ceOptionGreeks.getLtp() + peOptionGreeks.getLtp());
//					if (profitFromCurrentStradle > maxProfitInThisTraddle) {
//						maxProfitInThisTraddle = profitFromCurrentStradle;
//					}
//					
//					if (profitFromCurrentStradle < -maxLossPerStraddle) {
//						needAlignment = true;
//					}
//					
//					fileLogTelegramWriter.write( "profitFromCurrentStradle="+profitFromCurrentStradle+" premiumWhenSold="+premiumWhenSold);
//					
//					if (Math.abs(ceOptionGreeks.getDelta()) - ceDeltaWhenFormed > 0.05f) {
//						needAlignment = true;
//					} else if (Math.abs(peOptionGreeks.getDelta()) - peDeltaWhenFormed > 0.05f) {
//						needAlignment = true;
//					}
//					fileLogTelegramWriter.write( "PE Delta diff="+(peDeltaWhenFormed - Math.abs(peOptionGreeks.getDelta()) ) +" peDeltaWhenFormed="+peDeltaWhenFormed);
//					if (ceDeltaWhenFormed - Math.abs(ceOptionGreeks.getDelta()) > 0.08f) {
//						needAlignment = true;
//					} else if (peDeltaWhenFormed - Math.abs(peOptionGreeks.getDelta())  > 0.08f) {
//						needAlignment = true;
//					}
//					
//				}
//				
//				if (needAlignment) {
//					String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised( baseDelta, this.optimalHedgeDistance);
//					if (!ceStraddleOptionName.equals(entryStraddleOptionNames[0])) {
//						if (!ceStraddleOptionName.equals("")) { // Exit and re enter
//							fileLogTelegramWriter.write( " Exiting ="+ceStraddleOptionName );
//							// Exit CE
//							if (this.placeActualOrder) {
//								placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
//							}
//							ceStraddleOptionName = "";
//						}
//						if (this.noOfOrders<maxAllowedNoOfOrders) {
//							ceStraddleOptionName =  entryStraddleOptionNames[0];
//							float cePrice = getPriceFromTicks(ceStraddleOptionName);
//							fileLogTelegramWriter.write( " Entering ="+ceStraddleOptionName +"(@"+cePrice+")");
//							// Place order
//							ceDbId = createAlgoSellOrder(ceStraddleOptionName, cePrice, noOfLots*lotSize);
//							if (this.placeActualOrder) {
//								if (ceHedgeOptionName.equals("")) {								
//									ceHedgeOptionName =  entryStraddleOptionNames[2];
//									placeRealOrder(ceHedgeOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
//								}
//								placeRealOrder(ceDbId, ceStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
//							}
//						} else {
//							prepareExit("Too many orders");
//						}
//					} else {
//						fileLogTelegramWriter.write( " Retaining ="+ceStraddleOptionName);
//					}
//					if (!peStraddleOptionName.equals(entryStraddleOptionNames[1])) {
//						if (!peStraddleOptionName.equals("")) { // Exit and re enter
//							fileLogTelegramWriter.write( " Exiting ="+peStraddleOptionName );
//							if (this.placeActualOrder) {
//								placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
//							}
//							peStraddleOptionName = "";
//						}
//						if (this.noOfOrders<maxAllowedNoOfOrders) {
//							peStraddleOptionName =  entryStraddleOptionNames[1];
//							float pePrice = getPriceFromTicks(peStraddleOptionName);
//							fileLogTelegramWriter.write( "Entering ="+peStraddleOptionName +"(@"+pePrice+")");
//							// Place order
//							peDbId = createAlgoSellOrder(peStraddleOptionName, pePrice, noOfLots*lotSize);
//							if (this.placeActualOrder) {
//								if (peHedgeOptionName.equals("")) {
//									peHedgeOptionName =  entryStraddleOptionNames[3];
//									placeRealOrder(peHedgeOptionName, noOfLots*lotSize, "BUY", true, KiteUtil.USE_NORMAL_ORDER_FALSE);
//								}
//								placeRealOrder(peDbId, peStraddleOptionName, noOfLots*lotSize, "SELL", false, KiteUtil.USE_NORMAL_ORDER_FALSE);
//							}
//						} else {
//							prepareExit("Too many orders");
//						}
//					} else {
//						fileLogTelegramWriter.write( " Retaining ="+peStraddleOptionName);
//					}
//					ceOptionGreeks = getOptionGreeks(ceStraddleOptionName);
//					peOptionGreeks = getOptionGreeks(peStraddleOptionName);
//					premiumWhenSold = (ceOptionGreeks!=null?ceOptionGreeks.getLtp():0f) + (peOptionGreeks!=null?peOptionGreeks.getLtp():0f);
//					maxProfitInThisTraddle = 0f;
//					ceDeltaWhenFormed = ceOptionGreeks!=null?ceOptionGreeks.getDelta():0f;
//					peDeltaWhenFormed = peOptionGreeks!=null?Math.abs(peOptionGreeks.getDelta()):0f;
//				}
//				if (!ceStraddleOptionName.equals("")) {
//					if (ceOptionGreeks.getLtp() + peOptionGreeks.getLtp() < premiumWhenSold) {
//						premiumWhenSold = ceOptionGreeks.getLtp() + peOptionGreeks.getLtp();
//					}
//				}
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
	
	private String getSellerDirectionByATMGreekGap( String lastKnownTrend) {
		String retVal = lastKnownTrend;
		
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			List<OptionGreek> optionGreeks = new ArrayList<OptionGreek>();
			
			if (this.backtestDate == null) { // Live
				optionGreeks = getSnapshotGreeksFromCache();
			} else {
				// First try to fetch from Snapshot table
				String fetchSql = "select DISTINCT(trading_symbol) as trading_symbol from nexcorio_option_snapshot"
						+ " where trading_symbol like '" + mainInstrument.getShortName() + "%' "
						+ " and record_date = '" + postgresShortDateFormat.format(getCurrentTime()) + "'";
				fileLogTelegramWriter.write("1. fetchSql="+fetchSql);
				
				List<String> optionnames = new ArrayList<>();			
				ResultSet rs = stmt.executeQuery(fetchSql);
				while (rs.next()) {
					optionnames.add(rs.getString("trading_symbol"));
				}
				rs.close();
				
				if (optionnames.size()==0) { // not found in snapshot		
					fetchSql = "select DISTINCT(trading_symbol) as trading_symbol from nexcorio_option_greeks"
							+ " where f_main_instrument = " + mainInstrument.getId() + " "
							+ " and quote_time > '" + postgresShortDateFormat.format(getCurrentTime()) + " 09:15:00'"
							+ " and quote_time < '" + postgresShortDateFormat.format(getCurrentTime()) + " 09:20:00'";
								
					rs = stmt.executeQuery(fetchSql);
					while (rs.next()) {
						optionnames.add(rs.getString("trading_symbol"));
					}
					rs.close();
					
					// Insert to snapshot
					for(String aSymbol: optionnames) {
						String insertSql = "INSERT INTO nexcorio_option_snapshot (id, trading_symbol, record_date)"
								+ " VALUES (nextval('nexcorio_option_snapshot_id_seq'),'" + aSymbol + "','" + postgresShortDateFormat.format(getCurrentTime()) + "')";
						stmt.executeUpdate(insertSql);
					}
				}
				for(String optionname:optionnames ) {
					OptionGreek aGreek = getOptionGreeks(optionname);
					if (aGreek!=null) {
						optionGreeks.add(aGreek);
					}
				}
			}
			
			StringBuffer ceBuffer = new StringBuffer();
			StringBuffer peBuffer = new StringBuffer();
			
//			Collections.sort(optionGreeks, new SortbyOiDesc());
//			
//			List<OptionGreek> ceOptionGreeks = new ArrayList<OptionGreek>();
//			List<OptionGreek> peOptionGreeks = new ArrayList<OptionGreek>();
//			
//			int recProcessed = 0;
//			for(OptionGreek aGreek: optionGreeks) {
//				if (aGreek.getOi()*aGreek.getLtp()/10000000>10) {
//					recProcessed++;
//					if (aGreek.getTradingSymbol().endsWith("CE")) {
//						ceBuffer.append(" " + aGreek.getTradingSymbol() + " Cr. " + aGreek.getOi()*aGreek.getLtp()/10000000f);
//						ceOptionGreeks.add(aGreek);
//					} else {
//						peBuffer.append(" " + aGreek.getTradingSymbol() + " Cr. " + aGreek.getOi()*aGreek.getLtp()/10000000f);
//						peOptionGreeks.add(aGreek);
//					}
//				}
//				if (recProcessed>=10) break;
//			}
//			fileLogTelegramWriter.write("ceBuffer="+ceBuffer.toString());
//			fileLogTelegramWriter.write("peBuffer="+peBuffer.toString());
//			
//			int ceSupportStrike = Integer.MAX_VALUE;
//			float ceSupportStrileWorth = 0f;
//			for(OptionGreek aGreek: ceOptionGreeks) {
//				int curStrike = getStrikePriceFromOptionName(aGreek.getTradingSymbol());
//				if (curStrike < ceSupportStrike) {
//					ceSupportStrike = curStrike;
//					ceSupportStrileWorth = aGreek.getOi()*aGreek.getLtp()/10000000f;
//				}
//			}
//			int peSupportStrike = Integer.MIN_VALUE;
//			float peSupportStrileWorth = 0f;
//			for(OptionGreek aGreek: peOptionGreeks) {
//				int curStrike = getStrikePriceFromOptionName(aGreek.getTradingSymbol());
//				if (curStrike > peSupportStrike) {
//					peSupportStrike = curStrike;
//					peSupportStrileWorth = aGreek.getOi()*aGreek.getLtp()/10000000f;
//				}
//			}
//			
//			fileLogTelegramWriter.write("B4 ceSupportStrike="+ceSupportStrike+" peSupportStrike="+peSupportStrike);
//			
//			if (ceSupportStrike != peSupportStrike) {
//				if (ceSupportStrileWorth > peSupportStrileWorth) {
//					peSupportStrike = ceSupportStrike;
//				} else {
//					ceSupportStrike = peSupportStrike;
//				}
//			}
//			fileLogTelegramWriter.write("A4 ceSupportStrike="+ceSupportStrike+" peSupportStrike="+peSupportStrike);
//			
//			if (this.instrumentLtp >= ceSupportStrike) {
//				retVal = "CE";
//			} else {
//				retVal = "PE";
//			}
			
			List<OptionGreek> ceOptionGreeks = new ArrayList<OptionGreek>();
			List<OptionGreek> peOptionGreeks = new ArrayList<OptionGreek>();
			for(OptionGreek aGreek: optionGreeks) {
				if (aGreek!=null) {
					if (aGreek.getTradingSymbol().endsWith("CE")) {
						ceOptionGreeks.add(aGreek);
					} else {
						peOptionGreeks.add(aGreek);
					}
				}
			}	
			Collections.sort(ceOptionGreeks, new SortbyIV());
			
			float lastIvRead = 0f;
			int ceStrikeb4Outier = 0;
			for(OptionGreek aGreek: ceOptionGreeks) {
				float delta = Math.abs(aGreek.getDelta());
				if (delta >= 0.1f && delta <= 0.9f) {
					float curIv = aGreek.getIv();
					if (lastIvRead<0.1f || curIv < lastIvRead + 5f) {
						ceStrikeb4Outier = getStrikePriceFromOptionName(aGreek.getTradingSymbol());
						lastIvRead = curIv;
					}
				}
			}
			lastIvRead = 0f;
			int peStrikeb4Outier = 0;
			for(OptionGreek aGreek: peOptionGreeks) {
				float delta = Math.abs(aGreek.getDelta());
				if (delta >= 0.1f && delta <= 0.9f) {
					float curIv = aGreek.getIv();
					if (lastIvRead<0.1f || curIv < lastIvRead + 5f) {
						peStrikeb4Outier = getStrikePriceFromOptionName(aGreek.getTradingSymbol());
						lastIvRead = curIv;
					}
				}
			}
			fileLogTelegramWriter.write("ceStrikeb4Outier="+ceStrikeb4Outier+" peStrikeb4Outier="+peStrikeb4Outier+ " CEside gap="+(this.instrumentLtp - ceStrikeb4Outier)+ " PEside gap="+(peStrikeb4Outier - this.instrumentLtp));
			
			if (peStrikeb4Outier - this.instrumentLtp < this.instrumentLtp - peStrikeb4Outier) {
				retVal = "PE";
			} else {
				retVal = "CE";
			}
			
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
}
