package com.nexcorio.algo.kite;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.analytics.OptionGreeksExtractorsThread;
import com.nexcorio.algo.dto.MainInstruments;
import com.nexcorio.algo.util.db.HDataSource;
import com.zerodhatech.models.Depth;
import com.zerodhatech.models.Tick;

public class ZerodhaIntradayStreamingThread implements Runnable {
	
	private static final Logger log = LogManager.getLogger(ZerodhaIntradayStreamingThread.class);
	
	DateTimeFormatter postgresLongDateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

	
	ArrayList<Tick> ticks = null;
		
	public ZerodhaIntradayStreamingThread(ArrayList<Tick> ticks) {
		super();
		this.ticks = ticks;
		Thread t = new Thread(this, "ZerodhaIntradayStreamingThread"+System.currentTimeMillis());
		t.setPriority(Thread.MAX_PRIORITY);
		t.start();
	}	
	
	@Override
	public void run() {	
		Long beginTime = System.currentTimeMillis();
		
		processTicks();

		Long endTime = System.currentTimeMillis();
		System.out.println("Recieved ticks size="+ticks.size() + " time taken(ms) " + (endTime-beginTime));
		log.info("Time taken="+(endTime-beginTime)+ " Active thread count=" +Thread.activeCount() + " Time consumes="+ (endTime-beginTime));
	}
	
	private void processTicks() {				
		try {
			for(int i=0;i<ticks.size();i++) {
				Tick aTick = ticks.get(i);
				if (aTick!=null && aTick.getTickTimestamp()!=null)  {
					saveTick(aTick);
				}
            }
			
		} catch(Exception ex) {
			log.error("Error"+ex.getMessage(),ex);
			ex.printStackTrace(System.out);
		}
	}
	
	private void saveTick(Tick aTick) {
		Connection conn = null;
		Statement stmt = null;
		try {
			conn = HDataSource.getConnection();
			stmt = conn.createStatement();
			// Save the tick
	    	String fetchNextSeq = "select nextval('nexcorio_tick_data_id_seq') as nextId";
	    	
	    	Long fStreamingId = null;
	    	ResultSet rs = stmt.executeQuery(fetchNextSeq);
			while (rs.next()) {
				fStreamingId = rs.getLong("nextId");
			}
			rs.close();
			
			String tradingSymbol = KiteCache.getInstrumentTokenToTradingSymbolCache(aTick.getInstrumentToken());
			MainInstruments mainInstrument = KiteCache.getTradingSymbolMainInstrumentCache(tradingSymbol);
			
			//log.info(tradingSymbol + " ltp="+aTick.getLastTradedPrice() + " oi="+aTick.getOi() );
			
			log.info("aTick.getTickTimestamp()="+aTick.getTickTimestamp());
			LocalDateTime dateTime = aTick.getTickTimestamp().toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
			String formattedDateTime = dateTime.format(postgresLongDateFormat); 
			log.info("formattedDateTime="+formattedDateTime);
			
			String insertSql = "INSERT INTO nexcorio_tick_data (id, f_main_instrument, trading_symbol, quote_time"
					+ ", last_traded_price, last_traded_qty, open_interest, total_buy_qty, total_sell_qty, volume_traded_today, avg_traded_price)"
					+ " VALUES (" + fStreamingId + "," + mainInstrument.getId() + ",'"+tradingSymbol+"'"
					+ ",'"+formattedDateTime+"'"
					+ ", " + aTick.getLastTradedPrice() + "," + aTick.getLastTradedQuantity() + "," + aTick.getOi() + "," + aTick.getTotalBuyQuantity() 
					+ "," + aTick.getTotalSellQuantity() + "," + aTick.getVolumeTradedToday() + "," + aTick.getAverageTradePrice() + ")";
			//log.info(insertSql);
			
			stmt.execute(insertSql);
			
			// Todo: What to do with Market Depth
			Map<String, ArrayList<Depth>> marketDepth = aTick.getMarketDepth();
			
			// Calculate or Extract FNO Analytics
			String exchange = KiteCache.getTradingSymbolExchangeCache(tradingSymbol);
			if (exchange!=null) { // Null means main instruent
				if (exchange.equalsIgnoreCase("NFO") || exchange.equalsIgnoreCase("BFO")) {
					if (tradingSymbol.endsWith("CE") || tradingSymbol.endsWith("PE")) { // Option Greeks
						new OptionGreeksExtractorsThread(fStreamingId, tradingSymbol, (float) aTick.getLastTradedPrice(), (float) aTick.getOi(), aTick.getTickTimestamp());
					}
				}
			}
			
		} catch(Exception ex) {
			ex.printStackTrace();
			log.error(ex);
		} finally {
			try {
				if (stmt!=null) stmt.close();
				if (conn!=null) conn.close();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}
}
