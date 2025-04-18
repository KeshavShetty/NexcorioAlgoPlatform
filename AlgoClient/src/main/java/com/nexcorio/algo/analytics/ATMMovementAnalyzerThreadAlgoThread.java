package com.nexcorio.algo.analytics;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.Calendar;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.kite.KiteCache;
import com.nexcorio.algo.util.FileLogTelegramWriter;
import com.nexcorio.algo.util.db.HDataSource;

public class ATMMovementAnalyzerThreadAlgoThread extends AnalyticsBaseClass implements Runnable {

	private static final Logger log = LogManager.getLogger(ATMMovementAnalyzerThreadAlgoThread.class);
	
	public ATMMovementAnalyzerThreadAlgoThread(String instrumentName, String backDateStr) {
		super();
		
		this.mainInstrument = KiteCache.getTradingSymbolMainInstrumentCache(instrumentName);
		
		this.algoname="ATMMovementAnalyzer";
		
		if (backDateStr!=null) {
			try {
				Calendar cal = Calendar.getInstance();
				cal.setTime(postgresLongDateFormat.parse(backDateStr));
				this.backtestDate = cal;
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		
		Thread t = new Thread(this, this.mainInstrument.getShortName()+this.algoname);
		t.setPriority(Thread.MAX_PRIORITY);
		t.start();
	}
	
	@Override
	public void run() {
		try {			
			fileLogTelegramWriter = new FileLogTelegramWriter(this.mainInstrument.getShortName(), this.algoname, this.backtestDate);
			
			do {
				//System.out.println("Going to sleep");
				sleep(15);				
				//System.out.println("Wakreup");
				fileLogTelegramWriter.write("====================================================================================================");
				this.instrumentLtp = getPriceFromTicks(this.mainInstrument.getShortName());
				fileLogTelegramWriter.write("instrumentLtp="+instrumentLtp);
				processATMMovement();
					
				if (timeout(15, 29, 0)) {
					prepareExit(" Exiting: Timeout");
				}
				
			} while(!this.exitThread);
			
			fileLogTelegramWriter.close();
		} catch (Exception e) {			
			log.error("Error"+e.getMessage(), e);
		}
	}
	
	private void processATMMovement() {
		try {
			processAndSaveRawStraddleData(0.4f);
			processAndSaveRawStraddleData(0.5f);
			processAndSaveRawStraddleData(0.6f);
			
		} catch (Exception e) {
			log.error("Error"+e.getMessage(),e);
			e.printStackTrace();
		}
	}
	
	
	private void processAndSaveRawStraddleData(float baseDelta) {
		Connection conn = null;
		try {			
			conn = HDataSource.getConnection();
			Statement stmt = conn.createStatement();
			
			String[] entryStraddleOptionNames = getStraddleOptionNamesByDeltaOptimised(baseDelta, 0); // Hedge distance 0
			
			OptionGreek ceOptionGreek = getOptionGreeks(entryStraddleOptionNames[0]);
			OptionGreek peOptionGreek = getOptionGreeks(entryStraddleOptionNames[1]);
			
			if (ceOptionGreek!=null && peOptionGreek!=null) {
				String insertSql = "INSERT INTO nexcorio_option_atm_movement_data (id, f_main_instrument, instrumentltp, base_delta, record_time, ceOptionname, peOptionname"
						+ ", ceDelta"
						+ ", peDelta"
						+ ", ceGamma"
						+ ", peGamma"
						+ ", ceVega"
						+ ", peVega"
						+ ", ceTheta"
						+ ", peTheta"
						+ ", ceIV"
						+ ", peIV"
						+ ", ceLtp"
						+ ", peLtp"
						+ ")" 
						+ " VALUES (nextval('nexcorio_option_atm_movement_data_id_seq')," + this.mainInstrument.getId()+ "," + this.instrumentLtp +"," + baseDelta +""
						+ ",'" + postgresLongDateFormat.format(getCurrentTime()) + "'"
						+ ",'" + entryStraddleOptionNames[0] + "'"
						+ ",'" + entryStraddleOptionNames[1] + "'"
						+ " ," + ceOptionGreek.getDelta() 
						+ " ," + peOptionGreek.getDelta()
						
						+ " ," + ceOptionGreek.getGamma() 
						+ " ," + peOptionGreek.getGamma()
						
						+ " ," + ceOptionGreek.getVega() 
						+ " ," + peOptionGreek.getVega()
						
						+ " ," + ceOptionGreek.getTheta() 
						+ " ," + peOptionGreek.getTheta()
						
						+ " ," + ceOptionGreek.getIv() 
						+ " ," + peOptionGreek.getIv()
						
						+ " ," + ceOptionGreek.getLtp() 
						+ " ," + peOptionGreek.getLtp()
						+ ")";
				log.info(insertSql);
				stmt.execute(insertSql);
			}
			stmt.close();
		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error"+e.getMessage(),e);
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				log.error(e);
			}
		}
	}
	
	public static void main(String[] args) {
		new ATMMovementAnalyzerThreadAlgoThread("BANKNIFTY", "2025-03-06 09:16:03");		
	}

}
