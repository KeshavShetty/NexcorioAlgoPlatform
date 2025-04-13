package com.nexcorio.algo.util;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.util.db.HDataSource;

public class FileLogTelegramWriter {
	
	private static final Logger log = LogManager.getLogger(FileLogTelegramWriter.class);
	
	FileWriter indLogWriter = null;

	String indexName = null;
	String algoname = null;
	boolean enableTelegram = false;
	Calendar backTestCal = null;
	int msgCount = 0;
	Long prevMsgId = null;
	String telegramMessage = "";
	
	public FileLogTelegramWriter(String indexName, String algoname, Calendar backTestCal) {
		super();
		this.indexName = indexName;
		this.algoname = algoname;
		this.backTestCal = backTestCal;
		
		if (backTestCal!=null) this.enableTelegram = false;
		// else this.enableTelegram = telegramMessageEnabled(indexName, algoname); //Todo
		
		try {
			SimpleDateFormat fileDateFormat = new SimpleDateFormat("dd-MM-yyyy-HH-mm-ss");
			this.indLogWriter = new FileWriter(ApplicationConfig.getProperty("logFileLocation") + this.algoname + "-" + fileDateFormat.format(this.backTestCal!=null?this.backTestCal.getTime():(new Date()))+".log");
		} catch (IOException e) {
			log.error("Error"+e.getMessage(), e);
		}
	}
	
	public void write(String logMessage ) {
		try {
			Date dateToUse = backTestCal!=null?backTestCal.getTime():(new Date());
			this.indLogWriter.write("\r\n" + dateToUse + " " + logMessage);
		} catch (IOException e) {
			log.error("Error"+e.getMessage(), e);
		}
	}
	
	public void write(Boolean postToTelegram, String logMessage) {
		
		write(logMessage);
		if (this.enableTelegram && postToTelegram) {
			telegramMessage = telegramMessage + (telegramMessage.length()!=0?"\r\n":"") + logMessage;
		}
	}
	
	public void flushTelegramMessage() {
		if (!telegramMessage.equals("")) {
			prevMsgId = TelegramUtil.postTelegramMessage((++msgCount) + ". " + "[" + this.indexName + "] " + this.algoname + "\r\n"+ telegramMessage, prevMsgId);
			telegramMessage = "";
		}
	}
	
	public void close() {
		try {
			this.indLogWriter.close();
		} catch (IOException e) {
			log.error("Error"+e.getMessage(), e);
		}
	}
	
	private boolean telegramMessageEnabled(String indexname, String algoname) {
		boolean retVal = false;
		Connection conn = null;
		try {
			conn = HDataSource.getConnection();
			Statement stmt = conn.createStatement();
			
			String fetchSql = "select telegram_messaage_enabled from options_place_actual_order_flags where indexname like '" + indexname +"' and algoname like '" + algoname + "' ";
			ResultSet rs = stmt.executeQuery(fetchSql);
			while (rs.next()) {
				retVal = rs.getBoolean("telegram_messaage_enabled");
			}
			rs.close();
			stmt.close();
			//System.out.println("retVal="+retVal);
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
		return retVal;
	}

	public boolean isEnableTelegram() {
		return enableTelegram;
	}

	public void setEnableTelegram(boolean enableTelegram) {
		this.enableTelegram = enableTelegram;
	}
	
}
