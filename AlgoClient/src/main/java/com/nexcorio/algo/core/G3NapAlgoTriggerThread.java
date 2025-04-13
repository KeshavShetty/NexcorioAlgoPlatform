package com.nexcorio.algo.core;

import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.nexcorio.algo.util.FileLogTelegramWriter;
import com.nexcorio.algo.util.KiteUtil;
import com.nexcorio.algo.util.db.HDataSource;

class G3NapAlgoDetails {
	
	Long napAlgoId;
	String algoname;
	String algoClassname;
	
	public Long getNapAlgoId() {
		return napAlgoId;
	}
	public void setNapAlgoId(Long napAlgoId) {
		this.napAlgoId = napAlgoId;
	}
	public String getAlgoname() {
		return algoname;
	}
	public void setAlgoname(String algoname) {
		this.algoname = algoname;
	}
	
	public String getAlgoClassname() {
		return algoClassname;
	}
	public void setAlgoClassname(String algoClassname) {
		this.algoClassname = algoClassname;
	}
}

public class G3NapAlgoTriggerThread implements Runnable {

	private static final Logger log = LogManager.getLogger(G3NapAlgoTriggerThread.class);
	
	FileLogTelegramWriter fileLogTelegramWriter;
	
	String algoname;
	boolean exitThread = false;
	
	SimpleDateFormat postgresLongDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public G3NapAlgoTriggerThread() {
		super();
	
		this.algoname="G3NapAlgoTriggerThread";
		
		Thread t = new Thread(this, this.algoname);
		t.setPriority(Thread.MAX_PRIORITY);
		t.start();
	}
			
	@Override
	public void run() {
		try {			
			fileLogTelegramWriter = new FileLogTelegramWriter("Generic", this.algoname, null);
			
			updateAlgoStatus("PENDING");
			
			Calendar cal = Calendar.getInstance();
			cal.set(Calendar.HOUR_OF_DAY, 9);		
			cal.set(Calendar.MINUTE, 15);
			String prevHourMinuteSecondPart = getHourMinuteSecondPart(cal.getTime());
			do {
				Thread.sleep(5*1000);
				fileLogTelegramWriter.write("Wakeup & fetching Pending Algos");
				String currentHourMinuteSecondPart = getHourMinuteSecondPart(new Date());
				
				List<G3NapAlgoDetails> algosToExecute = getPendingActiveAlgosToExecute(prevHourMinuteSecondPart, currentHourMinuteSecondPart);
				
				for(G3NapAlgoDetails aAlgo: algosToExecute) {
					fileLogTelegramWriter.write("Triggering " + aAlgo.getNapAlgoId() + " " +aAlgo.getAlgoClassname());
					triggerAlgo(aAlgo.getNapAlgoId(), aAlgo.getAlgoClassname());
				}
				
				prevHourMinuteSecondPart = currentHourMinuteSecondPart;
				
				if ((new Date()).after(KiteUtil.getDailyCustomTime(15, 20, 0))) {
					this.exitThread = true;
				}
			} while(!this.exitThread); 
			updateAlgoStatus("EXITED");
			fileLogTelegramWriter.close();
		} catch (Exception e) {			
			log.error("Error"+e.getMessage(), e);
		}
	}
	
	private List<G3NapAlgoDetails> getPendingActiveAlgosToExecute(String prevHourMinuteSecondPart, String currentHourMinuteSecondPart) {
		List<G3NapAlgoDetails> retList =  new ArrayList<G3NapAlgoDetails>();
		
		Connection conn = null;
		try {
			conn = HDataSource.getConnection();
			Statement stmt = conn.createStatement();
			
			String opOIFetch = "select id, algo_class_name from nexcorio_options_algo_strategy where isactive=TRUE and entry_time > '" + prevHourMinuteSecondPart + "' and entry_time <='" + currentHourMinuteSecondPart+"'";
			fileLogTelegramWriter.write("opOIFetch="+opOIFetch);
			
			ResultSet rs = stmt.executeQuery(opOIFetch);
			while (rs.next()) {
				G3NapAlgoDetails aAlgo = new G3NapAlgoDetails();
				aAlgo.setNapAlgoId(rs.getLong("id"));
				aAlgo.setAlgoClassname(rs.getString("algo_class_name"));
				retList.add(aAlgo);
			}
			rs.close();
			stmt.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		return retList;
	}
	
	private void triggerAlgo(Long napAlgoId, String algoClassname) {
		
		try {
			Class<?> myClass = Class.forName(algoClassname);
			Constructor<?> ctr = myClass.getConstructor(Long.class, String.class);
			Object object = ctr.newInstance(new Object[] { napAlgoId, null });
		} catch (Exception e) {
			log.error(e.getMessage());
			e.printStackTrace();
		}
	}
	
	private String getHourMinuteSecondPart(Date calTime) {
		String retStr = "";
		String[] timeParts = postgresLongDateFormat.format(calTime).split(" ");
		retStr = timeParts[1];
		return retStr;
	}
	
	private void updateAlgoStatus(String status) {
		Connection conn = null;
		try {
			conn = HDataSource.getConnection();
			Statement stmt = conn.createStatement();	
			String updateSql = "UPDATE nexcorio_options_algo_strategy set status='" + status + "'";
			fileLogTelegramWriter.write(updateSql);
			stmt.executeUpdate(updateSql);
			stmt.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) {
		new G3NapAlgoTriggerThread();
		
	}
}
