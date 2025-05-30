package com.nexcorio.algo.junk;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.util.db.HDataSource;

public class ATMThread implements Runnable {

	Long atmId;
	List<String> optionnames;
	
	public ATMThread(Long atmId, List<String> optionnames) {
		super();
		this.atmId = atmId;
		this.optionnames = optionnames;
		
		Thread t = new Thread(this, atmId+"");
		t.setPriority(Thread.MAX_PRIORITY);
		t.start();
	}

	@Override
	public void run() {
		
		List<OptionGreek> ceOptionGreeks = new ArrayList<>();
		List<OptionGreek> peOptionGreeks = new ArrayList<>();
		for(String optionname:optionnames ) {
			OptionGreek aGreek = getOptionGreeks(optionname);
			if (aGreek!=null) {
				if (optionname.endsWith("CE")) {
					ceOptionGreeks.add(aGreek);
				} else {
					peOptionGreeks.add(aGreek);
				}
			}
		}
		
		float avgCeIV = 0f;
		float avgPeIV = 0f;
		
		float totalCEGamma = 0f;
		float totalPEGamma = 0f;
		
		float totalCEVega = 0f;
		float totalPEVega = 0f;
		
		for(OptionGreek aGreek: ceOptionGreeks) {
			avgCeIV = avgCeIV + aGreek.getIv();
			totalCEGamma =  totalCEGamma + aGreek.getGamma();
			totalCEVega =  totalCEVega + aGreek.getVega();
		}
		
		for(OptionGreek aGreek: peOptionGreeks) {
			avgPeIV = avgPeIV + aGreek.getIv();
			totalPEGamma =  totalPEGamma + aGreek.getGamma();
			totalPEVega =  totalPEVega + aGreek.getVega();
		}
		
		avgCeIV = avgCeIV/(float)ceOptionGreeks.size();
		avgPeIV = avgPeIV/(float)peOptionGreeks.size();
		//System.out.println("avgCeIV="+avgCeIV+" avgPeIV="+avgPeIV+" totalCEGamma="+totalCEGamma+" totalPEGamma="+totalPEGamma+" totalCEVega="+totalCEVega+" totalPEVega="+totalPEVega);
		
		saveGreek(avgCeIV, avgPeIV, totalCEGamma, totalPEGamma, totalCEVega, totalPEVega);
	}

	private void saveGreek(float avgCeIV, float avgPeIV, float totalCEGamma, float totalPEGamma, float totalCEVega, float totalPEVega) {
		Connection conn = null;
		try {
			conn = HDataSource.getConnection();
			Statement stmt = conn.createStatement();
			
			String updateSql = "UPDATE nexcorio_option_atm_movement_data "
					+ "set totalceiv=" + avgCeIV
					+ ", totalpeiv=" + avgPeIV
					
					+ ", totalcegamma=" +totalCEGamma
					+ ", totalpegamma=" +totalPEGamma
					
					+ ", totalcevega=" +totalCEVega
					+ ", totalpevega=" +totalPEVega
					+ " where id="+atmId;
			System.out.println(updateSql);
			
			stmt.execute(updateSql);
			
			stmt.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (conn!=null) conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	protected OptionGreek getOptionGreeks(String optionName) {
		
		if (optionName==null || optionName.equals("")) return null;
		
		OptionGreek retVal = null;
		Connection conn = null;
		try {
			conn = HDataSource.getConnection();
			Statement stmt = conn.createStatement();
			
			String fetchSql = "select iv, delta, vega, theta, gamma, ltp, oi from nexcorio_option_greeks  where trading_symbol = '" + optionName + "'"
					+ " and quote_time <= (select record_time from nexcorio_option_atm_movement_data where id=" + atmId + ")"
					+ " order by quote_time desc limit 1";
			//System.out.println(fetchSql);
			
			ResultSet rs = stmt.executeQuery(fetchSql);
			while (rs.next()) {
				retVal = new OptionGreek(optionName, rs.getFloat("iv"), rs.getFloat("delta"), rs.getFloat("vega"), rs.getFloat("theta"), rs.getFloat("gamma"), rs.getFloat("ltp"), rs.getFloat("oi"));
			}
			rs.close();
			stmt.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (conn!=null) conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return retVal;
	}
}
