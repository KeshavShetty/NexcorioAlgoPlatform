package com.nexcorio.algo.junk;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.util.db.HDataSource;

class SortbyIV implements Comparator<OptionGreek> 
{ 
    // Used for sorting in ascending order of 
    // roll number 
    public int compare(OptionGreek a, OptionGreek b) 
    { 
    	if (a.getIv() < b.getIv()) return -1;
    	else if (a.getIv() > b.getIv()) return 1;
    	else return 0;
    } 
}

public class ATMThread implements Runnable {

	Long atmId;
	float underlyingInstrumentLtp;
	List<String> optionnames;
	Timestamp recTimestamp;
	
	public ATMThread(Long atmId, float underlyingInstrumentLtp, List<String> optionnames, Timestamp timestamp) {
		super();
		this.atmId = atmId;
		this.optionnames = optionnames;
		this.underlyingInstrumentLtp = underlyingInstrumentLtp;
		this.recTimestamp = timestamp;
		Thread t = new Thread(this, atmId+"");
		t.setPriority(Thread.MAX_PRIORITY);
		t.start();
	}
	

	@Override
	public void run() {
		System.out.println("Run reached");
		
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
		
		int upperBound5 = (int) (this.underlyingInstrumentLtp + 50*5);
		int lowerBound5 = (int) (this.underlyingInstrumentLtp - 50*5);
		
		int upperBound10 = (int) (this.underlyingInstrumentLtp + 50*10);
		int lowerBound10 = (int) (this.underlyingInstrumentLtp - 50*10);
		
		int upperBound20 = (int) (this.underlyingInstrumentLtp + 50*20);
		int lowerBound20 = (int) (this.underlyingInstrumentLtp - 50*20);
		
		float selective5CeGamma = 0f;
		float selective5PeGamma = 0f;
		float selective5CeIV = 0f;
		float selective5PeIV = 0f;
		int selective5CECount = 0;
		int selective5PECount = 0;
		
		float selective10CeGamma = 0f;
		float selective10PeGamma = 0f;
		float selective10CeIV = 0f;
		float selective10PeIV = 0f;
		int selective10CECount = 0;
		int selective10PECount = 0;
		
		float selective20CeGamma = 0f;
		float selective20PeGamma = 0f;
		float selective20CeIV = 0f;
		float selective20PeIV = 0f;
		int selective20CECount = 0;
		int selective20PECount = 0;
		
		Collections.sort(ceOptionGreeks, new SortbyIV());
		Collections.sort(peOptionGreeks, new SortbyIV());
		System.out.println("Sort done");
		
		float deltaRangeCEAvgLtp = 0f;
		float deltaRangeCEAvgIv = 0f;
		float deltaRangeCEAvgDelta = 0f;
		float deltaRangeCEAvgGamma = 0f;
		float deltaRangeCEAvgVega = 0f;
		float lastIvRead = 0f;
		int recCount = 0;
		
		for(OptionGreek aGreek: ceOptionGreeks) {
			int optionStrike = getStrike(aGreek.getTradingSymbol());
			
			if (optionStrike <= upperBound5 && optionStrike >= lowerBound5) {
				selective5CeGamma = selective5CeGamma + aGreek.getGamma();
				selective5CeIV = selective5CeGamma + aGreek.getIv();
				selective5CECount++;
			}
			
			if (optionStrike <= upperBound10 && optionStrike >= lowerBound10) {
				selective10CeGamma = selective10CeGamma + aGreek.getGamma();
				selective10CeIV = selective10CeGamma + aGreek.getIv();
				selective10CECount++;
			}
			
			if (optionStrike <= upperBound20 && optionStrike >= lowerBound20) {
				selective20CeGamma = selective20CeGamma + aGreek.getGamma();
				selective20CeIV = selective20CeGamma + aGreek.getIv();
				selective20CECount++;
			}
			
			if (aGreek.getDelta() >= 0.1f && aGreek.getDelta() <= 0.9f) {
				if (lastIvRead<0.1f || aGreek.getIv() < lastIvRead +5f) {
					//System.out.println("Include "+aGreek.getTradingSymbol()+" iv"+aGreek.getIv());
					deltaRangeCEAvgLtp = deltaRangeCEAvgLtp + aGreek.getLtp();
					deltaRangeCEAvgIv = deltaRangeCEAvgIv + aGreek.getIv();
					deltaRangeCEAvgDelta = deltaRangeCEAvgDelta + Math.abs(aGreek.getDelta());
					deltaRangeCEAvgGamma = deltaRangeCEAvgGamma + aGreek.getGamma();
					deltaRangeCEAvgVega = deltaRangeCEAvgVega + aGreek.getVega();
					
					lastIvRead = aGreek.getIv(); 
					recCount++;
				} else {
					//System.out.println("Skip "+aGreek.getTradingSymbol()+" iv"+aGreek.getIv());
				}
			}
		}
		deltaRangeCEAvgLtp = deltaRangeCEAvgLtp/(float)recCount;
		deltaRangeCEAvgIv  = deltaRangeCEAvgIv/(float)recCount;
		deltaRangeCEAvgDelta =  deltaRangeCEAvgDelta/(float)recCount;
		deltaRangeCEAvgGamma = deltaRangeCEAvgGamma/(float)recCount;
		deltaRangeCEAvgVega = deltaRangeCEAvgVega/(float)recCount;
		
		selective5CeGamma = selective5CECount!=0?selective5CeGamma/selective5CECount:0;
		selective5CeIV = selective5CECount!=0?selective5CeIV/selective5CECount:0;
		
		selective10CeGamma = selective10CECount!=0?selective10CeGamma/selective10CECount:0;
		selective10CeIV = selective10CECount!=0?selective10CeIV/selective10CECount:0;
		
		selective20CeGamma = selective20CECount!=0?selective20CeGamma/selective20CECount:0;
		selective20CeIV = selective20CECount!=0?selective20CeIV/selective20CECount:0;
		
		
		float deltaRangePEAvgLtp = 0f;
		float deltaRangePEAvgIv = 0f;
		float deltaRangePEAvgDelta = 0f;
		float deltaRangePEAvgGamma = 0f;
		float deltaRangePEAvgVega = 0f;
		lastIvRead = 0f;
		recCount = 0;
		
		for(OptionGreek aGreek: peOptionGreeks) {
			int optionStrike = getStrike(aGreek.getTradingSymbol());
			
			if (optionStrike <= upperBound5 && optionStrike >= lowerBound5) {
				selective5PeGamma = selective5PeGamma + aGreek.getGamma();
				selective5PeIV = selective5PeGamma + aGreek.getIv();
				selective5PECount++;
			}
			
			if (optionStrike <= upperBound10 && optionStrike >= lowerBound10) {
				selective10PeGamma = selective10PeGamma + aGreek.getGamma();
				selective10PeIV = selective10PeGamma + aGreek.getIv();
				selective10PECount++;
			}
			
			if (optionStrike <= upperBound20 && optionStrike >= lowerBound20) {
				selective20PeGamma = selective20PeGamma + aGreek.getGamma();
				selective20PeIV = selective20PeGamma + aGreek.getIv();
				selective20PECount++;
			}
			
			if (aGreek.getDelta() >= -0.9f && aGreek.getDelta() <= -0.1f) {
				if (lastIvRead<0.1f || aGreek.getIv() < lastIvRead +5f) {
					//System.out.println("Include "+aGreek.getTradingSymbol()+" iv"+aGreek.getIv());
					deltaRangePEAvgLtp = deltaRangePEAvgLtp + aGreek.getLtp();
					deltaRangePEAvgIv = deltaRangePEAvgIv + aGreek.getIv();
					deltaRangePEAvgDelta = deltaRangePEAvgDelta + Math.abs(aGreek.getDelta());
					deltaRangePEAvgGamma = deltaRangePEAvgGamma + aGreek.getGamma();
					deltaRangePEAvgVega = deltaRangePEAvgVega + aGreek.getVega();
					
					lastIvRead = aGreek.getIv();
					recCount++;
				}  else {
					//System.out.println("Skip "+aGreek.getTradingSymbol()+" iv"+aGreek.getIv());
				}
			}
			
		}
		deltaRangePEAvgLtp = deltaRangePEAvgLtp/(float)recCount;
		deltaRangePEAvgIv  = deltaRangePEAvgIv/(float)recCount;
		deltaRangePEAvgDelta =  deltaRangePEAvgDelta/(float)recCount;
		deltaRangePEAvgGamma = deltaRangePEAvgGamma/(float)recCount;
		deltaRangePEAvgVega = deltaRangePEAvgVega/(float)recCount;
		
		selective5PeGamma = selective5PECount!=0?selective5PeGamma/selective5PECount:0;
		selective5PeIV = selective5PECount!=0?selective5PeIV/selective5PECount:0;
		
		selective10PeGamma = selective10PECount!=0?selective10PeGamma/selective10PECount:0;
		selective10PeIV = selective10PECount!=0?selective10PeIV/selective10PECount:0;
		
		selective20PeGamma = selective20PECount!=0?selective20PeGamma/selective20PECount:0;
		selective20PeIV = selective20PECount!=0?selective20PeIV/selective20PECount:0;
		
		
		
		saveGreek(selective5CeGamma, selective5CeIV, selective10CeGamma, selective10CeIV, selective20CeGamma, selective20CeIV,
				selective5PeGamma, selective5PeIV, selective10PeGamma, selective10PeIV, selective20PeGamma, selective20PeIV,
				deltaRangeCEAvgLtp, deltaRangeCEAvgIv, deltaRangeCEAvgDelta, deltaRangeCEAvgGamma, deltaRangeCEAvgVega,
				deltaRangePEAvgLtp, deltaRangePEAvgIv, deltaRangePEAvgDelta, deltaRangePEAvgGamma, deltaRangePEAvgVega);
	}

	private void saveGreek(float selective5CeGamma, float selective5CeIV, float selective10CeGamma, float selective10CeIV, float selective20CeGamma, float selective20CeIV,
			float selective5PeGamma, float selective5PeIV, float selective10PeGamma, float selective10PeIV, float selective20PeGamma, float selective20PeIV,
			float deltaRangeCEAvgLtp, float deltaRangeCEAvgIv, float deltaRangeCEAvgDelta, float deltaRangeCEAvgGamma, float deltaRangeCEAvgVega,
			float deltaRangePEAvgLtp, float deltaRangePEAvgIv, float deltaRangePEAvgDelta, float deltaRangePEAvgGamma, float deltaRangePEAvgVega) {
		Connection conn = null;
		try {
			conn = HDataSource.getConnection();
			Statement stmt = conn.createStatement();
			
			String updateSql = "UPDATE nexcorio_option_atm_movement_data set "
					
					+ "  selective10strike_avgcegamma=" + selective10CeGamma 
					+ ", selective10strike_avgpegamma=" + selective10PeGamma
					+ ", selective10strike_avgceiv=" + selective10CeIV
					+ ", selective10strike_avgpeiv=" + selective10PeIV
					+ ", selective20strike_avgcegamma=" + selective20CeGamma
					+ ", selective20strike_avgpegamma=" + selective20PeGamma
					+ ", selective20strike_avgceiv=" + selective20CeIV
					+ ", selective20strike_avgpeiv=" + selective20PeIV
					+ ", deltaRangeCEAvgLtp=" + deltaRangeCEAvgLtp
					+ ", deltaRangeCEAvgIv=" + deltaRangeCEAvgIv
					+ ", deltaRangeCEAvgDelta=" + deltaRangeCEAvgDelta
					+ ", deltaRangeCEAvgGamma=" + deltaRangeCEAvgGamma
					+ ", deltaRangeCEAvgVega=" + deltaRangeCEAvgVega
					+ ", deltaRangePEAvgLtp=" + deltaRangePEAvgLtp
					+ ", deltaRangePEAvgIv=" + deltaRangePEAvgIv
					+ ", deltaRangePEAvgDelta=" + deltaRangePEAvgDelta
					+ ", deltaRangePEAvgGamma=" + deltaRangePEAvgGamma
					+ ", deltaRangePEAvgVega=" +deltaRangePEAvgVega
					+ " where id="+atmId;
			System.out.println(updateSql);
			
			
			stmt.executeUpdate(updateSql);
			
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
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			SimpleDateFormat postgresLongDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

			
			String fetchSql = "select iv, delta, vega, theta, gamma, ltp, oi from nexcorio_option_greeks  where trading_symbol = '" + optionName + "'"
					+ " and quote_time <= '" + postgresLongDateFormat.format(recTimestamp) + "'"
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
	
	private int getStrike(String optionname) {
		int retVal = 0;
		String strikename = optionname.substring(optionname.length()-7, optionname.length()-2);
		//System.out.println(strikename);
		retVal = Integer.parseInt(strikename);
		return retVal;
	}
	
	public static void main(String[] args) {
		//System.out.println(getStrike("NIFTY25JUN25300PE"));
	}
}
