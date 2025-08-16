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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.util.FileLogTelegramWriter;
import com.nexcorio.algo.util.db.HDataSource;

class SortbyIV implements Comparator<OptionGreek> 
{ 
    // Comparator 
    public int compare(OptionGreek a, OptionGreek b) 
    { 
    	if (a.getIv() < b.getIv()) return -1;
    	else if (a.getIv() > b.getIv()) return 1;
    	else return 0;
    } 
}

class SortbyWorthDesc implements Comparator<OptionGreek> 
{ 
    // Comparator 
    public int compare(OptionGreek a, OptionGreek b) 
    { 
    	if (a.getOi()*a.getLtp() > b.getOi()*b.getLtp()) return -1;
    	else if (a.getOi()*a.getLtp() < b.getOi()*b.getLtp()) return 1;
    	else return 0;
    } 
}

public class ATMThread implements Runnable {

	Long atmId;
	float underlyingInstrumentLtp;
	List<String> optionnames;
	Timestamp recTimestamp;
	FileLogTelegramWriter fileLogTelegramWriter = null;
	
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
		
		Collections.sort(ceOptionGreeks, new SortbyIV());
		Collections.sort(peOptionGreeks, new SortbyIV());
		System.out.println("Sort done");
		
		Map<String, Float> retMap = new HashMap<>();
		
		float lastIvRead = 0f;
		int recCount = 0;
		int fullcount = 0;
		float deltaRangeCEAvgLtp = 0f;
		float deltaRangeCEAvgIv = 0f;
		float deltaRangeCEFullAvgIv = 0f;
		float deltaRangeCEAvgDelta = 0f;
		float deltaRangeCEAvgGamma = 0f;
		float deltaRangeCEFullGamma = 0f;
		float deltaRangeCEAvgVega = 0f;
		float deltaRangeCEWorth = 0f;
		float deltaRangeCEOI = 0f;
		float deltaRangeCEDeltaOI = 0f;
		float deltaRangeCEFullDeltaOI = 0f;
		float deltaRangeCEGammaOI = 0f;
		float dr49CEAvgIV = 0f;
		int dr49Count = 0;
		float ceDeltaOIWorth = 0f;
		
		float dr16CEAvgIV = 0f;
		int dr16Count = 0;

		float lowerDelta = 0.1f;
		float upperDelta = 0.9f;
		
		List<Float> fullCEIvList = new ArrayList<Float>();
		
		List<Float> dr16CEIvList = new ArrayList<Float>();
		List<Float> dr49CEIvList = new ArrayList<Float>();
		List<Float> dr46CEIvList = new ArrayList<Float>();
		List<Float> dr4PlusCEIvList = new ArrayList<Float>();
		List<Float> outlierCEIvList = new ArrayList<Float>();
		
		for(OptionGreek aGreek: ceOptionGreeks) {
			float delta = Math.abs(aGreek.getDelta());
			fullCEIvList.add(aGreek.getIv());
			
			if (delta >= 0.1f && delta <= 0.6f) dr16CEIvList.add(aGreek.getIv());
			if (delta >= 0.4f && delta <= 0.9f) dr49CEIvList.add(aGreek.getIv());
			if (delta >= 0.4f && delta <= 0.6f) dr46CEIvList.add(aGreek.getIv());
			if (delta >= 0.4f ) dr4PlusCEIvList.add(aGreek.getIv());
			
			if (delta >= lowerDelta && delta <= upperDelta) {
				
				float curIv = aGreek.getIv();
				float ltp = aGreek.getLtp();
				float oi = aGreek.getOi();
				float gamma = aGreek.getGamma();
				
				if (lastIvRead<0.1f || curIv < lastIvRead + 5f) {
					deltaRangeCEAvgLtp = deltaRangeCEAvgLtp + ltp;
					deltaRangeCEAvgIv = deltaRangeCEAvgIv + curIv;
					ceDeltaOIWorth = ceDeltaOIWorth + oi*delta;	
					deltaRangeCEAvgDelta = deltaRangeCEAvgDelta + delta;
					deltaRangeCEAvgGamma = deltaRangeCEAvgGamma + gamma;
					deltaRangeCEAvgVega = deltaRangeCEAvgVega + aGreek.getVega();
					deltaRangeCEWorth = deltaRangeCEWorth + oi*ltp;
					deltaRangeCEOI = deltaRangeCEOI + oi;
					deltaRangeCEDeltaOI = deltaRangeCEDeltaOI + oi*delta;
					deltaRangeCEGammaOI = deltaRangeCEGammaOI + oi*gamma;
					lastIvRead = curIv; 
					recCount++;
				} else {
					outlierCEIvList.add(curIv);
				}
				fullcount++;
				deltaRangeCEFullAvgIv = deltaRangeCEFullAvgIv + curIv;
				deltaRangeCEFullGamma = deltaRangeCEFullGamma + gamma;
				deltaRangeCEFullDeltaOI = deltaRangeCEFullDeltaOI + oi* delta;
				
				if (delta >= 0.4f && delta <= 0.9f) {
					dr49CEAvgIV = dr49CEAvgIV + curIv;
					dr49Count++;
				}
				if (delta >= 0.1f && delta <= 0.6f) {
					dr16CEAvgIV = dr16CEAvgIV + curIv;
					dr16Count++;
				}
			}
		}
		int countCETotal = fullcount;
		int countCEOutlier = fullcount - recCount;
		
		float deltaRangeHybridCEAvgIv = 0f;
		float deltaRangeHybridCEAvgGamma = 0f;
		if ((float)recCount/(float)fullcount < 0.65f) {				
			deltaRangeHybridCEAvgIv = deltaRangeCEFullAvgIv/(float)fullcount;
			deltaRangeHybridCEAvgGamma = deltaRangeCEFullGamma/(float)fullcount;
		} else {
			deltaRangeHybridCEAvgIv = deltaRangeCEAvgIv/(float)recCount;
			deltaRangeHybridCEAvgGamma = deltaRangeCEAvgGamma/(float)recCount;
		}
		
		deltaRangeCEAvgLtp = deltaRangeCEAvgLtp/(float)recCount;
		deltaRangeCEAvgIv  = deltaRangeCEAvgIv/(float)recCount;
		deltaRangeCEAvgDelta =  deltaRangeCEAvgDelta/(float)recCount;
		deltaRangeCEAvgGamma = deltaRangeCEAvgGamma/(float)recCount;
		deltaRangeCEAvgVega = deltaRangeCEAvgVega/(float)recCount;
		deltaRangeCEWorth = deltaRangeCEWorth/10000000f; // in Crores				
		deltaRangeCEFullAvgIv = deltaRangeCEFullAvgIv/(float)fullcount;
		
		retMap.put("deltaRangeCEAvgLtp", deltaRangeCEAvgLtp);
		retMap.put("deltaRangeCEAvgIv", deltaRangeCEAvgIv);
		retMap.put("deltaRangeCEAvgDelta", deltaRangeCEAvgDelta);
		retMap.put("deltaRangeCEAvgGamma", deltaRangeCEAvgGamma);
		retMap.put("deltaRangeCEAvgVega", deltaRangeCEAvgVega);
		retMap.put("deltaRangeCEWorth", deltaRangeCEWorth);
		retMap.put("deltaRangeCEOI", deltaRangeCEOI/10000000f);
		retMap.put("deltaRangeCEDeltaOI", deltaRangeCEDeltaOI/10000000f);
		retMap.put("deltaRangeCEFullDeltaOI", deltaRangeCEFullDeltaOI/10000000f);
		retMap.put("deltaRangeCEGammaOI", deltaRangeCEGammaOI);
		retMap.put("deltaRangeCEFullAvgIv", deltaRangeCEFullAvgIv);
		retMap.put("deltaRangeHybridCEAvgIv",deltaRangeHybridCEAvgIv);
		retMap.put("deltaRangeHybridCEAvgGamma",deltaRangeHybridCEAvgGamma);
		retMap.put("deltaRangeCEOutlierRatio", (float)fullcount/(float)recCount);
		retMap.put("dr49CEAvgIV",dr49CEAvgIV!=0?dr49CEAvgIV/(float)dr49Count:0);
		retMap.put("dr16CEAvgIV",dr16CEAvgIV!=0?dr16CEAvgIV/(float)dr16Count:0);
		retMap.put("countCETotal",(float) countCETotal);
		retMap.put("countCEOutlier",(float) countCEOutlier);
		retMap.put("ceDeltaOIWorth",ceDeltaOIWorth);
		
		retMap.put("fullRangeCETotalIV",(float) fullCEIvList.stream().mapToDouble(d -> d).sum());
		
		retMap.put("dr16CETotalIV",(float) dr16CEIvList.stream().mapToDouble(d -> d).sum());
		retMap.put("dr49CETotalIV",(float) dr49CEIvList.stream().mapToDouble(d -> d).sum());
		retMap.put("dr46CETotalIV",(float) dr46CEIvList.stream().mapToDouble(d -> d).sum());
		retMap.put("dr4PlusCETotalIV",(float) dr4PlusCEIvList.stream().mapToDouble(d -> d).sum());
		
		retMap.put("outlierCEMinIV", outlierCEIvList.size()>0?outlierCEIvList.get(0):0f);
		retMap.put("outlierCEMaxIV", outlierCEIvList.size()>0?outlierCEIvList.get(outlierCEIvList.size()-1):0f);
		retMap.put("outlierCETotalIV",(float) outlierCEIvList.stream().mapToDouble(d -> d).sum());
		retMap.put("outlierCEAvgIV",(float) outlierCEIvList.stream().mapToDouble(d -> d).average().orElse(0.0));
		retMap.put("outlierCEMedianIV", (float) outlierCEIvList.stream().mapToDouble(d -> d).sorted().skip((outlierCEIvList.size()-1)/2).limit(2-outlierCEIvList.size()%2).average().orElse(0.0) );
		
		// Now PE
		lastIvRead = 0f;
		recCount = 0;
		fullcount = 0;
		float deltaRangePEAvgLtp = 0f;
		float deltaRangePEAvgIv = 0f;
		
		float deltaRangePEFullAvgIv = 0f;
		float deltaRangePEFullGamma = 0f;
		float deltaRangePEAvgDelta = 0f;
		float deltaRangePEAvgGamma = 0f;
		float deltaRangePEAvgVega = 0f;
		float deltaRangePEWorth = 0f;
		float deltaRangePEOI = 0f;
		
		float deltaRangePEDeltaOI = 0f;
		float deltaRangePEGammaOI = 0f;
		float deltaRangePEFullDeltaOI = 0f;
		float deltaRangePEvolume1min = 0f;
		
		float dr49PEAvgIV = 0f;
		dr49Count = 0;
		float dr16PEAvgIV = 0f;
		dr16Count = 0;
		float peDeltaOIWorth = 0f;
		
		List<Float> fullPEIvList = new ArrayList<Float>();

		List<Float> dr16PEIvList = new ArrayList<Float>();
		List<Float> dr49PEIvList = new ArrayList<Float>();
		List<Float> dr46PEIvList = new ArrayList<Float>();
		List<Float> dr4PlusPEIvList = new ArrayList<Float>();
		
		List<Float> outlierPEIvList = new ArrayList<Float>();
		
		for(OptionGreek aGreek: peOptionGreeks) {
			float delta = Math.abs(aGreek.getDelta());		
			fullPEIvList.add(aGreek.getIv());
			
			if (delta >= 0.1f && delta <= 0.6f) dr16PEIvList.add(aGreek.getIv());
			if (delta >= 0.4f && delta <= 0.9f) dr49PEIvList.add(aGreek.getIv());
			if (delta >= 0.4f && delta <= 0.6f) dr46PEIvList.add(aGreek.getIv());
			if (delta >= 0.4f ) dr4PlusPEIvList.add(aGreek.getIv());
			
			if (delta >= lowerDelta && delta <= upperDelta) {
				float curIv = aGreek.getIv();						
				float ltp = aGreek.getLtp();
				float oi = aGreek.getOi();				
				float gamma = aGreek.getGamma();
				if (lastIvRead<0.1f || curIv < lastIvRead + 5f) {
					deltaRangePEAvgLtp = deltaRangePEAvgLtp + ltp;
					deltaRangePEAvgIv = deltaRangePEAvgIv + curIv;
					peDeltaOIWorth = peDeltaOIWorth + oi*delta;
					deltaRangePEAvgDelta = deltaRangePEAvgDelta + delta;
					deltaRangePEAvgGamma = deltaRangePEAvgGamma + gamma;
					deltaRangePEAvgVega = deltaRangePEAvgVega + aGreek.getVega();
					deltaRangePEWorth = deltaRangePEWorth + oi*ltp;
					deltaRangePEOI = deltaRangePEOI + oi;
					deltaRangePEDeltaOI = deltaRangePEDeltaOI + oi*delta;
					deltaRangePEGammaOI = deltaRangePEGammaOI + oi*gamma;
					lastIvRead = curIv; 
					recCount++;
				} else {
					outlierPEIvList.add(curIv);
				}
				fullcount++;
				deltaRangePEFullAvgIv = deltaRangePEFullAvgIv + curIv;
				deltaRangePEFullGamma = deltaRangePEFullGamma + gamma;
				deltaRangePEFullDeltaOI = deltaRangePEFullDeltaOI + oi* delta;
				if (delta >= 0.4f && delta <= 0.9f) {
					dr49PEAvgIV = dr49PEAvgIV + curIv;
					dr49Count++;
				}
				if (delta >= 0.1f && delta <= 0.6f) {
					dr16PEAvgIV = dr16PEAvgIV + curIv;
					dr16Count++;
				}
			}
		}
		int countPETotal = fullcount;
		int countPEOutlier = fullcount - recCount;
		
		float deltaRangeHybridPEAvgIv = 0f;
		float deltaRangeHybridPEAvgGamma = 0f;
		if ((float)recCount/(float)fullcount < 0.65f) {
			deltaRangeHybridPEAvgIv = deltaRangePEFullAvgIv/(float)fullcount;
			deltaRangeHybridPEAvgGamma = deltaRangePEFullGamma/(float)fullcount;
		} else {
			deltaRangeHybridPEAvgIv = deltaRangePEAvgIv/(float)recCount;
			deltaRangeHybridPEAvgGamma = deltaRangePEAvgGamma/(float)recCount;
		}
		
		deltaRangePEAvgLtp = deltaRangePEAvgLtp/(float)recCount;
		deltaRangePEAvgIv  = deltaRangePEAvgIv/(float)recCount;
		deltaRangePEAvgDelta =  deltaRangePEAvgDelta/(float)recCount;
		deltaRangePEAvgGamma = deltaRangePEAvgGamma/(float)recCount;
		deltaRangePEAvgVega = deltaRangePEAvgVega/(float)recCount;
		deltaRangePEWorth = deltaRangePEWorth/10000000f; // in Crores
		deltaRangePEFullAvgIv = deltaRangePEFullAvgIv/(float)fullcount;
		
		retMap.put("deltaRangePEAvgLtp", deltaRangePEAvgLtp);
		retMap.put("deltaRangePEAvgIv", deltaRangePEAvgIv);
		retMap.put("deltaRangePEAvgDelta", deltaRangePEAvgDelta);
		retMap.put("deltaRangePEAvgGamma", deltaRangePEAvgGamma);
		retMap.put("deltaRangePEAvgVega", deltaRangePEAvgVega);
		retMap.put("deltaRangePEWorth", deltaRangePEWorth);
		retMap.put("deltaRangePEOI", deltaRangePEOI/10000000f);
		retMap.put("deltaRangePEDeltaOI", deltaRangePEDeltaOI/10000000f);
		retMap.put("deltaRangePEFullDeltaOI", deltaRangePEFullDeltaOI/10000000f);
		retMap.put("deltaRangePEGammaOI", deltaRangePEGammaOI);
		retMap.put("deltaRangePEFullAvgIv", deltaRangePEFullAvgIv);
		retMap.put("deltaRangeHybridPEAvgIv", deltaRangeHybridPEAvgIv);
		retMap.put("deltaRangePEvolume1min", deltaRangePEvolume1min);
		retMap.put("deltaRangeHybridPEAvgGamma",deltaRangeHybridPEAvgGamma);
		retMap.put("deltaRangePEOutlierRatio", (float)fullcount/(float)recCount);
		retMap.put("dr49PEAvgIV",dr49PEAvgIV!=0?dr49PEAvgIV/(float)dr49Count:0);
		retMap.put("dr16PEAvgIV",dr16PEAvgIV!=0?dr16PEAvgIV/(float)dr16Count:0);
		retMap.put("countPETotal",(float) countPETotal);
		retMap.put("countPEOutlier",(float) countPEOutlier);
		retMap.put("peDeltaOIWorth", peDeltaOIWorth);
		
		
		retMap.put("fullRangePETotalIV",(float) fullPEIvList.stream().mapToDouble(d -> d).sum());
		
		retMap.put("dr16PETotalIV",(float) dr16PEIvList.stream().mapToDouble(d -> d).sum());
		retMap.put("dr49PETotalIV",(float) dr49PEIvList.stream().mapToDouble(d -> d).sum());
		retMap.put("dr46PETotalIV",(float) dr46PEIvList.stream().mapToDouble(d -> d).sum());
		retMap.put("dr4PlusPETotalIV",(float) dr4PlusPEIvList.stream().mapToDouble(d -> d).sum());
		
		retMap.put("outlierPEMinIV", outlierPEIvList.size()>0?outlierPEIvList.get(0):0f);
		retMap.put("outlierPEMaxIV", outlierPEIvList.size()>0?outlierPEIvList.get(outlierPEIvList.size()-1):0f);
		retMap.put("outlierPETotalIV",(float) outlierPEIvList.stream().mapToDouble(d -> d).sum());
		retMap.put("outlierPEAvgIV",(float) outlierPEIvList.stream().mapToDouble(d -> d).average().orElse(0.0));
		retMap.put("outlierPEMedianIV", (float) outlierPEIvList.stream().mapToDouble(d -> d).sorted().skip((outlierPEIvList.size()-1)/2).limit(2-outlierPEIvList.size()%2).average().orElse(0.0) );
		
		saveGreek(retMap);
		
		//fileLogTelegramWriter.close();
	}

	private void saveGreek(Map<String, Float> retMap) {
		Connection conn = null;
		try {
			conn = HDataSource.getConnection();
			Statement stmt = conn.createStatement();
			
			String updateSql = "UPDATE nexcorio_option_atm_movement_data set "
					
					+ "  outlierCEMinIV=" + retMap.get("outlierCEMinIV")
					+ ", outlierPEMinIV=" + retMap.get("outlierPEMinIV")
					
					+ ", outlierCEMaxIV=" + retMap.get("outlierCEMaxIV")
					+ ", outlierPEMaxIV=" + retMap.get("outlierPEMaxIV")
					
					+ ", outlierCETotalIV=" + retMap.get("outlierCETotalIV")
					+ ", outlierPETotalIV=" + retMap.get("outlierPETotalIV")
					
					+ ", outlierCEAvgIV=" + retMap.get("outlierCEAvgIV")
					+ ", outlierPEAvgIV=" + retMap.get("outlierPEAvgIV")
					
					+ ", outlierCEMedianIV=" + retMap.get("outlierCEMedianIV")
					+ ", outlierPEMedianIV=" + retMap.get("outlierPEMedianIV")

					
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
					+ " and f_main_instrument=2"
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
		List<Float> outlierCEIvList = new ArrayList<Float>();
//		outlierCEIvList.add(1f);
//		outlierCEIvList.add(2f);
//		outlierCEIvList.add(3f);
		float median = (float) outlierCEIvList.stream().mapToDouble(d -> d).sorted().skip((outlierCEIvList.size()-1)/2).limit(2-outlierCEIvList.size()%2).average().orElse(0.0);
		System.out.println(median);
	}
}
