package com.nexcorio.algo.junk;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.util.ApplicationConfig;
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

class SortbyAbsDelta implements Comparator<OptionGreek> 
{ 
    // Comparator 
    public int compare(OptionGreek a, OptionGreek b) 
    { 
    	if (Math.abs(a.getDelta()) < Math.abs(b.getDelta())) return -1;
    	else if (Math.abs(a.getDelta()) > Math.abs(b.getDelta())) return 1;
    	else return 0;
    } 
}

class SortbyOiDesc implements Comparator<OptionGreek> 
{ 
    // Comparator 
    public int compare(OptionGreek a, OptionGreek b) 
    { 
    	if (a.getOi() > b.getOi()) return -1;
    	else if (a.getOi() < b.getOi()) return 1;
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

class SortbyStrike implements Comparator<OptionGreek> 
{ 
    // Comparator 
    public int compare(OptionGreek a, OptionGreek b) 
    { 
    	if (a.getStrike() < b.getStrike()) return -1;
    	else if (a.getStrike() > b.getStrike()) return 1;
    	else return 0;
    } 
}

public class ATMThread implements Runnable {

	Long atmId;
	float underlyingInstrumentLtp;
	List<String> optionnames;
	Timestamp recTimestamp;
	FileLogTelegramWriter fileLogTelegramWriter = null;
	long mainInstrumentId;
	
	public ATMThread(Long atmId, float underlyingInstrumentLtp, List<String> optionnames, Timestamp timestamp, long mainInstrumentId) {
		super();
		this.atmId = atmId;
		this.optionnames = optionnames;
		this.underlyingInstrumentLtp = underlyingInstrumentLtp;
		this.recTimestamp = timestamp;
		this.mainInstrumentId = mainInstrumentId;
		Thread t = new Thread(this, atmId+"");
		t.setPriority(Thread.MAX_PRIORITY);
		t.start();
	}
	

	private void balanceDelta(List<OptionGreek> selectedCEGreeks, List<OptionGreek> selectedPEGreeks) {
		List<OptionGreek> firstList = selectedCEGreeks.size() < selectedPEGreeks.size() ? selectedCEGreeks:selectedPEGreeks;
		List<OptionGreek> secondList = selectedCEGreeks.size() >= selectedPEGreeks.size() ? selectedCEGreeks:selectedPEGreeks;
		
		for(int i=0;i<firstList.size();i++) {
			
		}
		
		
	}
	
	@Override
	public void run() {
		Calendar cal = Calendar.getInstance();
		cal.setTime(recTimestamp);
		fileLogTelegramWriter = new FileLogTelegramWriter("NIFTY", this.atmId+"", cal);
		//System.out.println("Run reached");
		
		List<OptionGreek> ceOptionGreeks = new ArrayList<>();
		List<OptionGreek> peOptionGreeks = new ArrayList<>();
		for(String optionname:optionnames ) {
			OptionGreek aGreek = getOptionGreeks(optionname, 0);
			if (aGreek!=null) {
				if (optionname.endsWith("CE")) {
					ceOptionGreeks.add(aGreek);
				} else {
					peOptionGreeks.add(aGreek);
				}
			}
		}
		
		List<OptionGreek> prevCeOptionGreeks = new ArrayList<OptionGreek>();
		List<OptionGreek> prevPeOptionGreeks = new ArrayList<OptionGreek>();
		for(String optionname:optionnames ) {
			OptionGreek aGreek = getOptionGreeks(optionname, -5);
			if (aGreek!=null) {
				if (aGreek.getTradingSymbol().endsWith("CE")) prevCeOptionGreeks.add(aGreek);
				else prevPeOptionGreeks.add(aGreek);
			}
		}
				
		Collections.sort(ceOptionGreeks, new SortbyIV());
		Collections.sort(peOptionGreeks, new SortbyIV());
		
		Map<String, OptionGreek> prevCeOptionGreeksMap = prevCeOptionGreeks.stream().collect(Collectors.toMap(OptionGreek::getTradingSymbol, item -> item));
		Map<String, OptionGreek> prevPeOptionGreeksMap = prevPeOptionGreeks.stream().collect(Collectors.toMap(OptionGreek::getTradingSymbol, item -> item));
		
		Map<String, Float> retMap = new HashMap<>();
		
		
		List<OptionGreek> selectedCEGreeks = new ArrayList<OptionGreek>();
		float lastIvRead = 0f;
		
		for(OptionGreek aGreek: ceOptionGreeks) {
			float delta = Math.abs(aGreek.getDelta());
			
			if (delta >= 0.1f && delta <= 0.9f) {
				selectedCEGreeks.add(0,aGreek);
			}
		}
		List<OptionGreek> selectedPEGreeks = new ArrayList<OptionGreek>();
		lastIvRead = 0f;
		for(OptionGreek aGreek: peOptionGreeks) {
			float delta = Math.abs(aGreek.getDelta());
			
			if (delta >= 0.1f && delta <= 0.9f) {
				selectedPEGreeks.add(0,aGreek);
			}
		}
		balanceDelta(selectedCEGreeks, selectedPEGreeks);
		while(selectedCEGreeks.size() != selectedPEGreeks.size()) {
			if (selectedCEGreeks.size() > selectedPEGreeks.size() ) {
				selectedCEGreeks.remove(0);
			} else {
				selectedPEGreeks.remove(0);
			}
		}
		retMap.put("tmpaccmlcetheta", (float) selectedCEGreeks.stream().mapToDouble(d -> d.getIv()).average().orElse(0.0));
		retMap.put("tmpaccmlpetheta", (float) selectedPEGreeks.stream().mapToDouble(d -> d.getIv()).average().orElse(0.0));
		
		float changeInCETheta = 0f;
		float changeInPETheta = 0f;
		
		for(OptionGreek aGreek : selectedCEGreeks) {
			changeInCETheta = changeInCETheta +  (prevCeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null?aGreek.getTheta()-prevCeOptionGreeksMap.get(aGreek.getTradingSymbol()).getTheta():0f);
		}
		for(OptionGreek aGreek : selectedPEGreeks) {
			changeInPETheta = changeInPETheta +  (prevPeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null?aGreek.getTheta()-prevPeOptionGreeksMap.get(aGreek.getTradingSymbol()).getTheta():0f);
		}
		retMap.put("tmpaccmlcetheta", changeInCETheta);
		retMap.put("tmpaccmlpetheta", changeInPETheta);
		//System.out.println("Sort done");
		
		
		
		// ITM till 0.9 delta  Avg IV of same number of strikes
		List<OptionGreek> drITMWhlStrkCEIvs = new ArrayList<OptionGreek>();
		List<OptionGreek> drITMWhlStrkPEIvs = new ArrayList<OptionGreek>();
		
		List<OptionGreek> above5WhlStrkCEIvs = new ArrayList<OptionGreek>();
		List<OptionGreek> above5WhlStrkPEIvs = new ArrayList<OptionGreek>();
		
		for(OptionGreek aGreek: ceOptionGreeks) {
			float delta = Math.abs(aGreek.getDelta());
			
			if (aGreek.getStrike()%100==0) {
				drITMWhlStrkCEIvs.add(aGreek);
				if (delta > 0.5f && delta < 0.8f) above5WhlStrkCEIvs.add(aGreek);
			}
		}
		for(OptionGreek aGreek: peOptionGreeks) {
			float delta = Math.abs(aGreek.getDelta());
			
			if (aGreek.getStrike()%100==0) {
				drITMWhlStrkPEIvs.add(aGreek);
				if (delta > 0.5f && delta < 0.8f) above5WhlStrkPEIvs.add(aGreek);
			}
		}
		int optimalLength =  drITMWhlStrkCEIvs.size() < drITMWhlStrkPEIvs.size() ?	drITMWhlStrkCEIvs.size() : drITMWhlStrkPEIvs.size();
		
		
		int above5OptimalLength =  above5WhlStrkCEIvs.size() < above5WhlStrkPEIvs.size() ?	above5WhlStrkCEIvs.size() : above5WhlStrkPEIvs.size();
		
		float drITMWhlStrkCEAvgIv = 0f;
		float drITMWhlStrkPEAvgIv = 0f;
		
		float drITMWhlStrkCEAvgGamma = 0f;
		float drITMWhlStrkPEAvgGamma = 0f;
		
		float drITMWhlStrkCEAvgVega = 0f;
		float drITMWhlStrkPEAvgVega = 0f;
		
		for(int i=0;i<optimalLength;i++) {
			drITMWhlStrkCEAvgIv = drITMWhlStrkCEAvgIv + drITMWhlStrkCEIvs.get(i).getIv();
			drITMWhlStrkPEAvgIv = drITMWhlStrkPEAvgIv + drITMWhlStrkPEIvs.get(i).getIv();
			
			drITMWhlStrkCEAvgGamma = drITMWhlStrkCEAvgGamma + drITMWhlStrkCEIvs.get(i).getGamma();
			drITMWhlStrkPEAvgGamma = drITMWhlStrkPEAvgGamma + drITMWhlStrkPEIvs.get(i).getGamma();
			
			drITMWhlStrkCEAvgVega = drITMWhlStrkCEAvgVega + drITMWhlStrkCEIvs.get(i).getVega();
			drITMWhlStrkPEAvgVega = drITMWhlStrkPEAvgVega + drITMWhlStrkPEIvs.get(i).getVega();
		}
		
		float above5WhlStrkCEAvgIv = 0f;
		float above5WhlStrkPEAvgIv = 0f;
		
		float above5WhlStrkCEAvgGama = 0f;
		float above5WhlStrkPEAvgGama = 0f;
		
		for(int i=0;i<above5OptimalLength;i++) {
			above5WhlStrkCEAvgIv = above5WhlStrkCEAvgIv + above5WhlStrkCEIvs.get(i).getIv();
			above5WhlStrkPEAvgIv = above5WhlStrkPEAvgIv + above5WhlStrkPEIvs.get(i).getIv();	
			
			above5WhlStrkCEAvgGama = above5WhlStrkCEAvgGama + above5WhlStrkCEIvs.get(i).getGamma();
			above5WhlStrkPEAvgGama = above5WhlStrkPEAvgGama + above5WhlStrkPEIvs.get(i).getGamma();
			
		}
		drITMWhlStrkCEAvgIv = drITMWhlStrkCEAvgIv/optimalLength;
		drITMWhlStrkPEAvgIv = drITMWhlStrkPEAvgIv/optimalLength;
		
		drITMWhlStrkCEAvgGamma = drITMWhlStrkCEAvgGamma/optimalLength;
		drITMWhlStrkPEAvgGamma = drITMWhlStrkPEAvgGamma/optimalLength;
		
		drITMWhlStrkCEAvgVega = drITMWhlStrkCEAvgVega/optimalLength;
		drITMWhlStrkPEAvgVega = drITMWhlStrkPEAvgVega/optimalLength;
		
		above5WhlStrkCEAvgIv = above5WhlStrkCEAvgIv/above5OptimalLength;
		above5WhlStrkPEAvgIv = above5WhlStrkPEAvgIv/above5OptimalLength;
		
		above5WhlStrkCEAvgGama = above5WhlStrkCEAvgGama/above5OptimalLength;
		above5WhlStrkPEAvgGama = above5WhlStrkPEAvgGama/above5OptimalLength;
		
		retMap.put("drITMWhlStrkSameSizeCEAvgIv", drITMWhlStrkCEAvgIv);
		retMap.put("drITMWhlStrkSameSizePEAvgIv", drITMWhlStrkPEAvgIv);
		
		retMap.put("drITMWhlStrkSameSizeCEAvgGamma", drITMWhlStrkCEAvgGamma);
		retMap.put("drITMWhlStrkSameSizePEAvgGamma", drITMWhlStrkPEAvgGamma);
		
		retMap.put("drITMWhlStrkSameSizeCEAvgVega", drITMWhlStrkCEAvgVega);
		retMap.put("drITMWhlStrkSameSizePEAvgVega", drITMWhlStrkPEAvgVega);
		
		retMap.put("above5WhlStrkCEAvgIv", above5WhlStrkCEAvgIv);
		retMap.put("above5WhlStrkPEAvgIv", above5WhlStrkPEAvgIv);
		
		retMap.put("above5WhlStrkCEAvgGama", above5WhlStrkCEAvgGama);
		retMap.put("above5WhlStrkPEAvgGama", above5WhlStrkPEAvgGama);
		
		float altAbove5WhlStrkCEAvgIv = 0f;
		float altAbove5WhlStrkPEAvgIv = 0f;
		
		
		while(above5WhlStrkCEIvs.size() != above5WhlStrkPEIvs.size()) {
			if (above5WhlStrkCEIvs.size() > above5WhlStrkPEIvs.size() ) {
				above5WhlStrkCEIvs.remove(0);
			} else {
				above5WhlStrkPEIvs.remove(0);
			}
		}
		float altAbove5WhlStrk5secCETheta = 0f;
		float altAbove5WhlStrk5secPETheta = 0f;
		for(int i=0;i<above5WhlStrkPEIvs.size();i++) {
			altAbove5WhlStrkCEAvgIv = altAbove5WhlStrkCEAvgIv + above5WhlStrkCEIvs.get(i).getIv();//Math.abs(above5WhlStrkCEIvs.get(i).getDelta());
			altAbove5WhlStrkPEAvgIv = altAbove5WhlStrkPEAvgIv + above5WhlStrkPEIvs.get(i).getIv();//Math.abs(above5WhlStrkPEIvs.get(i).getDelta());
			
			for(OptionGreek prevGreek: prevCeOptionGreeks) {
				if (above5WhlStrkCEIvs.get(i).getTradingSymbol().equals(prevGreek.getTradingSymbol())) {
					altAbove5WhlStrk5secCETheta = altAbove5WhlStrk5secCETheta + (Math.abs(above5WhlStrkCEIvs.get(i).getTheta()) - Math.abs(prevGreek.getTheta()));
				}
			}
			for(OptionGreek prevGreek: prevPeOptionGreeks) {
				if (above5WhlStrkPEIvs.get(i).getTradingSymbol().equals(prevGreek.getTradingSymbol())) {
					altAbove5WhlStrk5secPETheta = altAbove5WhlStrk5secPETheta + (Math.abs(above5WhlStrkPEIvs.get(i).getTheta()) - Math.abs(prevGreek.getTheta()));
				}
			}
		}
		altAbove5WhlStrkCEAvgIv = altAbove5WhlStrkCEAvgIv/(float)above5WhlStrkPEIvs.size();
		altAbove5WhlStrkPEAvgIv = altAbove5WhlStrkPEAvgIv/(float)above5WhlStrkPEIvs.size();
		
		retMap.put("altAbove5WhlStrkCEAvgIv", altAbove5WhlStrkCEAvgIv);
		retMap.put("altAbove5WhlStrkPEAvgIv", altAbove5WhlStrkPEAvgIv);
		
		retMap.put("altAbove5WhlStrk5secCETheta",altAbove5WhlStrk5secCETheta);
		retMap.put("altAbove5WhlStrk5secPETheta",altAbove5WhlStrk5secPETheta);
		
		lastIvRead = 0f;
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
		
		int ceDelta1_2Count = 0;
		int ceDelta2_8Count = 0;
		int ceDelta8_9Count = 0;
		
		float lastNonOutlierCEDelta = 0f;
		
		List<Float> fullCEIvList = new ArrayList<Float>();
		
		float ceOutlierStrike = 0f;
		
		List<OptionGreek> dr16CEIvList = new ArrayList<OptionGreek>();
		List<OptionGreek> dr49CEIvList = new ArrayList<OptionGreek>();
		List<OptionGreek> dr46CEIvList = new ArrayList<OptionGreek>();
		List<Float> dr4PlusCEIvList = new ArrayList<Float>();
		List<Float> outlierCEIvList = new ArrayList<Float>();
		
		StringBuffer ceGreekDetails = new StringBuffer();
		List<OptionGreek> dr19WholeStrikeCEIvList = new ArrayList<OptionGreek>();
		float wholeStrikeCEDeltaOI = 0f;
		int limitedOutlierCEIvCount = 0;
		Set<Integer> nonOutlierCEStrikes = new HashSet<Integer>();
		for(OptionGreek aGreek: ceOptionGreeks) {
			float delta = Math.abs(aGreek.getDelta());
			fullCEIvList.add(aGreek.getIv());
			
			if (delta >= 0.1f && delta <= 0.6f) dr16CEIvList.add(aGreek);
			if (delta >= 0.4f && delta <= 0.9f) dr49CEIvList.add(aGreek);
			if (delta >= 0.4f && delta <= 0.6f) dr46CEIvList.add(aGreek);
			if (delta >= 0.4f ) dr4PlusCEIvList.add(aGreek.getIv());
			
			if (delta >= lowerDelta && delta <= upperDelta) {
				
				float curIv = aGreek.getIv();
				float ltp = aGreek.getLtp();
				float oi = aGreek.getOi();
				float gamma = aGreek.getGamma();
				
				if (lastIvRead<0.1f || curIv < lastIvRead + 5f) {
					ceGreekDetails.append(" " + aGreek.getTradingSymbol()+"[D:"+aGreek.getDelta()+",IV:"+aGreek.getIv()+",G:"+aGreek.getGamma()+",LTP:"+aGreek.getLtp());
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
					lastNonOutlierCEDelta = delta;
					nonOutlierCEStrikes.add(getStrike(aGreek.getTradingSymbol()));
					ceOutlierStrike = getStrike(aGreek.getTradingSymbol());
				} else {
					if (outlierCEIvList.size()==0) ceOutlierStrike = getStrike(aGreek.getTradingSymbol());
					ceGreekDetails.append(">><<" + aGreek.getTradingSymbol()+"[D:"+aGreek.getDelta()+",IV:"+aGreek.getIv()+",G:"+aGreek.getGamma()+",LTP:"+aGreek.getLtp());
					outlierCEIvList.add(curIv);
					if (delta >= 0.2f && delta <= 0.8f) {
						limitedOutlierCEIvCount++;
					}
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
				if (getStrike(aGreek.getTradingSymbol())%100 == 0) { // Strike ends with 00 (full number, no 50s) 
					dr19WholeStrikeCEIvList.add(aGreek);
					wholeStrikeCEDeltaOI = wholeStrikeCEDeltaOI + oi* delta;
				}
				if (delta >= 0.1f && delta < 0.2f) {
					ceDelta1_2Count++;
				} else if (delta >= 0.2f && delta < 0.8f) {
					ceDelta2_8Count++;
				} else {
					ceDelta8_9Count++;
				}
			}
		}
		
		retMap.put("ceDelta1_2Count", (float) ceDelta1_2Count);
		retMap.put("ceDelta2_8Count", (float) ceDelta2_8Count);
		retMap.put("ceDelta8_9Count", (float) ceDelta8_9Count);
		
		lastIvRead = 0f;
		int ceRec=0;
		int ceFullRec=0;
		List<Float> refCEIvDiffs = new ArrayList<Float>();
		for(OptionGreek aGreek: ceOptionGreeks) {
			float delta = Math.abs(aGreek.getDelta());
			if (delta >= lowerDelta && delta <= upperDelta) {
				float curIv = aGreek.getIv();				
				if (lastIvRead<0.1f || curIv < lastIvRead + 2f) {
					ceRec++;
					lastIvRead = curIv;	
				}
				ceFullRec++;
			}
		}
		lastIvRead = 0f;
		int peRec = 0;
		int peFullRec = 0;
		List<Float> refPEIvDiffs = new ArrayList<Float>();
		for(OptionGreek aGreek: peOptionGreeks) {
			float delta = Math.abs(aGreek.getDelta());
			if (delta >= lowerDelta && delta <= upperDelta) {
				
				float curIv = aGreek.getIv();				
				if (lastIvRead<0.1f || curIv < lastIvRead + 2f) {
					peRec++;
					lastIvRead = curIv;	
				}
				peFullRec++;
			}
		}
		retMap.put("newCountCEOutlier",(float) (ceFullRec-ceRec));
		retMap.put("newCountPEOutlier",(float) (peFullRec-peRec));
//		retMap.put("avgCEIVDiff",(float) refCEIvDiffs.stream().mapToDouble(d -> d).average().orElse(0.0));
//		retMap.put("avgPEIVDiff",(float) refPEIvDiffs.stream().mapToDouble(d -> d).average().orElse(0.0));
		
		
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
		
		retMap.put("ceOutlierStrike", ceOutlierStrike);
		retMap.put("ceOutlierStrikeDistance", (this.underlyingInstrumentLtp - ceOutlierStrike));
		
		retMap.put("fullRangeCETotalIV",(float) fullCEIvList.stream().mapToDouble(d -> d).sum());
		
		retMap.put("dr16CETotalIV",(float) dr16CEIvList.stream().mapToDouble(d -> d.getIv()).sum());
		retMap.put("dr49CETotalIV",(float) dr49CEIvList.stream().mapToDouble(d -> d.getIv()).sum());
		retMap.put("dr46CETotalIV",(float) dr46CEIvList.stream().mapToDouble(d -> d.getIv()).sum());
		retMap.put("dr4PlusCETotalIV",(float) dr4PlusCEIvList.stream().mapToDouble(d -> d).sum());
		
		retMap.put("outlierCEMinIV", outlierCEIvList.size()>0?outlierCEIvList.get(0):0f);
		retMap.put("outlierCEMaxIV", outlierCEIvList.size()>0?outlierCEIvList.get(outlierCEIvList.size()-1):0f);
		retMap.put("outlierCETotalIV",(float) outlierCEIvList.stream().mapToDouble(d -> d).sum());
		retMap.put("outlierCEAvgIV",(float) outlierCEIvList.stream().mapToDouble(d -> d).average().orElse(0.0));
		retMap.put("outlierCEMedianIV", (float) outlierCEIvList.stream().mapToDouble(d -> d).sorted().skip((outlierCEIvList.size()-1)/2).limit(2-outlierCEIvList.size()%2).average().orElse(0.0) );
		
		retMap.put("dr19WholeStrikeCEAvgIV",(float) dr19WholeStrikeCEIvList.stream().mapToDouble(d -> d.getIv()).average().orElse(0.0));
		retMap.put("wholeStrikeCEDeltaOI", wholeStrikeCEDeltaOI/10000000f);
		retMap.put("limitedOutlierCEIvCount", (float) limitedOutlierCEIvCount);
		retMap.put("dr19WholeStrikeCELtp",(float) dr19WholeStrikeCEIvList.stream().mapToDouble(d -> d.getVega()).sum());
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
		
		int peDelta1_2Count = 0;
		int peDelta2_8Count = 0;
		int peDelta8_9Count = 0;
		
		List<Float> fullPEIvList = new ArrayList<Float>();
		float peOutlierStrike = 0f;
		List<OptionGreek> dr16PEIvList = new ArrayList<OptionGreek>();
		List<OptionGreek> dr49PEIvList = new ArrayList<OptionGreek>();
		List<OptionGreek> dr46PEIvList = new ArrayList<OptionGreek>();
		List<Float> dr4PlusPEIvList = new ArrayList<Float>();
		
		List<Float> outlierPEIvList = new ArrayList<Float>();
		StringBuffer peGreekDetails = new StringBuffer();
		
		List<OptionGreek> dr19WholeStrikePEIvList = new ArrayList<OptionGreek>();
		float wholeStrikePEDeltaOI = 0f;
		int limitedOutlierPEIvCount = 0;
		float lastNonOutlierPEDelta =0f;
		Set<Integer> nonOutlierPEStrikes = new HashSet<Integer>();
		for(OptionGreek aGreek: peOptionGreeks) {
			float delta = Math.abs(aGreek.getDelta());		
			fullPEIvList.add(aGreek.getIv());
			
			if (delta >= 0.1f && delta <= 0.6f) dr16PEIvList.add(aGreek);
			if (delta >= 0.4f && delta <= 0.9f) dr49PEIvList.add(aGreek);
			if (delta >= 0.4f && delta <= 0.6f) dr46PEIvList.add(aGreek);
			if (delta >= 0.4f ) dr4PlusPEIvList.add(aGreek.getIv());
			
			if (delta >= lowerDelta && delta <= upperDelta) {
				float curIv = aGreek.getIv();						
				float ltp = aGreek.getLtp();
				float oi = aGreek.getOi();				
				float gamma = aGreek.getGamma();
				if (lastIvRead<0.1f || curIv < lastIvRead + 5f) {
					peGreekDetails.append(" " + aGreek.getTradingSymbol()+"[D:"+aGreek.getDelta()+",IV:"+aGreek.getIv()+",G:"+aGreek.getGamma()+",LTP:"+aGreek.getLtp());
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
					lastNonOutlierPEDelta = delta;
					nonOutlierPEStrikes.add(getStrike(aGreek.getTradingSymbol()));
					peOutlierStrike = getStrike(aGreek.getTradingSymbol());
				} else {
					if (outlierPEIvList.size()==0) peOutlierStrike = getStrike(aGreek.getTradingSymbol());
					peGreekDetails.append(">><<" + aGreek.getTradingSymbol()+",D:"+aGreek.getDelta()+",IV:"+aGreek.getIv()+",G:"+aGreek.getGamma()+",LTP:"+aGreek.getLtp());
					outlierPEIvList.add(curIv);
					if (delta >= 0.2f && delta <= 0.8f) {
						limitedOutlierPEIvCount++;
					}
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
				if (getStrike(aGreek.getTradingSymbol())%100 == 0) { // Strike ends with 00 (full number, no 50s) 
					dr19WholeStrikePEIvList.add(aGreek);
					wholeStrikePEDeltaOI = wholeStrikePEDeltaOI + oi* delta;
				}
				if (delta >= 0.1f && delta < 0.2f) {
					peDelta1_2Count++;
				} else if (delta >= 0.2f && delta < 0.8f) {
					peDelta2_8Count++;
				} else {
					peDelta8_9Count++;
				}
			}
		}
		
		retMap.put("peDelta1_2Count", (float) peDelta1_2Count);
		retMap.put("peDelta2_8Count", (float) peDelta2_8Count);
		retMap.put("peDelta8_9Count", (float) peDelta8_9Count);
		
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
		
		retMap.put("peOutlierStrike", peOutlierStrike);
		retMap.put("peOutlierStrikeDistance", (peOutlierStrike - this.underlyingInstrumentLtp));
		
		retMap.put("fullRangePETotalIV",(float) fullPEIvList.stream().mapToDouble(d -> d).sum());
		
		retMap.put("dr16PETotalIV",(float) dr16PEIvList.stream().mapToDouble(d -> d.getIv()).sum());
		retMap.put("dr49PETotalIV",(float) dr49PEIvList.stream().mapToDouble(d -> d.getIv()).sum());
		retMap.put("dr46PETotalIV",(float) dr46PEIvList.stream().mapToDouble(d -> d.getIv()).sum());
		retMap.put("dr4PlusPETotalIV",(float) dr4PlusPEIvList.stream().mapToDouble(d -> d).sum());
		
		retMap.put("outlierPEMinIV", outlierPEIvList.size()>0?outlierPEIvList.get(0):0f);
		retMap.put("outlierPEMaxIV", outlierPEIvList.size()>0?outlierPEIvList.get(outlierPEIvList.size()-1):0f);
		retMap.put("outlierPETotalIV",(float) outlierPEIvList.stream().mapToDouble(d -> d).sum());
		retMap.put("outlierPEAvgIV",(float) outlierPEIvList.stream().mapToDouble(d -> d).average().orElse(0.0));
		retMap.put("outlierPEMedianIV", (float) outlierPEIvList.stream().mapToDouble(d -> d).sorted().skip((outlierPEIvList.size()-1)/2).limit(2-outlierPEIvList.size()%2).average().orElse(0.0) );
		retMap.put("dr19WholeStrikePEAvgIV",(float) dr19WholeStrikePEIvList.stream().mapToDouble(d -> d.getIv()).average().orElse(0.0));
		retMap.put("wholeStrikePEDeltaOI", wholeStrikePEDeltaOI/10000000f);
		retMap.put("limitedOutlierPEIvCount", (float) limitedOutlierPEIvCount);
		retMap.put("dr19WholeStrikePELtp",(float) dr19WholeStrikePEIvList.stream().mapToDouble(d -> d.getVega()).sum());
		// Calculate adjusted Adjust Ltp, IV and greeks
		OptionGreek[] adjustedATMGreeks = getExactATMQuandrangle(ceOptionGreeks, peOptionGreeks, 0.5f);
		OptionGreek adjustedATMCEGreek = adjustedATMGreeks[0];
		OptionGreek adjustedATMPEGreek = adjustedATMGreeks[1];
		
		float lowestDelta = lastNonOutlierCEDelta < lastNonOutlierPEDelta?lastNonOutlierCEDelta:lastNonOutlierPEDelta;
		
		List<OptionGreek> restrcitedCEList = new ArrayList<OptionGreek>();
		for(OptionGreek aGreek: ceOptionGreeks) {
			float delta = Math.abs(aGreek.getDelta());
			if (delta >= lowestDelta) {
				restrcitedCEList.add(aGreek);
			}
		}
		List<OptionGreek> restrcitedPEList = new ArrayList<OptionGreek>();
		for(OptionGreek aGreek: peOptionGreeks) {
			float delta = Math.abs(aGreek.getDelta());
			if (delta >= lowestDelta) {
				restrcitedPEList.add(aGreek);
			}
		}
		retMap.put("resDeltaRangeCEAvgIv",(float) restrcitedCEList.stream().mapToDouble(d -> d.getIv()).average().orElse(0.0));
		retMap.put("resDeltaRangePEAvgIv",(float) restrcitedPEList.stream().mapToDouble(d -> d.getIv()).average().orElse(0.0));
		
		
		// Gamma exposure
		Map<Integer, Float> gammaPerStrike = new HashMap<Integer, Float>();
		for(OptionGreek aGreek: ceOptionGreeks) {
			float gammaExposure = aGreek.getOi()*aGreek.getGamma();
			int strike = getStrike(aGreek.getTradingSymbol());
			gammaExposure = gammaExposure + (gammaPerStrike.get(strike)!=null?gammaPerStrike.get(strike):0f);
			gammaPerStrike.put(strike, gammaExposure);
		}
		
		for(OptionGreek aGreek: peOptionGreeks) {
			float gammaExposure = aGreek.getOi()*aGreek.getGamma();
			int strike = getStrike(aGreek.getTradingSymbol());
			gammaExposure = gammaExposure - (gammaPerStrike.get(strike)!=null?gammaPerStrike.get(strike):0f);
			gammaPerStrike.put(strike, gammaExposure);
		}
		
		// Convert HashMap entries to a List
        List<Map.Entry<Integer, Float>> entryList = new ArrayList<>(gammaPerStrike.entrySet());

        // Sort the List by value in ascending order
        Collections.sort(entryList, (entry1, entry2) -> entry1.getValue().compareTo(entry2.getValue()));

        // Create a LinkedHashMap to store the sorted entries
        LinkedHashMap<Integer, Float> sortedMap = new LinkedHashMap<>();
        for (Map.Entry<Integer, Float> entry : entryList) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }
        
        float minGammaExposure = Float.MAX_VALUE;
        float maxGammaExposure = Float.MIN_VALUE;
        float netGammaExposure = 0f;
        
        int minGammaExposureStrike = 0;
        int maxGammaExposureStrike = 0;
        
		Iterator<Integer> iter = sortedMap.keySet().iterator();
		while (iter.hasNext()) {
			int strike = iter.next();
			if (gammaPerStrike.get(strike) < -1000 || gammaPerStrike.get(strike) > 1000) {
				if (gammaPerStrike.get(strike) < minGammaExposure) {
					minGammaExposure = gammaPerStrike.get(strike);
					minGammaExposureStrike = strike;
				}
				if (gammaPerStrike.get(strike) > maxGammaExposure) {
					maxGammaExposure = gammaPerStrike.get(strike);
					maxGammaExposureStrike = strike;
				}
				fileLogTelegramWriter.write(" For Strike " + strike + " gamma exposure " + gammaPerStrike.get(strike));
			}
			netGammaExposure = netGammaExposure + gammaPerStrike.get(strike);
		}
		retMap.put("minGammaExposure", Math.abs(minGammaExposure));
		retMap.put("maxGammaExposure",maxGammaExposure);
		retMap.put("netGammaExposure",netGammaExposure);
		
		retMap.put("minGammaExposureStrike", (float) minGammaExposureStrike);
		retMap.put("maxGammaExposureStrike", (float) maxGammaExposureStrike);
		
		
		
		
		// Gamma exposure with strike distance
		Map<Integer, Float> gammaPerStrikeDistance = new HashMap<Integer, Float>();
		for(OptionGreek aGreek: ceOptionGreeks) {
			int strike = getStrike(aGreek.getTradingSymbol());
			float gammaExposure = aGreek.getOi()*aGreek.getGamma()*((strike-this.underlyingInstrumentLtp)/2f);
			gammaExposure = gammaExposure + (gammaPerStrikeDistance.get(strike)!=null?gammaPerStrikeDistance.get(strike):0f);
			gammaPerStrikeDistance.put(strike, gammaExposure);
		}
		
		for(OptionGreek aGreek: peOptionGreeks) {
			int strike = getStrike(aGreek.getTradingSymbol());
			float gammaExposure = aGreek.getOi()*aGreek.getGamma()*((this.underlyingInstrumentLtp-strike)/2f);
			gammaExposure = gammaExposure - (gammaPerStrikeDistance.get(strike)!=null?gammaPerStrikeDistance.get(strike):0f);
			gammaPerStrikeDistance.put(strike, gammaExposure);
		}
		
		// Convert HashMap entries to a List
        List<Map.Entry<Integer, Float>> entryList2 = new ArrayList<>(gammaPerStrikeDistance.entrySet());

        // Sort the List by value in ascending order
        Collections.sort(entryList2, (entry1, entry2) -> entry1.getValue().compareTo(entry2.getValue()));

        // Create a LinkedHashMap to store the sorted entries
        LinkedHashMap<Integer, Float> sortedMap2 = new LinkedHashMap<>();
        for (Map.Entry<Integer, Float> entry : entryList2) {
            sortedMap2.put(entry.getKey(), entry.getValue());
        }
        
        float minGammaExposureStrikeDistance = Float.MAX_VALUE;
        float maxGammaExposureStrikeDistance = Float.MIN_VALUE;
        float netGammaExposureStrikeDistance = 0f;
        
        float minGammaExposure0_250StrikeDistance = Float.MAX_VALUE;
        float maxGammaExposure0_250StrikeDistance = Float.MIN_VALUE;
        
        float minGammaExposure0_500StrikeDistance = Float.MAX_VALUE;
        float maxGammaExposure0_500StrikeDistance = Float.MIN_VALUE;
        
        
		iter = sortedMap2.keySet().iterator();
		while (iter.hasNext()) {
			int strike = iter.next();
			
			if (gammaPerStrikeDistance.get(strike) < minGammaExposureStrikeDistance) {
				minGammaExposureStrikeDistance = gammaPerStrikeDistance.get(strike);
			}
			if (gammaPerStrikeDistance.get(strike) > maxGammaExposureStrikeDistance) {
				maxGammaExposureStrikeDistance = gammaPerStrikeDistance.get(strike);
			}
			fileLogTelegramWriter.write(" For Strike " + strike + " strike distance gamma exposure " + gammaPerStrikeDistance.get(strike));
		
			netGammaExposureStrikeDistance = netGammaExposureStrikeDistance + gammaPerStrikeDistance.get(strike);
			
			if ((strike > this.underlyingInstrumentLtp + 250 && strike < this.underlyingInstrumentLtp + 500)
					|| (strike < this.underlyingInstrumentLtp - 250 && strike > this.underlyingInstrumentLtp - 500) ){
				if (gammaPerStrikeDistance.get(strike) < minGammaExposure0_250StrikeDistance) {
					minGammaExposure0_250StrikeDistance = gammaPerStrikeDistance.get(strike);
				}
				if (gammaPerStrikeDistance.get(strike) > maxGammaExposure0_250StrikeDistance) {
					maxGammaExposure0_250StrikeDistance = gammaPerStrikeDistance.get(strike);
				}
			}
			if (strike < this.underlyingInstrumentLtp + 500 && strike > this.underlyingInstrumentLtp - 500) {
				if (gammaPerStrikeDistance.get(strike) < minGammaExposure0_500StrikeDistance) {
					minGammaExposure0_500StrikeDistance = gammaPerStrikeDistance.get(strike);
				}
				if (gammaPerStrikeDistance.get(strike) > maxGammaExposure0_500StrikeDistance) {
					maxGammaExposure0_500StrikeDistance = gammaPerStrikeDistance.get(strike);
				}
			}
			
		}
		retMap.put("minGammaExposureWithStrike", Math.abs(minGammaExposureStrikeDistance)/1000f);
		retMap.put("maxGammaExposureWithStrike",maxGammaExposureStrikeDistance/1000f);
		retMap.put("netGammaExposureWithStrike",netGammaExposureStrikeDistance);
		
		retMap.put("minGammaExposure0_250StrikeDistance", Math.abs(minGammaExposure0_250StrikeDistance)/1000f);
		retMap.put("maxGammaExposure0_250StrikeDistance", Math.abs(maxGammaExposure0_250StrikeDistance)/1000f);
		
		retMap.put("minGammaExposure0_500StrikeDistance", Math.abs(minGammaExposure0_500StrikeDistance)/1000f);
		retMap.put("maxGammaExposure0_500StrikeDistance", Math.abs(maxGammaExposure0_500StrikeDistance)/1000f);
		
		// Excluding outlier Gamma exposure with strike distance
		minGammaExposureStrikeDistance = Float.MAX_VALUE;
        maxGammaExposureStrikeDistance = Float.MIN_VALUE;
        netGammaExposureStrikeDistance = 0f;
        
		iter = sortedMap2.keySet().iterator();
		while (iter.hasNext()) {
			int strike = iter.next();
			if (nonOutlierCEStrikes.contains(strike) && nonOutlierPEStrikes.contains(strike)) {
				if (gammaPerStrikeDistance.get(strike) < minGammaExposureStrikeDistance) {
					minGammaExposureStrikeDistance = gammaPerStrikeDistance.get(strike);
				}
				if (gammaPerStrikeDistance.get(strike) > maxGammaExposureStrikeDistance) {
					maxGammaExposureStrikeDistance = gammaPerStrikeDistance.get(strike);
				}
				fileLogTelegramWriter.write(" For Strike " + strike + " strike distance gamma exposure " + gammaPerStrikeDistance.get(strike));
			
				netGammaExposureStrikeDistance = netGammaExposureStrikeDistance + gammaPerStrikeDistance.get(strike);
			}
		}
		retMap.put("minGameXpWithStrikeXoutlier", Math.abs(minGammaExposureStrikeDistance)/1000f);
		retMap.put("maxGameXpWithStrikeXoutlier",maxGammaExposureStrikeDistance/1000f);
		retMap.put("netGameXpWithStrikeXoutlier",netGammaExposureStrikeDistance);
		
		
		
		// Selective 5
		float instrumentLtp = underlyingInstrumentLtp; 
		int upperBound = (int) (instrumentLtp + 250);
		int lowerBound = (int) (instrumentLtp - 250);
		int ceRecCount = 0;
		float avgCEGamma = 0f;
		for(OptionGreek aGreek: ceOptionGreeks) {
			int strike = getStrike(aGreek.getTradingSymbol());
			if (strike >=lowerBound && strike <= upperBound ) {
				avgCEGamma = avgCEGamma + aGreek.getGamma();
				ceRecCount++;
			}
		}
		avgCEGamma = avgCEGamma/(float)ceRecCount;
		retMap.put("selective5AvgCEGamma",avgCEGamma);
		int peRecCount = 0;
		float avgPEGamma = 0f;
		for(OptionGreek aGreek: ceOptionGreeks) {
			int strike = getStrike(aGreek.getTradingSymbol());
			if (strike >=lowerBound && strike <= upperBound ) {
				avgPEGamma = avgPEGamma + aGreek.getGamma();
				peRecCount++;
			}
		}
		avgPEGamma = avgPEGamma/(float)peRecCount;
		retMap.put("selective5AvgPEGamma",avgPEGamma);
		
		
		// Top 5 gamma
		List<OptionGreek> optionGreeks = new ArrayList<>();
		optionGreeks.addAll(ceOptionGreeks);
		optionGreeks.addAll(peOptionGreeks);
		
		Collections.sort(optionGreeks, new SortbyOiDesc());
		
		Set<Integer> top5Strikes = new HashSet<Integer>();
		float top5CeGamma = 0f;
		float top5PeGamma = 0f;
		int recProcessed = 0;
		for(OptionGreek aGreek: optionGreeks) {
			if (aGreek.getOi()*aGreek.getLtp()/10000000>10) {
				recProcessed++;
				if (aGreek.getTradingSymbol().endsWith("CE")) top5CeGamma = top5CeGamma + aGreek.getGamma();
				else top5PeGamma = top5PeGamma + aGreek.getGamma();
				top5Strikes.add(getStrike(aGreek.getTradingSymbol()));
			}
			if (recProcessed>=5) break;
		}
		retMap.put("top5CeGamma",top5CeGamma);
		retMap.put("top5PeGamma",top5PeGamma);
		retMap.put("futureOI", getFutureOI());
		
		recProcessed = 0;
		float topCEWorth = Float.MIN_VALUE;
		float topPEWorth = Float.MIN_VALUE;
		int highWorthCEStrike = 0;
		int highWorthPEStrike = 0;
		for(OptionGreek aGreek: optionGreeks) {
			float oiWorth = aGreek.getOi()*aGreek.getLtp()/10000000f;
			if (oiWorth>10) {
				
				recProcessed++;
				if (aGreek.getTradingSymbol().endsWith("CE")) {
					if (oiWorth > topCEWorth) {
						topCEWorth = oiWorth;
						highWorthCEStrike = aGreek.getStrike();
					}
				} else {
					if (oiWorth > topPEWorth) {
						topPEWorth = oiWorth;
						highWorthPEStrike = aGreek.getStrike();
					}
				}
			}
			if (recProcessed>=10) break;
		}
		retMap.put("highWorthCEStrike",(float) highWorthCEStrike);
		retMap.put("highWorthPEStrike",(float) highWorthPEStrike);
		
		// Top 5 strike Gamma Exposure
		float minGammaExposureTopN = Float.MAX_VALUE;
        float maxGammaExposureTopN = Float.MIN_VALUE;
        float netGammaExposureTopN = 0f;
        
		iter = sortedMap2.keySet().iterator();
		while (iter.hasNext()) {
			int strike = iter.next();
			if (top5Strikes.contains(strike)) {
				if (gammaPerStrikeDistance.get(strike) < minGammaExposureTopN) {
					minGammaExposureTopN = gammaPerStrikeDistance.get(strike);
				}
				if (gammaPerStrikeDistance.get(strike) > maxGammaExposureTopN) {
					maxGammaExposureTopN = gammaPerStrikeDistance.get(strike);
				}
				fileLogTelegramWriter.write(" For Strike " + strike + " strike distance gamma exposure " + gammaPerStrikeDistance.get(strike));
				netGammaExposureTopN = netGammaExposureTopN + gammaPerStrikeDistance.get(strike);
			}	
		}
		retMap.put("minGammaExposureTopN", Math.abs(minGammaExposureTopN)/1000f);
		retMap.put("maxGammaExposureTopN",maxGammaExposureTopN/1000f);
		retMap.put("netGammaExposureTopN",netGammaExposureTopN/1000f);
		
		
		
		float changein5secCeIV = 0;
		float changein5secPeIV = 0;
		
		
		// 4-9
		float dn1Delta = 0.4f;
		float up1Delta = 0.9f;
		
		float dr49Changein5secCeDelta = 0f;
		float dr49Changein5secPeDelta = 0f;
		
		float dr49Changein5secCeGamma = 0f;
		float dr49Changein5secPeGamma = 0f;
		
		float dr49Changein5secCeVega = 0f;
		float dr49Changein5secPeVega = 0f;
		
		float dr49Changein5secCeTheta = 0f;
		float dr49Changein5secPeTheta = 0f;
				
		float dr49Changein5secCeIV = 0f;
		float dr49Changein5secPeIV = 0f;
				
		float dr49Changein5secCeLtp = 0f;
		float dr49Changein5secPeLtp = 0f;
		
		
		float dr49WhlStrkChangein5secCeDelta = 0f;
		float dr49WhlStrkChangein5secPeDelta = 0f;
		
		float dr49WhlStrkChangein5secCeGamma = 0f;
		float dr49WhlStrkChangein5secPeGamma = 0f;
		
		float dr49WhlStrkChangein5secCeVega = 0f;
		float dr49WhlStrkChangein5secPeVega = 0f;
		
		float dr49WhlStrkChangein5secCeTheta = 0f;
		float dr49WhlStrkChangein5secPeTheta = 0f;
				
		float dr49WhlStrkChangein5secCeIV = 0f;
		float dr49WhlStrkChangein5secPeIV = 0f;
				
		float dr49WhlStrkChangein5secCeLtp = 0f;
		float dr49WhlStrkChangein5secPeLtp = 0f;
		
		
		float drWhlStrkChangein5secCeDelta = 0f;
		float drWhlStrkChangein5secPeDelta = 0f;
		
		float drWhlStrkChangein5secCeGamma = 0f;
		float drWhlStrkChangein5secPeGamma = 0f;
		
		float drWhlStrkChangein5secCeVega = 0f;
		float drWhlStrkChangein5secPeVega = 0f;
		
		float drWhlStrkChangein5secCeTheta = 0f;
		float drWhlStrkChangein5secPeTheta = 0f;
		
		float tmpChangein5secCeTheta = 0f;
		float tmpChangein5secPeTheta = 0f;
				
		float drWhlStrkChangein5secCeIV = 0f;
		float drWhlStrkChangein5secPeIV = 0f;
				
		float drWhlStrkChangein5secCeLtp = 0f;
		float drWhlStrkChangein5secPeLtp = 0f;
		
		int maxSpotChange = 500;
		
		float pt200Changein5secCeTheta = 0f;
		float pt200Changein5secPeTheta = 0f;
		
		float pt200TotalCallOi = 0f;
		float pt200TotalPutOi = 0f;
		
		List<Float> strk250CEIvs = new ArrayList<Float>();
		List<Float> strk250PEIvs = new ArrayList<Float>();
		
		List<Float> dr14CEIvs = new ArrayList<Float>();
		List<Float> dr14PEIvs = new ArrayList<Float>();
		
		int lowerPts = 400;
		int upperPts = 700;
		
		for(OptionGreek aGreek: ceOptionGreeks) {
			if (Math.abs(aGreek.getDelta()) >= dn1Delta && Math.abs(aGreek.getDelta()) <= up1Delta) {
				for(OptionGreek prevGreek: prevCeOptionGreeks) {
					if (aGreek.getTradingSymbol().equals(prevGreek.getTradingSymbol())) {
						dr49Changein5secCeDelta = dr49Changein5secCeDelta + (Math.abs(aGreek.getDelta()) - Math.abs(prevGreek.getDelta()));
						dr49Changein5secCeGamma = dr49Changein5secCeGamma + (aGreek.getGamma() - prevGreek.getGamma());
						dr49Changein5secCeVega = dr49Changein5secCeVega + (aGreek.getVega() - prevGreek.getVega());
						dr49Changein5secCeTheta = dr49Changein5secCeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevGreek.getTheta()));
						dr49Changein5secCeIV = dr49Changein5secCeIV + (aGreek.getIv() - prevGreek.getIv());
						dr49Changein5secCeLtp  = dr49Changein5secCeLtp + (aGreek.getLtp() - prevGreek.getLtp());
						break;
					}
				}
				if (aGreek.getStrike()%100==0) {
					for(OptionGreek prevGreek: prevCeOptionGreeks) {
						if (aGreek.getTradingSymbol().equals(prevGreek.getTradingSymbol())) {
							dr49WhlStrkChangein5secCeDelta = dr49WhlStrkChangein5secCeDelta + (Math.abs(aGreek.getDelta()) - Math.abs(prevGreek.getDelta()));
							dr49WhlStrkChangein5secCeGamma = dr49WhlStrkChangein5secCeGamma + (aGreek.getGamma() - prevGreek.getGamma());
							dr49WhlStrkChangein5secCeVega = dr49WhlStrkChangein5secCeVega + (aGreek.getVega() - prevGreek.getVega());
							dr49WhlStrkChangein5secCeTheta = dr49WhlStrkChangein5secCeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevGreek.getTheta()));
							dr49WhlStrkChangein5secCeIV = dr49WhlStrkChangein5secCeIV + (aGreek.getIv() - prevGreek.getIv());
							dr49WhlStrkChangein5secCeLtp  = dr49WhlStrkChangein5secCeLtp + (aGreek.getLtp() - prevGreek.getLtp());
							break;
						}
					}
				}
			}
			
			if (aGreek.getStrike() > underlyingInstrumentLtp + 500 && aGreek.getStrike() < underlyingInstrumentLtp + 750 ) {
				
				for(OptionGreek prevGreek: prevCeOptionGreeks) {
					if (aGreek.getTradingSymbol().equals(prevGreek.getTradingSymbol())) {
						drWhlStrkChangein5secCeDelta = drWhlStrkChangein5secCeDelta + (Math.abs(aGreek.getDelta()) - Math.abs(prevGreek.getDelta()));
						drWhlStrkChangein5secCeGamma = drWhlStrkChangein5secCeGamma + (aGreek.getGamma() - prevGreek.getGamma());
						drWhlStrkChangein5secCeVega = drWhlStrkChangein5secCeVega + (aGreek.getVega() - prevGreek.getVega());
						drWhlStrkChangein5secCeTheta = drWhlStrkChangein5secCeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevGreek.getTheta()));
						drWhlStrkChangein5secCeIV = drWhlStrkChangein5secCeIV + (aGreek.getIv() - prevGreek.getIv());
						drWhlStrkChangein5secCeLtp  = drWhlStrkChangein5secCeLtp + (aGreek.getLtp() - prevGreek.getLtp());
						break;
					}
				}
				strk250CEIvs.add(aGreek.getIv());
			}
			if (aGreek.getStrike() > underlyingInstrumentLtp + lowerPts && aGreek.getStrike() < underlyingInstrumentLtp + upperPts ) {
				for(OptionGreek prevGreek: prevCeOptionGreeks) {
					if (aGreek.getTradingSymbol().equals(prevGreek.getTradingSymbol())) {
						tmpChangein5secCeTheta = tmpChangein5secCeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevGreek.getTheta()));
						break;
					}
				}
			}
			
			if ( Math.abs(aGreek.getDelta()) <= 0.4f) {
				dr14CEIvs.add(aGreek.getIv());
			}
			
			if (aGreek.getStrike() < underlyingInstrumentLtp + 200 && aGreek.getStrike() > underlyingInstrumentLtp - 200 ) {						
				for(OptionGreek prevGreek: prevCeOptionGreeks) {
					if (aGreek.getTradingSymbol().equals(prevGreek.getTradingSymbol())) {
						pt200Changein5secCeTheta = pt200Changein5secCeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevGreek.getTheta()));
					}
				}
				pt200TotalCallOi = pt200TotalCallOi + aGreek.getOi();
			}
			
		}
		for(OptionGreek aGreek: peOptionGreeks) {
			if (Math.abs(aGreek.getDelta()) >= dn1Delta && Math.abs(aGreek.getDelta()) <= up1Delta) { 
				for(OptionGreek prevGreek: prevPeOptionGreeks) {
					if (aGreek.getTradingSymbol().equals(prevGreek.getTradingSymbol())) {
						dr49Changein5secPeDelta = dr49Changein5secPeDelta + (Math.abs(aGreek.getDelta()) - Math.abs(prevGreek.getDelta()));
						dr49Changein5secPeGamma = dr49Changein5secPeGamma + (aGreek.getGamma() - prevGreek.getGamma());
						dr49Changein5secPeVega = dr49Changein5secPeVega + (aGreek.getVega() - prevGreek.getVega());
						dr49Changein5secPeTheta = dr49Changein5secPeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevGreek.getTheta()));
						dr49Changein5secPeIV = dr49Changein5secPeIV + (aGreek.getIv() - prevGreek.getIv());
						dr49Changein5secPeLtp  = dr49Changein5secPeLtp + (aGreek.getLtp() - prevGreek.getLtp());
						break;
					}
				}
				if (aGreek.getStrike()%100==0) {
					for(OptionGreek prevGreek: prevPeOptionGreeks) {
						if (aGreek.getTradingSymbol().equals(prevGreek.getTradingSymbol())) {
							dr49WhlStrkChangein5secPeDelta = dr49WhlStrkChangein5secPeDelta + (Math.abs(aGreek.getDelta()) - Math.abs(prevGreek.getDelta()));
							dr49WhlStrkChangein5secPeGamma = dr49WhlStrkChangein5secPeGamma + (aGreek.getGamma() - prevGreek.getGamma());
							dr49WhlStrkChangein5secPeVega = dr49WhlStrkChangein5secPeVega + (aGreek.getVega() - prevGreek.getVega());
							dr49WhlStrkChangein5secPeTheta = dr49WhlStrkChangein5secPeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevGreek.getTheta()));
							dr49WhlStrkChangein5secPeIV = dr49WhlStrkChangein5secPeIV + (aGreek.getIv() - prevGreek.getIv());
							dr49WhlStrkChangein5secPeLtp  = dr49WhlStrkChangein5secPeLtp + (aGreek.getLtp() - prevGreek.getLtp());
							break;
						}
					}
				}
			}
			
			if (aGreek.getStrike() < underlyingInstrumentLtp -500 && aGreek.getStrike() > underlyingInstrumentLtp -750) {
				for(OptionGreek prevGreek: prevPeOptionGreeks) {
					if (aGreek.getTradingSymbol().equals(prevGreek.getTradingSymbol())) {
						drWhlStrkChangein5secPeDelta = drWhlStrkChangein5secPeDelta + (Math.abs(aGreek.getDelta()) - Math.abs(prevGreek.getDelta()));
						drWhlStrkChangein5secPeGamma = drWhlStrkChangein5secPeGamma + (aGreek.getGamma() - prevGreek.getGamma());
						drWhlStrkChangein5secPeVega = drWhlStrkChangein5secPeVega + (aGreek.getVega() - prevGreek.getVega());
						drWhlStrkChangein5secPeTheta = drWhlStrkChangein5secPeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevGreek.getTheta()));
						drWhlStrkChangein5secPeIV = drWhlStrkChangein5secPeIV + (aGreek.getIv() - prevGreek.getIv());
						drWhlStrkChangein5secPeLtp  = drWhlStrkChangein5secPeLtp + (aGreek.getLtp() - prevGreek.getLtp());
						break;
					}
				}
				strk250PEIvs.add(aGreek.getIv());
			}
			
			if (aGreek.getStrike() < underlyingInstrumentLtp -lowerPts && aGreek.getStrike() > underlyingInstrumentLtp -upperPts) {
				for(OptionGreek prevGreek: prevPeOptionGreeks) {
					if (aGreek.getTradingSymbol().equals(prevGreek.getTradingSymbol())) {
						tmpChangein5secPeTheta = tmpChangein5secPeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevGreek.getTheta()));
						break;
					}
				}
			}
			
			
			if (Math.abs(aGreek.getDelta()) <= 0.4f) {
				dr14PEIvs.add(aGreek.getIv());
			}
			if (aGreek.getStrike() < underlyingInstrumentLtp + 200 && aGreek.getStrike() > underlyingInstrumentLtp - 200 ) {						
				for(OptionGreek prevGreek: prevPeOptionGreeks) {
					if (aGreek.getTradingSymbol().equals(prevGreek.getTradingSymbol())) {
						pt200Changein5secPeTheta = pt200Changein5secPeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevGreek.getTheta()));
					}
				}
				pt200TotalPutOi = pt200TotalPutOi + aGreek.getOi();
			}
		}		
		retMap.put("dr49Changein5secCeGamma", dr49Changein5secCeGamma);
		retMap.put("dr49Changein5secPeGamma", dr49Changein5secPeGamma);		
		retMap.put("dr49Changein5secCeVega", dr49Changein5secCeVega);
		retMap.put("dr49Changein5secPeVega", dr49Changein5secPeVega);		
		retMap.put("dr49Changein5secCeTheta", dr49Changein5secCeTheta);
		retMap.put("dr49Changein5secPeTheta", dr49Changein5secPeTheta);		
		retMap.put("dr49Changein5secCeDelta", dr49Changein5secCeDelta);
		retMap.put("dr49Changein5secPeDelta", dr49Changein5secPeDelta);		
		retMap.put("dr49Changein5secCeIV", dr49Changein5secCeIV);
		retMap.put("dr49Changein5secPeIV", dr49Changein5secPeIV);		
		retMap.put("dr49Changein5secCeLtp", dr49Changein5secCeLtp);
		retMap.put("dr49Changein5secPeLtp", dr49Changein5secPeLtp);
		
		retMap.put("dr49WhlStrkChangein5secCeGamma", dr49WhlStrkChangein5secCeGamma);
		retMap.put("dr49WhlStrkChangein5secPeGamma", dr49WhlStrkChangein5secPeGamma);		
		retMap.put("dr49WhlStrkChangein5secCeVega", dr49WhlStrkChangein5secCeVega);
		retMap.put("dr49WhlStrkChangein5secPeVega", dr49WhlStrkChangein5secPeVega);		
		retMap.put("dr49WhlStrkChangein5secCeTheta", dr49WhlStrkChangein5secCeTheta);
		retMap.put("dr49WhlStrkChangein5secPeTheta", dr49WhlStrkChangein5secPeTheta);		
		retMap.put("dr49WhlStrkChangein5secCeDelta", dr49WhlStrkChangein5secCeDelta);
		retMap.put("dr49WhlStrkChangein5secPeDelta", dr49WhlStrkChangein5secPeDelta);		
		retMap.put("dr49WhlStrkChangein5secCeIV", dr49WhlStrkChangein5secCeIV);
		retMap.put("dr49WhlStrkChangein5secPeIV", dr49WhlStrkChangein5secPeIV);		
		retMap.put("dr49WhlStrkChangein5secCeLtp", dr49WhlStrkChangein5secCeLtp);
		retMap.put("dr49WhlStrkChangein5secPeLtp", dr49WhlStrkChangein5secPeLtp);
		
		retMap.put("drWhlStrkChangein5secCeGamma", drWhlStrkChangein5secCeGamma);
		retMap.put("drWhlStrkChangein5secPeGamma", drWhlStrkChangein5secPeGamma);		
		retMap.put("drWhlStrkChangein5secCeVega", drWhlStrkChangein5secCeVega);
		retMap.put("drWhlStrkChangein5secPeVega", drWhlStrkChangein5secPeVega);		
		retMap.put("drWhlStrkChangein5secCeTheta", drWhlStrkChangein5secCeTheta);
		retMap.put("drWhlStrkChangein5secPeTheta", drWhlStrkChangein5secPeTheta);		
		retMap.put("drWhlStrkChangein5secCeDelta", drWhlStrkChangein5secCeDelta);
		retMap.put("drWhlStrkChangein5secPeDelta", drWhlStrkChangein5secPeDelta);		
		retMap.put("drWhlStrkChangein5secCeIV", drWhlStrkChangein5secCeIV);
		retMap.put("drWhlStrkChangein5secPeIV", drWhlStrkChangein5secPeIV);		
		retMap.put("drWhlStrkChangein5secCeLtp", drWhlStrkChangein5secCeLtp);
		retMap.put("drWhlStrkChangein5secPeLtp", drWhlStrkChangein5secPeLtp);
		
		retMap.put("strk250CEAvgIv", (float) strk250CEIvs.stream().mapToDouble(d -> d).average().orElse(0.0));
		retMap.put("strk250PEAvgIv", (float) strk250PEIvs.stream().mapToDouble(d -> d).average().orElse(0.0));
		
		retMap.put("dr14CEAvgIv", (float) dr14CEIvs.stream().mapToDouble(d -> d).average().orElse(0.0));
		retMap.put("dr14PEAvgIv", (float) dr14PEIvs.stream().mapToDouble(d -> d).average().orElse(0.0));

		retMap.put("pt200Changein5secCeTheta", pt200Changein5secCeTheta);
		retMap.put("pt200Changein5secPeTheta", pt200Changein5secPeTheta);
		
		retMap.put("pt200TotalCallOi", pt200TotalCallOi);
		retMap.put("pt200TotalPutOi", pt200TotalPutOi);
		
		retMap.put("tmpChangein5secCeTheta", tmpChangein5secCeTheta);
		retMap.put("tmpChangein5secPeTheta", tmpChangein5secPeTheta);
		
		
		
		
		float otm250x750Changein5secCeTheta = 0f;
		float otm250x750Changein5secPeTheta = 0f;
		
		List<OptionGreek> itm1000x500CeIvs = new ArrayList<OptionGreek>();
		List<OptionGreek> itm1000x500PeIvs = new ArrayList<OptionGreek>();
		
		for(OptionGreek aGreek: ceOptionGreeks) {
			if (aGreek.getStrike()%100==0) {
				if (aGreek.getStrike() > underlyingInstrumentLtp + 250 && aGreek.getStrike() < underlyingInstrumentLtp + 750) {
					if (prevCeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null) otm250x750Changein5secCeTheta = otm250x750Changein5secCeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevCeOptionGreeksMap.get(aGreek.getTradingSymbol()).getTheta()));
					
				}
				if (aGreek.getStrike() > underlyingInstrumentLtp - 1000 && aGreek.getStrike() < underlyingInstrumentLtp - 500 ) {					
					if (prevCeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null) itm1000x500CeIvs.add(aGreek);
				}
			}
		}
		for(OptionGreek aGreek: peOptionGreeks) {
			if (aGreek.getStrike()%100==0) {
				if (aGreek.getStrike() < underlyingInstrumentLtp - 250 && aGreek.getStrike() > underlyingInstrumentLtp - 750 ) {
					if (prevPeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null) otm250x750Changein5secPeTheta = otm250x750Changein5secPeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevPeOptionGreeksMap.get(aGreek.getTradingSymbol()).getTheta()));
				}
				if (aGreek.getStrike() < underlyingInstrumentLtp + 1000 && aGreek.getStrike() > underlyingInstrumentLtp + 500 ) {
					if (prevPeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null) itm1000x500PeIvs.add(aGreek);
				}
			}
		}
		
		retMap.put("otm250x750Changein5secCeTheta", otm250x750Changein5secCeTheta);
		retMap.put("otm250x750Changein5secPeTheta", otm250x750Changein5secPeTheta);
		
		retMap.put("itm1000x500AvgCeIv", (float) itm1000x500CeIvs.stream().mapToDouble(d -> d.getIv()).average().orElse(0.0));
		retMap.put("itm1000x500AvgPeIv", (float) itm1000x500PeIvs.stream().mapToDouble(d -> d.getIv()).average().orElse(0.0));
		
		//otm1000_750avgCeiv+otm750_500avgCeiv
		
		int fromPts = -250;
		int toPts = 0;
		
		float otm250_0Changein5secCeTheta = 0f;
		float otm250_0Changein5secPeTheta = 0f;
		
		float otm0_250Changein5secCeTheta = 0f;
		float otm0_250Changein5secPeTheta = 0f;
		
		float otm250_500Changein5secCeTheta = 0f;
		float otm250_500Changein5secPeTheta = 0f;
		
		float otm500_750Changein5secCeTheta = 0f;
		float otm500_750Changein5secPeTheta = 0f;
		
		float otm750_1000Changein5secCeTheta = 0f;
		float otm750_1000Changein5secPeTheta = 0f;
		
		float otm1000_750Changein5secCeTheta = 0f;
		float otm1000_750Changein5secPeTheta = 0f;
		
		float otm750_500Changein5secCeTheta = 0f;
		float otm750_500Changein5secPeTheta = 0f;
		
		float otm500_250Changein5secCeTheta = 0f;
		float otm500_250Changein5secPeTheta = 0f;
		
		List<OptionGreek> otm250_0CeIvs = new ArrayList<OptionGreek>();
		List<OptionGreek> otm250_0PeIvs = new ArrayList<OptionGreek>();
		
		List<OptionGreek> otm0_250CeIvs = new ArrayList<OptionGreek>();
		List<OptionGreek> otm0_250PeIvs = new ArrayList<OptionGreek>();
		
		List<OptionGreek> otm250_500CeIvs = new ArrayList<OptionGreek>();
		List<OptionGreek> otm250_500PeIvs = new ArrayList<OptionGreek>();
		
		List<OptionGreek> otm500_750CeIvs = new ArrayList<OptionGreek>();
		List<OptionGreek> otm500_750PeIvs = new ArrayList<OptionGreek>();
		
		List<OptionGreek> otm750_1000CeIvs = new ArrayList<OptionGreek>();
		List<OptionGreek> otm750_1000PeIvs = new ArrayList<OptionGreek>();
		
		List<OptionGreek> otm1000_750CeIvs = new ArrayList<OptionGreek>();
		List<OptionGreek> otm1000_750PeIvs = new ArrayList<OptionGreek>();
		
		List<OptionGreek> otm750_500CeIvs = new ArrayList<OptionGreek>();
		List<OptionGreek> otm750_500PeIvs = new ArrayList<OptionGreek>();
		
		List<OptionGreek> otm500_250CeIvs = new ArrayList<OptionGreek>();
		List<OptionGreek> otm500_250PeIvs = new ArrayList<OptionGreek>();
		
		for(OptionGreek aGreek: ceOptionGreeks) {
			if (aGreek.getStrike()%100==0) {
				if (aGreek.getStrike() > underlyingInstrumentLtp + fromPts - 750 && aGreek.getStrike() < underlyingInstrumentLtp + toPts - 750 ) {
					if (prevCeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null) otm1000_750Changein5secCeTheta = otm1000_750Changein5secCeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevCeOptionGreeksMap.get(aGreek.getTradingSymbol()).getTheta()));
					if (prevCeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null) otm1000_750CeIvs.add(aGreek);
				}
				if (aGreek.getStrike() > underlyingInstrumentLtp + fromPts - 500 && aGreek.getStrike() < underlyingInstrumentLtp + toPts - 500 ) {
					if (prevCeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null) otm750_500Changein5secCeTheta = otm750_500Changein5secCeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevCeOptionGreeksMap.get(aGreek.getTradingSymbol()).getTheta()));
					if (prevCeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null) otm750_500CeIvs.add(aGreek);
				}
				if (aGreek.getStrike() > underlyingInstrumentLtp + fromPts - 250 && aGreek.getStrike() < underlyingInstrumentLtp + toPts - 250 ) {
					if (prevCeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null) otm500_250Changein5secCeTheta = otm500_250Changein5secCeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevCeOptionGreeksMap.get(aGreek.getTradingSymbol()).getTheta()));
					if (prevCeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null) otm500_250CeIvs.add(aGreek);
				}
				if (aGreek.getStrike() > underlyingInstrumentLtp + fromPts && aGreek.getStrike() < underlyingInstrumentLtp + toPts) {
					if (prevCeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null) otm250_0Changein5secCeTheta = otm250_0Changein5secCeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevCeOptionGreeksMap.get(aGreek.getTradingSymbol()).getTheta()));
					if (prevCeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null) otm250_0CeIvs.add(aGreek);
				}
				if (aGreek.getStrike() > underlyingInstrumentLtp + fromPts + 250 && aGreek.getStrike() < underlyingInstrumentLtp + toPts + 250) {
					if (prevCeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null) otm0_250Changein5secCeTheta = otm0_250Changein5secCeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevCeOptionGreeksMap.get(aGreek.getTradingSymbol()).getTheta()));
					if (prevCeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null) otm0_250CeIvs.add(aGreek);
				}
				if (aGreek.getStrike() > underlyingInstrumentLtp + fromPts + 500 && aGreek.getStrike() < underlyingInstrumentLtp + toPts + 500) {
					if (prevCeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null) otm250_500Changein5secCeTheta = otm250_500Changein5secCeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevCeOptionGreeksMap.get(aGreek.getTradingSymbol()).getTheta()));
					if (prevCeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null) otm250_500CeIvs.add(aGreek);
				}
				if (aGreek.getStrike() > underlyingInstrumentLtp + fromPts + 750 && aGreek.getStrike() < underlyingInstrumentLtp + toPts + 750) {
					if (prevCeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null) otm500_750Changein5secCeTheta = otm500_750Changein5secCeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevCeOptionGreeksMap.get(aGreek.getTradingSymbol()).getTheta()));
					if (prevCeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null) otm500_750CeIvs.add(aGreek);
				}
				if (aGreek.getStrike() > underlyingInstrumentLtp + fromPts + 1000 && aGreek.getStrike() < underlyingInstrumentLtp + toPts + 1000) {
					if (prevCeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null) otm750_1000Changein5secCeTheta = otm750_1000Changein5secCeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevCeOptionGreeksMap.get(aGreek.getTradingSymbol()).getTheta()));
					if (prevCeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null) otm750_1000CeIvs.add(aGreek);
				}
			}
		}
		for(OptionGreek aGreek: peOptionGreeks) {
			if (aGreek.getStrike()%100==0) {
				if (aGreek.getStrike() < underlyingInstrumentLtp - fromPts + 750 && aGreek.getStrike() > underlyingInstrumentLtp - toPts + 750 ) { // 1000-750
					if (prevPeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null) otm1000_750Changein5secPeTheta = otm1000_750Changein5secPeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevPeOptionGreeksMap.get(aGreek.getTradingSymbol()).getTheta()));
					if (prevPeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null) otm1000_750PeIvs.add(aGreek);
				}
				if (aGreek.getStrike() < underlyingInstrumentLtp - fromPts + 500 && aGreek.getStrike() > underlyingInstrumentLtp - toPts + 500 ) { // 750-500
					if (prevPeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null) otm750_500Changein5secPeTheta = otm750_500Changein5secPeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevPeOptionGreeksMap.get(aGreek.getTradingSymbol()).getTheta()));
					if (prevPeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null) otm750_500PeIvs.add(aGreek);
				}
				if (aGreek.getStrike() < underlyingInstrumentLtp - fromPts + 250 && aGreek.getStrike() > underlyingInstrumentLtp - toPts + 250 ) {
					if (prevPeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null) otm500_250Changein5secPeTheta = otm500_250Changein5secPeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevPeOptionGreeksMap.get(aGreek.getTradingSymbol()).getTheta()));
					if (prevPeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null) otm500_250PeIvs.add(aGreek);
				}
				if (aGreek.getStrike() < underlyingInstrumentLtp - fromPts && aGreek.getStrike() > underlyingInstrumentLtp - toPts) {
					if (prevPeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null) otm250_0Changein5secPeTheta = otm250_0Changein5secPeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevPeOptionGreeksMap.get(aGreek.getTradingSymbol()).getTheta()));
					if (prevPeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null) otm250_0PeIvs.add(aGreek);
				}
				if (aGreek.getStrike() < underlyingInstrumentLtp - fromPts - 250 && aGreek.getStrike() > underlyingInstrumentLtp - toPts - 250 ) {
					if (prevPeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null) otm0_250Changein5secPeTheta = otm0_250Changein5secPeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevPeOptionGreeksMap.get(aGreek.getTradingSymbol()).getTheta()));
					if (prevPeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null) otm0_250PeIvs.add(aGreek);
				}
				if (aGreek.getStrike() < underlyingInstrumentLtp - fromPts - 500 && aGreek.getStrike() > underlyingInstrumentLtp - toPts - 500 ) {
					if (prevPeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null) otm250_500Changein5secPeTheta = otm250_500Changein5secPeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevPeOptionGreeksMap.get(aGreek.getTradingSymbol()).getTheta()));
					if (prevPeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null) otm250_500PeIvs.add(aGreek);
				}
				if (aGreek.getStrike() < underlyingInstrumentLtp - fromPts - 750 && aGreek.getStrike() > underlyingInstrumentLtp - toPts - 750 ) {
					if (prevPeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null) otm500_750Changein5secPeTheta = otm500_750Changein5secPeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevPeOptionGreeksMap.get(aGreek.getTradingSymbol()).getTheta()));
					if (prevPeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null) otm500_750PeIvs.add(aGreek);
				}
				if (aGreek.getStrike() < underlyingInstrumentLtp - fromPts - 1000 && aGreek.getStrike() > underlyingInstrumentLtp - toPts - 1000 ) {
					if (prevPeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null) otm750_1000Changein5secPeTheta = otm750_1000Changein5secPeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevPeOptionGreeksMap.get(aGreek.getTradingSymbol()).getTheta()));
					if (prevPeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null) otm750_1000PeIvs.add(aGreek);
				}
			}
		}
		retMap.put("otm1000_750Changein5secCeTheta", otm1000_750Changein5secCeTheta);
		retMap.put("otm1000_750Changein5secPeTheta", otm1000_750Changein5secPeTheta);
		
		retMap.put("otm750_500Changein5secCeTheta", otm750_500Changein5secCeTheta);
		retMap.put("otm750_500Changein5secPeTheta", otm750_500Changein5secPeTheta);
		
		retMap.put("otm500_250Changein5secCeTheta", otm500_250Changein5secCeTheta);
		retMap.put("otm500_250Changein5secPeTheta", otm500_250Changein5secPeTheta);
		
		retMap.put("otm250_0Changein5secCeTheta", otm250_0Changein5secCeTheta);
		retMap.put("otm250_0Changein5secPeTheta", otm250_0Changein5secPeTheta);
		
		retMap.put("otm0_250Changein5secCeTheta", otm0_250Changein5secCeTheta);
		retMap.put("otm0_250Changein5secPeTheta", otm0_250Changein5secPeTheta);
		
		retMap.put("otm250_500Changein5secCeTheta", otm250_500Changein5secCeTheta);
		retMap.put("otm250_500Changein5secPeTheta", otm250_500Changein5secPeTheta);
		
		retMap.put("otm500_750Changein5secCeTheta", otm500_750Changein5secCeTheta);
		retMap.put("otm500_750Changein5secPeTheta", otm500_750Changein5secPeTheta);
		
		retMap.put("otm750_1000Changein5secCeTheta", otm750_1000Changein5secCeTheta);
		retMap.put("otm750_1000Changein5secPeTheta", otm750_1000Changein5secPeTheta);
		 
		 
		retMap.put("otm1000_750AvgCeIv", (float) otm1000_750CeIvs.stream().mapToDouble(d -> d.getIv()).average().orElse(0.0));
		retMap.put("otm1000_750AvgPeIv", (float) otm1000_750PeIvs.stream().mapToDouble(d -> d.getIv()).average().orElse(0.0));
		
		retMap.put("otm750_500AvgCeIv", (float) otm750_500CeIvs.stream().mapToDouble(d -> d.getIv()).average().orElse(0.0));
		retMap.put("otm750_500AvgPeIv", (float) otm750_500PeIvs.stream().mapToDouble(d -> d.getIv()).average().orElse(0.0));
		
		retMap.put("otm500_250AvgCeIv", (float) otm500_250CeIvs.stream().mapToDouble(d -> d.getIv()).average().orElse(0.0));
		retMap.put("otm500_250AvgPeIv", (float) otm500_250PeIvs.stream().mapToDouble(d -> d.getIv()).average().orElse(0.0));
		
		retMap.put("otm250_0AvgCeIv", (float) otm250_0CeIvs.stream().mapToDouble(d -> d.getIv()).average().orElse(0.0));
		retMap.put("otm250_0AvgPeIv", (float) otm250_0PeIvs.stream().mapToDouble(d -> d.getIv()).average().orElse(0.0));
		
		retMap.put("otm0_250AvgCeIv", (float) otm0_250CeIvs.stream().mapToDouble(d -> d.getIv()).average().orElse(0.0));
		retMap.put("otm0_250AvgPeIv", (float) otm0_250PeIvs.stream().mapToDouble(d -> d.getIv()).average().orElse(0.0));
		
		retMap.put("otm250_500AvgCeIv", (float) otm250_500CeIvs.stream().mapToDouble(d -> d.getIv()).average().orElse(0.0));
		retMap.put("otm250_500AvgPeIv", (float) otm250_500PeIvs.stream().mapToDouble(d -> d.getIv()).average().orElse(0.0));
		
		retMap.put("otm500_750AvgCeIv", (float) otm500_750CeIvs.stream().mapToDouble(d -> d.getIv()).average().orElse(0.0));
		retMap.put("otm500_750AvgPeIv", (float) otm500_750PeIvs.stream().mapToDouble(d -> d.getIv()).average().orElse(0.0));
		
		retMap.put("otm750_1000AvgCeIv", (float) otm750_1000CeIvs.stream().mapToDouble(d -> d.getIv()).average().orElse(0.0));
		retMap.put("otm750_1000AvgPeIv", (float) otm750_1000PeIvs.stream().mapToDouble(d -> d.getIv()).average().orElse(0.0));

		// ATM, ITM, OTM Accml Theta
		lowerPts = 300;
		upperPts = 1000;
		float whlStrkOTMChangein5secCeTheta = 0f;
		float whlStrkATMChangein5secCeTheta = 0f;
		float whlStrkITMChangein5secCeTheta = 0f;
		for(OptionGreek aGreek: ceOptionGreeks) {
			//if (aGreek.getStrike()%100==0) {
				// CE OTM
				if (aGreek.getStrike() > underlyingInstrumentLtp + lowerPts && aGreek.getStrike() < underlyingInstrumentLtp + upperPts) {
					for(OptionGreek prevGreek: prevCeOptionGreeks) {
						if (aGreek.getTradingSymbol().equals(prevGreek.getTradingSymbol())) {
							whlStrkOTMChangein5secCeTheta = whlStrkOTMChangein5secCeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevGreek.getTheta()));
							break;
						}
					}
				}
				// CE ATM
				if (aGreek.getStrike() < underlyingInstrumentLtp + lowerPts && aGreek.getStrike() > underlyingInstrumentLtp - lowerPts) {
					for(OptionGreek prevGreek: prevCeOptionGreeks) {
						if (aGreek.getTradingSymbol().equals(prevGreek.getTradingSymbol())) {
							whlStrkATMChangein5secCeTheta = whlStrkATMChangein5secCeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevGreek.getTheta()));
							break;
						}
					}
				}
				// CE ITM
				if (aGreek.getStrike() < underlyingInstrumentLtp - lowerPts && aGreek.getStrike() > underlyingInstrumentLtp - upperPts) {
					for(OptionGreek prevGreek: prevCeOptionGreeks) {
						if (aGreek.getTradingSymbol().equals(prevGreek.getTradingSymbol())) {
							whlStrkITMChangein5secCeTheta = whlStrkITMChangein5secCeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevGreek.getTheta()));
							break;
						}
					}
				}
			//}
		}
		
		float whlStrkOTMChangein5secPeTheta = 0f;
		float whlStrkATMChangein5secPeTheta = 0f;
		float whlStrkITMChangein5secPeTheta = 0f;
		for(OptionGreek aGreek: peOptionGreeks) {
			if (aGreek.getStrike()%100==0) {
				// CE OTM
				if (aGreek.getStrike() < underlyingInstrumentLtp - lowerPts && aGreek.getStrike() > underlyingInstrumentLtp - upperPts) {
					for(OptionGreek prevGreek: prevPeOptionGreeks) {
						if (aGreek.getTradingSymbol().equals(prevGreek.getTradingSymbol())) {
							whlStrkOTMChangein5secPeTheta = whlStrkOTMChangein5secPeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevGreek.getTheta()));
							break;
						}
					}
				}
				// CE ATM
				if (aGreek.getStrike() < underlyingInstrumentLtp + lowerPts && aGreek.getStrike() > underlyingInstrumentLtp - lowerPts) {
					for(OptionGreek prevGreek: prevPeOptionGreeks) {
						if (aGreek.getTradingSymbol().equals(prevGreek.getTradingSymbol())) {
							whlStrkATMChangein5secPeTheta = whlStrkATMChangein5secPeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevGreek.getTheta()));
							break;
						}
					}
				}
				// CE ITM
				if (aGreek.getStrike() > underlyingInstrumentLtp + lowerPts && aGreek.getStrike() < underlyingInstrumentLtp + upperPts) {
					for(OptionGreek prevGreek: prevPeOptionGreeks) {
						if (aGreek.getTradingSymbol().equals(prevGreek.getTradingSymbol())) {
							whlStrkITMChangein5secPeTheta = whlStrkITMChangein5secPeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevGreek.getTheta()));
							break;
						}
					}
				}
			}
		}
		retMap.put("whlStrkOTMChangein5secCeTheta", whlStrkOTMChangein5secCeTheta);
		retMap.put("whlStrkOTMChangein5secPeTheta", whlStrkOTMChangein5secPeTheta);
		
		retMap.put("whlStrkATMChangein5secCeTheta", whlStrkATMChangein5secCeTheta);
		retMap.put("whlStrkATMChangein5secPeTheta", whlStrkATMChangein5secPeTheta);
		
		retMap.put("whlStrkITMChangein5secCeTheta", whlStrkITMChangein5secCeTheta);
		retMap.put("whlStrkITMChangein5secPeTheta", whlStrkITMChangein5secPeTheta);
		
		// 1-6
		float dn2Delta = 0.1f;
		float up2Delta = 0.6f;
		
		float dr16Changein5secCeDelta = 0f;
		float dr16Changein5secPeDelta = 0f;
		
		float dr16Changein5secCeGamma = 0f;
		float dr16Changein5secPeGamma = 0f;
		
		float dr16Changein5secCeVega = 0f;
		float dr16Changein5secPeVega = 0f;
		
		float dr16Changein5secCeTheta = 0f;
		float dr16Changein5secPeTheta = 0f;
				
		float dr16Changein5secCeIV = 0f;
		float dr16Changein5secPeIV = 0f;
				
		float dr16Changein5secCeLtp = 0f;
		float dr16Changein5secPeLtp = 0f;
		
		for(OptionGreek aGreek: ceOptionGreeks) {
			if (Math.abs(aGreek.getDelta()) >= dn2Delta && Math.abs(aGreek.getDelta()) <= up2Delta) {
				for(OptionGreek prevGreek: prevCeOptionGreeks) {
					if (aGreek.getTradingSymbol().equals(prevGreek.getTradingSymbol())) {
						dr16Changein5secCeDelta = dr16Changein5secCeDelta + (Math.abs(aGreek.getDelta()) - Math.abs(prevGreek.getDelta()));
						dr16Changein5secCeGamma = dr16Changein5secCeGamma + (aGreek.getGamma() - prevGreek.getGamma());
						dr16Changein5secCeVega = dr16Changein5secCeVega + (aGreek.getVega() - prevGreek.getVega());
						dr16Changein5secCeTheta = dr16Changein5secCeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevGreek.getTheta()));
						dr16Changein5secCeIV = dr16Changein5secCeIV + (aGreek.getIv() - prevGreek.getIv());
						dr16Changein5secCeLtp  = dr16Changein5secCeLtp + (aGreek.getLtp() - prevGreek.getLtp());
						break;
					}
				}
			}
		}
		for(OptionGreek aGreek: peOptionGreeks) {
			if (Math.abs(aGreek.getDelta()) >= dn2Delta && Math.abs(aGreek.getDelta()) <= up2Delta) { 
				for(OptionGreek prevGreek: prevPeOptionGreeks) {
					if (aGreek.getTradingSymbol().equals(prevGreek.getTradingSymbol())) {
						dr16Changein5secPeDelta = dr16Changein5secPeDelta + (Math.abs(aGreek.getDelta()) - Math.abs(prevGreek.getDelta()));
						dr16Changein5secPeGamma = dr16Changein5secPeGamma + (aGreek.getGamma() - prevGreek.getGamma());
						dr16Changein5secPeVega = dr16Changein5secPeVega + (aGreek.getVega() - prevGreek.getVega());
						dr16Changein5secPeTheta = dr16Changein5secPeTheta + (Math.abs(aGreek.getTheta()) - Math.abs(prevGreek.getTheta()));
						dr16Changein5secPeIV = dr16Changein5secPeIV + (aGreek.getIv() - prevGreek.getIv());
						dr16Changein5secPeLtp  = dr16Changein5secPeLtp + (aGreek.getLtp() - prevGreek.getLtp());
						break;
					}
				}
			}
		}		
		retMap.put("dr16Changein5secCeGamma", dr16Changein5secCeGamma);
		retMap.put("dr16Changein5secPeGamma", dr16Changein5secPeGamma);		
		retMap.put("dr16Changein5secCeVega", dr16Changein5secCeVega);
		retMap.put("dr16Changein5secPeVega", dr16Changein5secPeVega);		
		retMap.put("dr16Changein5secCeTheta", dr16Changein5secCeTheta);
		retMap.put("dr16Changein5secPeTheta", dr16Changein5secPeTheta);		
		retMap.put("dr16Changein5secCeDelta", dr16Changein5secCeDelta);
		retMap.put("dr16Changein5secPeDelta", dr16Changein5secPeDelta);		
		retMap.put("dr16Changein5secCeIV", dr16Changein5secCeIV);
		retMap.put("dr16Changein5secPeIV", dr16Changein5secPeIV);		
		retMap.put("dr16Changein5secCeLtp", dr16Changein5secCeLtp);
		retMap.put("dr16Changein5secPeLtp", dr16Changein5secPeLtp);
		
		// Top5 from each type 5 sec iv change
		recCount = 0;
		float top5EachTypeChangein5secCeIV = 0;
		float top5EachTypeChangein5secPeIV = 0;
		for(OptionGreek aGreek: optionGreeks) {
			if (aGreek.getOi()*aGreek.getLtp()/10000000>10 && recCount < 5) {
				recCount++;
				if (aGreek.getTradingSymbol().endsWith("CE")) {
					for(OptionGreek prevGreek: prevCeOptionGreeks) {
						if (aGreek.getTradingSymbol().equals(prevGreek.getTradingSymbol())) {
							top5EachTypeChangein5secCeIV = top5EachTypeChangein5secCeIV + (aGreek.getIv() - prevGreek.getIv());
						}
					}
				} else if (aGreek.getTradingSymbol().endsWith("PE")) {
					for(OptionGreek prevGreek: prevPeOptionGreeks) {
						if (aGreek.getTradingSymbol().equals(prevGreek.getTradingSymbol())) {
							top5EachTypeChangein5secPeIV = top5EachTypeChangein5secPeIV + (aGreek.getIv() - prevGreek.getIv());
						}
					}
				}
			}
		}
		retMap.put("top5EachTypeChangein5secCeIV", top5EachTypeChangein5secCeIV);
		retMap.put("top5EachTypeChangein5secPeIV", top5EachTypeChangein5secPeIV);
		
		
		Collections.sort(ceOptionGreeks, new SortbyIV());
		Collections.sort(peOptionGreeks, new SortbyIV());
		
		lastIvRead = 0f;
		List<OptionGreek> dr49ExOutlierCEIvList = new ArrayList<OptionGreek>();		
		for(OptionGreek aGreek: ceOptionGreeks) {
			float delta = Math.abs(aGreek.getDelta());
			
			if (delta >= 0.4f && delta <= 0.9f) {
				float curIv = aGreek.getIv();
				if (lastIvRead<0.1f || curIv < lastIvRead + 5f) {
					dr49ExOutlierCEIvList.add(aGreek);
					lastIvRead = curIv; 
					recCount++;
				}
			}
		}		
		lastIvRead = 0f;
		List<OptionGreek> dr49ExOutlierPEIvList = new ArrayList<OptionGreek>();		
		for(OptionGreek aGreek: peOptionGreeks) {
			float delta = Math.abs(aGreek.getDelta());
			
			if (delta >= 0.4f && delta <= 0.9f) {
				float curIv = aGreek.getIv();
				if (lastIvRead<0.1f || curIv < lastIvRead + 5f) {
					dr49ExOutlierPEIvList.add(aGreek);
					lastIvRead = curIv; 
					recCount++;
				}
			}
		}
		retMap.put("dr49ExOutlierCEAvgIv", (float) dr49ExOutlierCEIvList.stream().mapToDouble(d -> d.getIv()).average().orElse(0.0));
		retMap.put("dr49ExOutlierPEAvgIv", (float) dr49ExOutlierPEIvList.stream().mapToDouble(d -> d.getIv()).average().orElse(0.0));
		
		// Cumulative IV diff between sequqntial strikes 
		Collections.sort(ceOptionGreeks, new SortbyStrike());
		Collections.sort(peOptionGreeks, new SortbyStrike());
		Collections.reverse(peOptionGreeks);
		
		lowerDelta = 0.1f;
		upperDelta = 0.5f;
		
		float prevIv = 0f;
		float cumulativeCEAvgIVDiff = 0f;
		recCount = 0;
		for(OptionGreek aGreek: ceOptionGreeks) {
			float delta = Math.abs(aGreek.getDelta());
			if (delta >= lowerDelta && delta <= upperDelta) {
				if (prevIv > 0.01f) {
					
					float ivDiff = aGreek.getIv()-prevIv;
					if (ivDiff > -1.5f && ivDiff < 1.5f) {
						recCount++;
						//cumulativeCEAvgIVDiff = cumulativeCEAvgIVDiff + ivDiff;
						cumulativeCEAvgIVDiff = cumulativeCEAvgIVDiff + aGreek.getIv();
						fileLogTelegramWriter.write("CE Strike "+ aGreek.getStrike()+" Iv Diff " + ivDiff + " delta="+delta + " Iv="+aGreek.getIv());
					}
				}
				prevIv = aGreek.getIv();
			}
		}
		retMap.put("cumulativeCEAvgIVDiff", cumulativeCEAvgIVDiff/(float)recCount);
		fileLogTelegramWriter.write("cumulativeCEAvgIVDiff="+cumulativeCEAvgIVDiff+" recCount="+recCount);
		prevIv = 0f;
		recCount = 0;
		float cumulativePEAvgIVDiff = 0f;
		for(OptionGreek aGreek: peOptionGreeks) {
			float delta = Math.abs(aGreek.getDelta());
			if (delta >= lowerDelta && delta <= upperDelta) {
				if (prevIv > 0.01f) {
					float ivDiff = aGreek.getIv()-prevIv;
					if (ivDiff > -1.5f && ivDiff < 1.5f) {
						recCount++;
						cumulativePEAvgIVDiff = cumulativePEAvgIVDiff + aGreek.getIv();
						fileLogTelegramWriter.write("PE Strike "+ aGreek.getStrike()+" Iv Diff " + ivDiff + " delta="+delta+ " Iv="+aGreek.getIv());
					}
				}
				prevIv = aGreek.getIv();
			}
		}
		retMap.put("cumulativePEAvgIVDiff", cumulativePEAvgIVDiff/(float)recCount);
		fileLogTelegramWriter.write("cumulativePEAvgIVDiff="+cumulativePEAvgIVDiff+" recCount="+recCount);
		
		
		Collections.sort(ceOptionGreeks, new SortbyAbsDelta());
		Collections.sort(peOptionGreeks, new SortbyAbsDelta());
		//System.out.println("Delta Sort done");
		
		List<Float> scaledCEIvs = new ArrayList<Float>();
		List<Float> scaledPEIvs = new ArrayList<Float>();
		float curDelta = 0.6f;
		do {
			float minDelta = 1f;
			OptionGreek lowerCEOptionGreek = null;
			for(OptionGreek aGreek: ceOptionGreeks) {
				if (getStrike(aGreek.getTradingSymbol())%100 == 0) {
					if (curDelta - Math.abs(aGreek.getDelta()) >= 0 ) {
						float deltaDiff = curDelta - Math.abs(aGreek.getDelta());
						if (deltaDiff < minDelta) {
							minDelta = deltaDiff;
							lowerCEOptionGreek = aGreek;
						}
					}
				}
				
			}
			minDelta = 1f;
			OptionGreek upperCEOptionGreek = null;
			for(OptionGreek aGreek: ceOptionGreeks) {
				if (getStrike(aGreek.getTradingSymbol())%100 == 0) {
					if (Math.abs(aGreek.getDelta())-curDelta >= 0 ) {
						float deltaDiff = Math.abs(aGreek.getDelta()) - curDelta;
						if (deltaDiff < minDelta) {
							minDelta = deltaDiff;
							upperCEOptionGreek = aGreek;
						}
					}
				}
			}
			
			minDelta = 1f;
			OptionGreek lowerPEOptionGreek = null;
			for(OptionGreek aGreek: peOptionGreeks) {
				if (getStrike(aGreek.getTradingSymbol())%100 == 0) {
					if (curDelta - Math.abs(aGreek.getDelta()) >= 0 ) {
						float deltaDiff = curDelta - Math.abs(aGreek.getDelta());
						if (deltaDiff < minDelta) {
							minDelta = deltaDiff;
							lowerPEOptionGreek = aGreek;
						}
					}
				}
			}
			minDelta = 1f;
			OptionGreek upperPEOptionGreek = null;
			for(OptionGreek aGreek: peOptionGreeks) {
				if (getStrike(aGreek.getTradingSymbol())%100 == 0) {
					if (Math.abs(aGreek.getDelta())-curDelta >= 0 ) {
						float deltaDiff = Math.abs(aGreek.getDelta()) - curDelta;
						if (deltaDiff < minDelta) {
							minDelta = deltaDiff;
							upperPEOptionGreek = aGreek;
						}
					}	
				}
			}
			
			float scaledCEIV = getScaledValue(Math.abs(lowerCEOptionGreek.getDelta()), Math.abs(upperCEOptionGreek.getDelta()), Math.abs(lowerCEOptionGreek.getDelta())/lowerCEOptionGreek.getGamma(),  Math.abs(upperCEOptionGreek.getDelta())/upperCEOptionGreek.getGamma(),  curDelta);
			float scaledPEIV = getScaledValue(Math.abs(lowerPEOptionGreek.getDelta()), Math.abs(upperPEOptionGreek.getDelta()), Math.abs(lowerPEOptionGreek.getDelta())/lowerPEOptionGreek.getGamma(),  Math.abs(upperPEOptionGreek.getDelta())/upperPEOptionGreek.getGamma(),  curDelta);
			
			fileLogTelegramWriter.write("Looking4Delta="+curDelta+ " lower CE Delta="+lowerCEOptionGreek.getDelta()+" upper CE Delta="+upperCEOptionGreek.getDelta()
				+ " lower PE Delta="+lowerPEOptionGreek.getDelta()+" upper PE Delta="+upperPEOptionGreek.getDelta() + " scaledCEIV="+scaledCEIV+" scaledPEIV="+scaledPEIV);
			
			scaledCEIvs.add(scaledCEIV);
			scaledPEIvs.add(scaledPEIV);
			
			curDelta = curDelta -0.1f;
		} while (curDelta>0.1f);
		
		retMap.put("scaledCEAvgIV",(float) scaledCEIvs.stream().mapToDouble(d -> d).average().orElse(0.0));
		retMap.put("scaledPEAvgIV",(float) scaledPEIvs.stream().mapToDouble(d -> d).average().orElse(0.0));
		
		// Top2 OiWorth 
		Collections.sort(ceOptionGreeks, new SortbyWorthDesc());
		Collections.sort(peOptionGreeks, new SortbyWorthDesc());
		int ceOiWorthCenter = (ceOptionGreeks.get(0).getStrike());// + ceOptionGreeks.get(1).getStrike())/2;
		int peOiWorthCenter = (peOptionGreeks.get(0).getStrike());// + peOptionGreeks.get(1).getStrike())/2;
		
		float ceOiWorthCenterWorth = ceOptionGreeks.get(0).getLtp()*ceOptionGreeks.get(0).getOi(); // + ceOptionGreeks.get(1).getLtp()*ceOptionGreeks.get(1).getOi();
		float peOiWorthCenterWorth = peOptionGreeks.get(0).getLtp()*peOptionGreeks.get(0).getOi(); // + peOptionGreeks.get(1).getLtp()*peOptionGreeks.get(1).getOi();
		
		retMap.put("ceOiWorthCenter",(float) ceOiWorthCenter);
		retMap.put("peOiWorthCenter",(float) peOiWorthCenter);
		
		retMap.put("ceOiWorthCenterWorth",ceOiWorthCenterWorth);
		retMap.put("peOiWorthCenterWorth",peOiWorthCenterWorth);
		
		// Gamma spike 
		OptionGreek[] atmGreeks = getATMGreeks(ceOptionGreeks, peOptionGreeks, 0.5f);
		
		for(OptionGreek aGreek: prevCeOptionGreeks) {
			if (aGreek.getTradingSymbol().equals(atmGreeks[0].getTradingSymbol())) {
				float changeInUnderlying = aGreek.getUnderlyingValue() -  atmGreeks[0].getUnderlyingValue();
				float calculatedNewGamma = aGreek.getGamma() + aGreek.getGamma()*changeInUnderlying;
				float gammaDifference =  (atmGreeks[0].getGamma() - calculatedNewGamma)*100f;
				retMap.put("ceGammaDifference",changeInUnderlying<0&&Math.abs(gammaDifference) > 1?gammaDifference*100f:0f);
				break;
			}
		}
		for(OptionGreek aGreek: prevPeOptionGreeks) {
			if (aGreek.getTradingSymbol().equals(atmGreeks[1].getTradingSymbol())) {
				float changeInUnderlying = aGreek.getUnderlyingValue() -  atmGreeks[1].getUnderlyingValue();
				float calculatedNewGamma = aGreek.getGamma() + aGreek.getGamma()*changeInUnderlying;
				float gammaDifference =  (atmGreeks[1].getGamma() - calculatedNewGamma)*100f;
				retMap.put("peGammaDifference",changeInUnderlying>0&&Math.abs(gammaDifference) > 1?gammaDifference*100f:0f);
				break;
			}
		}
		// CE		
		float perfectATMCEStrike = this.underlyingInstrumentLtp + ( (0.5f - Math.abs(atmGreeks[0].getDelta()) )/atmGreeks[0].getGamma() );
		float perfectATMPEStrike = this.underlyingInstrumentLtp - ( (0.5f - Math.abs(atmGreeks[1].getDelta()) )/atmGreeks[1].getGamma() );
		retMap.put("perfectATMCEStrike",perfectATMCEStrike);
		retMap.put("perfectATMPEStrike",perfectATMPEStrike);
		
		
		// EachTypeTop5
		Collections.sort(ceOptionGreeks, new SortbyOiDesc());
		Collections.sort(peOptionGreeks, new SortbyOiDesc());
		
		top5EachTypeChangein5secCeIV = 0;
		top5EachTypeChangein5secPeIV = 0;
		
		float top5EachTypeChangein5secCeGamma = 0;
		float top5EachTypeChangein5secPeGamma = 0;
		
		float top5EachTypeChangein5secCeTheta = 0;
		float top5EachTypeChangein5secPeTheta = 0;
		
		int ceCount = 0;
		for(OptionGreek aGreek: ceOptionGreeks) {
			if (aGreek.getOi()*aGreek.getLtp()/10000000>10) {
				for(OptionGreek prevGreek: prevCeOptionGreeks) {
					if (aGreek.getTradingSymbol().equals(prevGreek.getTradingSymbol())) {
						top5EachTypeChangein5secCeIV = top5EachTypeChangein5secCeIV + (aGreek.getIv() - prevGreek.getIv());
						top5EachTypeChangein5secCeGamma = top5EachTypeChangein5secCeGamma + (aGreek.getGamma() - prevGreek.getGamma());
						top5EachTypeChangein5secCeTheta = top5EachTypeChangein5secCeTheta + (aGreek.getTheta() - prevGreek.getTheta());
						break;
					}
				}
				ceCount++;
			}
			if (ceCount>=5) break;
		}
		
		int peCount = 0;
		for(OptionGreek aGreek: peOptionGreeks) {
			if (aGreek.getOi()*aGreek.getLtp()/10000000>10) {
				for(OptionGreek prevGreek: prevPeOptionGreeks) {
					if (aGreek.getTradingSymbol().equals(prevGreek.getTradingSymbol())) {
						top5EachTypeChangein5secPeIV = top5EachTypeChangein5secPeIV + (aGreek.getIv() - prevGreek.getIv());
						top5EachTypeChangein5secPeGamma = top5EachTypeChangein5secPeGamma + (aGreek.getGamma() - prevGreek.getGamma());
						top5EachTypeChangein5secPeTheta = top5EachTypeChangein5secPeTheta + (aGreek.getTheta() - prevGreek.getTheta());
						break;
					}
				}
				peCount++;
			}
			if (peCount>=5) break;
		}
		
		retMap.put("top5EachTypeChangein5secCeIV", top5EachTypeChangein5secCeIV);
		retMap.put("top5EachTypeChangein5secPeIV", top5EachTypeChangein5secPeIV);
		
		retMap.put("top5EachTypeChangein5secCeTheta", top5EachTypeChangein5secCeTheta);
		retMap.put("top5EachTypeChangein5secPeTheta", top5EachTypeChangein5secPeTheta);
		
		retMap.put("top5EachTypeChangein5secCeGamma", top5EachTypeChangein5secCeGamma);
		retMap.put("top5EachTypeChangein5secPeGamma", top5EachTypeChangein5secPeGamma);
		
		//saveGreek(retMap, adjustedATMCEGreek, adjustedATMPEGreek);
		saveAccumulatedChangeInIVGreek(retMap, adjustedATMCEGreek, adjustedATMPEGreek);
		
		fileLogTelegramWriter.write("ceGreekDetails="+ceGreekDetails);
		fileLogTelegramWriter.write("peGreekDetails="+peGreekDetails);
		
		fileLogTelegramWriter.write("dr19WholeStrikeCEAvgIV="+retMap.get("dr19WholeStrikeCEAvgIV")+" dr19WholeStrikePEAvgIV="+retMap.get("dr19WholeStrikePEAvgIV"));
		
		fileLogTelegramWriter.close();
		
		// delete the file
		cal = Calendar.getInstance();
		cal.setTime(recTimestamp);
		SimpleDateFormat fileDateFormat = new SimpleDateFormat("dd-MM-yyyy-HH-mm-ss");
		String filename = ApplicationConfig.getProperty("logFileLocation") + this.atmId + "-" + fileDateFormat.format(cal.getTime())+".log";
        File file = new File(filename);
        //System.out.println("Delteing file "+filename);
        if (file.delete()) {
            //System.out.println("File deleted successfully");
        } else {
        	System.out.println("Failed File deleted successfully");
        }
	}
	
	private Map<String, Float> getPrev5SecFullRangeIvs() {
		Map<String, Float> retMap = new HashMap<String, Float>();
		
		Connection conn = null;
		try {			
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			String fetchSql = "select fullRangeCETotalIV, fullRangePETotalIV from nexcorio_option_atm_movement_data where f_main_instrument=" + this.mainInstrumentId
					+ " AND id < " + atmId + "  order by id desc limit 1";
			ResultSet rs = stmt.executeQuery(fetchSql);
			while (rs.next()) {
				retMap.put("fullRangeCETotalIV", rs.getFloat("fullRangeCETotalIV"));
				retMap.put("fullRangePETotalIV", rs.getFloat("fullRangePETotalIV"));
			}
			rs.close();
			stmt.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		
		
		return retMap;
	}
	private float getFutureOI() {
		float retVal =0f;
		Connection conn = null;
		try {
			conn = HDataSource.getConnection();
			Statement stmt = conn.createStatement();
			
			String futurePrefix = getNextNFUTUREExpiryDatePrefix(2L, "NFO-FUT");
			
			SimpleDateFormat postgresLongDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			
			String fetchSql = "select total_buy_qty-total_sell_qty as open_interest from nexcorio_tick_data where trading_symbol = '" + futurePrefix +"'"
					+( " and quote_time <='" + postgresLongDateFormat.format( recTimestamp.getTime() )+ "'") 
					+ " order by quote_time desc limit 1";
			fileLogTelegramWriter.write(fetchSql);
			ResultSet rs = stmt.executeQuery(fetchSql);
			while (rs.next()) {
				retVal = rs.getFloat("open_interest");
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

	protected String getNextNFUTUREExpiryDatePrefix(Long mainInstrumentId, String exchange) {
		fileLogTelegramWriter.write("In getNextNFUTUREExpiryDatePrefix exchange="+exchange);
		String retStr = "";
		
		Connection conn = null;
		Statement stmt = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			stmt = conn.createStatement();
			
			String fnoExchange = "NFO-FUT";
			if (exchange.equalsIgnoreCase("BSE")) fnoExchange = "BFO-FUT";
			
			SimpleDateFormat postgresShortDateFormat = new SimpleDateFormat("yyyy-MM-dd");
			
			String fetchSql = "SELECT fno_prefix from nexcorio_fno_expiry_dates WHERE f_main_instrument="+mainInstrumentId
					+ " and fno_segment='" + fnoExchange + "' "
					+ " and expiry_date >= '" + postgresShortDateFormat.format(recTimestamp.getTime()) + "' "
					+ " ORDER BY expiry_date ASC LIMIT 1";
			
			ResultSet rs = stmt.executeQuery(fetchSql);
			
			while(rs.next()) {
				retStr = rs.getString("fno_prefix") + "FUT";
			}
			rs.close();			
			stmt.close();
			fileLogTelegramWriter.write("retStr="+retStr);
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (conn!=null) conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return retStr;
	}
	
	private void saveAccumulatedChangeInIVGreek(Map<String, Float> retMap, OptionGreek adjustedATMCEGreek, OptionGreek adjustedATMPEGreek) {
		Connection conn = null;
		try {
			conn = HDataSource.getConnection();
			Statement stmt = conn.createStatement();
			
			SimpleDateFormat postgresLongDateFormat  = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			SimpleDateFormat postgresShortDateFormat = new SimpleDateFormat("yyyy-MM-dd");
			
			String updateSql = "UPDATE nexcorio_option_atm_movement_data set "
					
					
//				+ "  tmpaccmlcetheta=" + retMap.get("tmpaccmlcetheta")
//				+ ", tmpaccmlpetheta=" + retMap.get("tmpaccmlpetheta")
					
					
					+ "  tmp5seccetheta=" + retMap.get("tmpaccmlcetheta")
					+ ", tmp5secpetheta=" + retMap.get("tmpaccmlpetheta")
					+ ", tmpaccmlcetheta=" + "(SELECT sum(tmp5seccetheta) FROM nexcorio_option_atm_movement_data"
							+ " where f_main_instrument=2 and record_time > '" + postgresShortDateFormat.format(recTimestamp) + " 09:15:05' and record_time <= '" + postgresLongDateFormat.format(recTimestamp) + "')"
					+ ", tmpaccmlpetheta=" + "(SELECT sum(tmp5secpetheta) FROM nexcorio_option_atm_movement_data"
							+ " where f_main_instrument=2 and record_time > '" + postgresShortDateFormat.format(recTimestamp) + " 09:15:05' and record_time <= '" + postgresLongDateFormat.format(recTimestamp) + "')"

//					+ ", itm1000x500AvgCeIv=" + retMap.get("itm1000x500AvgCeIv")
//					+ ", itm1000x500AvgPeIv=" + retMap.get("itm1000x500AvgPeIv")

//			
//			retMap.put("", (float) itm1000x500CeIvs.stream().mapToDouble(d -> d.getIv()).average().orElse(0.0));
//			retMap.put("itm1000x500AvgPeIv", (float) itm1000x500PeIvs.stream().mapToDouble(d -> d.getIv()).average().orElse(0.0));
					
					
					
					
					
//					+ "  minGammaExposure0_250StrikeDistance=" + retMap.get("minGammaExposure0_250StrikeDistance")
//					+ ", maxGammaExposure0_250StrikeDistance=" + retMap.get("maxGammaExposure0_250StrikeDistance")
//					+ ", minGammaExposure0_500StrikeDistance=" + retMap.get("minGammaExposure0_500StrikeDistance")
//					+ ", maxGammaExposure0_500StrikeDistance=" + retMap.get("maxGammaExposure0_500StrikeDistance")
					
//					+ "  otm1000_750Changein5secCeTheta=" + retMap.get("otm1000_750Changein5secCeTheta")
//					+ ", otm1000_750Changein5secPeTheta=" + retMap.get("otm1000_750Changein5secPeTheta")
//					+ ", otm1000_750AccmlCeTheta=" + "(SELECT sum(otm1000_750Changein5secCeTheta) FROM nexcorio_option_atm_movement_data"
//							+ " where f_main_instrument=2 and record_time > '" + postgresShortDateFormat.format(recTimestamp) + " 09:15:05' and record_time <= '" + postgresLongDateFormat.format(recTimestamp) + "')"
//					+ ", otm1000_750AccmlPeTheta=" + "(SELECT sum(otm1000_750Changein5secPeTheta) FROM nexcorio_option_atm_movement_data"
//							+ " where f_main_instrument=2 and record_time > '" + postgresShortDateFormat.format(recTimestamp) + " 09:15:05' and record_time <= '" + postgresLongDateFormat.format(recTimestamp) + "')"
//				
//					+ ", otm750_500Changein5secCeTheta=" + retMap.get("otm750_500Changein5secCeTheta")
//					+ ", otm750_500Changein5secPeTheta=" + retMap.get("otm750_500Changein5secPeTheta")
//					+ ", otm750_500AccmlCeTheta=" + "(SELECT sum(otm750_500Changein5secCeTheta) FROM nexcorio_option_atm_movement_data"
//							+ " where f_main_instrument=2 and record_time > '" + postgresShortDateFormat.format(recTimestamp) + " 09:15:05' and record_time <= '" + postgresLongDateFormat.format(recTimestamp) + "')"
//					+ ", otm750_500AccmlPeTheta=" + "(SELECT sum(otm750_500Changein5secPeTheta) FROM nexcorio_option_atm_movement_data"
//							+ " where f_main_instrument=2 and record_time > '" + postgresShortDateFormat.format(recTimestamp) + " 09:15:05' and record_time <= '" + postgresLongDateFormat.format(recTimestamp) + "')"
//
//					+ ", otm500_250Changein5secCeTheta=" + retMap.get("otm500_250Changein5secCeTheta")
//					+ ", otm500_250Changein5secPeTheta=" + retMap.get("otm500_250Changein5secPeTheta")
//					+ ", otm500_250AccmlCeTheta=" + "(SELECT sum(otm500_250Changein5secCeTheta) FROM nexcorio_option_atm_movement_data"
//							+ " where f_main_instrument=2 and record_time > '" + postgresShortDateFormat.format(recTimestamp) + " 09:15:05' and record_time <= '" + postgresLongDateFormat.format(recTimestamp) + "')"
//					+ ", otm500_250AccmlPeTheta=" + "(SELECT sum(otm500_250Changein5secPeTheta) FROM nexcorio_option_atm_movement_data"
//							+ " where f_main_instrument=2 and record_time > '" + postgresShortDateFormat.format(recTimestamp) + " 09:15:05' and record_time <= '" + postgresLongDateFormat.format(recTimestamp) + "')"
//					
//					+ ", otm250_0Changein5secCeTheta=" + retMap.get("otm250_0Changein5secCeTheta")
//					+ ", otm250_0Changein5secPeTheta=" + retMap.get("otm250_0Changein5secPeTheta")
//					+ ", otm250_0AccmlCeTheta=" + "(SELECT sum(otm250_0Changein5secCeTheta) FROM nexcorio_option_atm_movement_data"
//							+ " where f_main_instrument=2 and record_time > '" + postgresShortDateFormat.format(recTimestamp) + " 09:15:05' and record_time <= '" + postgresLongDateFormat.format(recTimestamp) + "')"
//					+ ", otm250_0AccmlPeTheta=" + "(SELECT sum(otm250_0Changein5secPeTheta) FROM nexcorio_option_atm_movement_data"
//							+ " where f_main_instrument=2 and record_time > '" + postgresShortDateFormat.format(recTimestamp) + " 09:15:05' and record_time <= '" + postgresLongDateFormat.format(recTimestamp) + "')"
//							
//					+ ", otm0_250Changein5secCeTheta=" + retMap.get("otm0_250Changein5secCeTheta")
//					+ ", otm0_250Changein5secPeTheta=" + retMap.get("otm0_250Changein5secPeTheta")
//					+ ", otm0_250AccmlCeTheta=" + "(SELECT sum(otm0_250Changein5secCeTheta) FROM nexcorio_option_atm_movement_data"
//							+ " where f_main_instrument=2 and record_time > '" + postgresShortDateFormat.format(recTimestamp) + " 09:15:05' and record_time <= '" + postgresLongDateFormat.format(recTimestamp) + "')"
//					+ ", otm0_250AccmlPeTheta=" + "(SELECT sum(otm0_250Changein5secPeTheta) FROM nexcorio_option_atm_movement_data"
//							+ " where f_main_instrument=2 and record_time > '" + postgresShortDateFormat.format(recTimestamp) + " 09:15:05' and record_time <= '" + postgresLongDateFormat.format(recTimestamp) + "')"
//							
//					+ ", otm250_500Changein5secCeTheta=" + retMap.get("otm250_500Changein5secCeTheta")
//					+ ", otm250_500Changein5secPeTheta=" + retMap.get("otm250_500Changein5secPeTheta")
//					+ ", otm250_500AccmlCeTheta=" + "(SELECT sum(otm250_500Changein5secCeTheta) FROM nexcorio_option_atm_movement_data"
//							+ " where f_main_instrument=2 and record_time > '" + postgresShortDateFormat.format(recTimestamp) + " 09:15:05' and record_time <= '" + postgresLongDateFormat.format(recTimestamp) + "')"
//					+ ", otm250_500AccmlPeTheta=" + "(SELECT sum(otm250_500Changein5secPeTheta) FROM nexcorio_option_atm_movement_data"
//							+ " where f_main_instrument=2 and record_time > '" + postgresShortDateFormat.format(recTimestamp) + " 09:15:05' and record_time <= '" + postgresLongDateFormat.format(recTimestamp) + "')"
//
//					+ ", otm500_750Changein5secCeTheta=" + retMap.get("otm500_750Changein5secCeTheta")
//					+ ", otm500_750Changein5secPeTheta=" + retMap.get("otm500_750Changein5secPeTheta")
//					+ ", otm500_750AccmlCeTheta=" + "(SELECT sum(otm500_750Changein5secCeTheta) FROM nexcorio_option_atm_movement_data"
//							+ " where f_main_instrument=2 and record_time > '" + postgresShortDateFormat.format(recTimestamp) + " 09:15:05' and record_time <= '" + postgresLongDateFormat.format(recTimestamp) + "')"
//					+ ", otm500_750AccmlPeTheta=" + "(SELECT sum(otm500_750Changein5secPeTheta) FROM nexcorio_option_atm_movement_data"
//							+ " where f_main_instrument=2 and record_time > '" + postgresShortDateFormat.format(recTimestamp) + " 09:15:05' and record_time <= '" + postgresLongDateFormat.format(recTimestamp) + "')"
//						
//					+ ", otm750_1000Changein5secCeTheta=" + retMap.get("otm750_1000Changein5secCeTheta")
//					+ ", otm750_1000Changein5secPeTheta=" + retMap.get("otm750_1000Changein5secPeTheta")
//					+ ", otm750_1000AccmlCeTheta=" + "(SELECT sum(otm750_1000Changein5secCeTheta) FROM nexcorio_option_atm_movement_data"
//							+ " where f_main_instrument=2 and record_time > '" + postgresShortDateFormat.format(recTimestamp) + " 09:15:05' and record_time <= '" + postgresLongDateFormat.format(recTimestamp) + "')"
//					+ ", otm750_1000AccmlPeTheta=" + "(SELECT sum(otm750_1000Changein5secPeTheta) FROM nexcorio_option_atm_movement_data"
//							+ " where f_main_instrument=2 and record_time > '" + postgresShortDateFormat.format(recTimestamp) + " 09:15:05' and record_time <= '" + postgresLongDateFormat.format(recTimestamp) + "')"
//							
//					
//					+ ", otm1000_750AvgCeIv=" + retMap.get("otm1000_750AvgCeIv")
//					+ ", otm1000_750AvgPeIv=" + retMap.get("otm1000_750AvgPeIv")
//					+ ", otm750_500AvgCeIv=" + retMap.get("otm750_500AvgCeIv")
//					+ ", otm750_500AvgPeIv=" + retMap.get("otm750_500AvgPeIv")
//					+ ", otm500_250AvgCeIv=" + retMap.get("otm500_250AvgCeIv")
//					+ ", otm500_250AvgPeIv=" + retMap.get("otm500_250AvgPeIv")
//					+ ", otm250_0AvgCeIv=" + retMap.get("otm250_0AvgCeIv")
//					+ ", otm250_0AvgPeIv=" + retMap.get("otm250_0AvgPeIv")
//					+ ", otm0_250AvgCeIv=" + retMap.get("otm0_250AvgCeIv")
//					+ ", otm0_250AvgPeIv=" + retMap.get("otm0_250AvgPeIv")
//					+ ", otm250_500AvgCeIv=" + retMap.get("otm250_500AvgCeIv")
//					+ ", otm250_500AvgPeIv=" + retMap.get("otm250_500AvgPeIv")
//					+ ", otm500_750AvgCeIv=" + retMap.get("otm500_750AvgCeIv")
//					+ ", otm500_750AvgPeIv=" + retMap.get("otm500_750AvgPeIv")
//					+ ", otm750_1000AvgCeIv=" + retMap.get("otm750_1000AvgCeIv")
//					+ ", otm750_1000AvgPeIv=" + retMap.get("otm750_1000AvgPeIv")
//
//						
//
////			
////			retMap.put("otm750_1000Changein5secCeTheta", );
////			retMap.put("otm750_1000Changein5secPeTheta", );
//			
//			
//					+ ", whlStrkOTMChangein5secCeTheta=" + retMap.get("whlStrkOTMChangein5secCeTheta")
//					+ ", whlStrkOTMChangein5secPeTheta=" + retMap.get("whlStrkOTMChangein5secPeTheta")
//					+ ", whlStrkOTMAccmlCETheta=" + "(SELECT sum(whlStrkOTMChangein5secCeTheta) FROM nexcorio_option_atm_movement_data"
//							+ " where f_main_instrument=2 and record_time > '" + postgresShortDateFormat.format(recTimestamp) + " 09:15:05' and record_time <= '" + postgresLongDateFormat.format(recTimestamp) + "')"
//					+ ", whlStrkOTMAccmlPETheta=" + "(SELECT sum(whlStrkOTMChangein5secPeTheta) FROM nexcorio_option_atm_movement_data"
//							+ " where f_main_instrument=2 and record_time > '" + postgresShortDateFormat.format(recTimestamp) + " 09:15:05' and record_time <= '" + postgresLongDateFormat.format(recTimestamp) + "')"
//							
//					+ ", whlStrkATMChangein5secCeTheta=" + retMap.get("whlStrkATMChangein5secCeTheta")
//					+ ", whlStrkATMChangein5secPeTheta=" + retMap.get("whlStrkATMChangein5secPeTheta")
//					+ ", whlStrkATMAccmlCETheta=" + "(SELECT sum(whlStrkATMChangein5secCeTheta) FROM nexcorio_option_atm_movement_data"
//							+ " where f_main_instrument=2 and record_time > '" + postgresShortDateFormat.format(recTimestamp) + " 09:15:05' and record_time <= '" + postgresLongDateFormat.format(recTimestamp) + "')"
//					+ ", whlStrkATMAccmlPETheta=" + "(SELECT sum(whlStrkATMChangein5secPeTheta) FROM nexcorio_option_atm_movement_data"
//							+ " where f_main_instrument=2 and record_time > '" + postgresShortDateFormat.format(recTimestamp) + " 09:15:05' and record_time <= '" + postgresLongDateFormat.format(recTimestamp) + "')"
//							
//					+ ", whlStrkITMChangein5secCeTheta=" + retMap.get("whlStrkITMChangein5secCeTheta")
//					+ ", whlStrkITMChangein5secPeTheta=" + retMap.get("whlStrkITMChangein5secPeTheta")
//					+ ", whlStrkITMAccmlCETheta=" + "(SELECT sum(whlStrkITMChangein5secCeTheta) FROM nexcorio_option_atm_movement_data"
//							+ " where f_main_instrument=2 and record_time > '" + postgresShortDateFormat.format(recTimestamp) + " 09:15:05' and record_time <= '" + postgresLongDateFormat.format(recTimestamp) + "')"
//					+ ", whlStrkITMAccmlPETheta=" + "(SELECT sum(whlStrkITMChangein5secPeTheta) FROM nexcorio_option_atm_movement_data"
//							+ " where f_main_instrument=2 and record_time > '" + postgresShortDateFormat.format(recTimestamp) + " 09:15:05' and record_time <= '" + postgresLongDateFormat.format(recTimestamp) + "')"
//										
//					+ ", drWhlStrkChangein5secCeIV=" + retMap.get("drWhlStrkChangein5secCeIV")
//					+ ", drWhlStrkChangein5secPeIV=" + retMap.get("drWhlStrkChangein5secPeIV")
//					+ ", drWhlStrkaccumulatedchangein5secceIV=" + "(SELECT sum(drWhlStrkChangein5secCeIV) FROM nexcorio_option_atm_movement_data"
//							+ " where f_main_instrument=2 and record_time > '" + postgresShortDateFormat.format(recTimestamp) + " 09:15:05' and record_time <= '" + postgresLongDateFormat.format(recTimestamp) + "')"
//					+ ", drWhlStrkaccumulatedchangein5secpeIV=" + "(SELECT sum(drWhlStrkChangein5secPeIV) FROM nexcorio_option_atm_movement_data"
//							+ " where f_main_instrument=2 and record_time > '" + postgresShortDateFormat.format(recTimestamp) + " 09:15:05' and record_time <= '" + postgresLongDateFormat.format(recTimestamp) + "')"
//							
//					+ ", drWhlStrkChangein5secCeLtp=" + retMap.get("drWhlStrkChangein5secCeLtp")
//					+ ", drWhlStrkChangein5secPeLtp=" + retMap.get("drWhlStrkChangein5secPeLtp")
//					+ ", drWhlStrkaccumulatedchangein5secceLtp=" + "(SELECT sum(drWhlStrkChangein5secCeLtp) FROM nexcorio_option_atm_movement_data"
//							+ " where f_main_instrument=2 and record_time > '" + postgresShortDateFormat.format(recTimestamp) + " 09:15:05' and record_time <= '" + postgresLongDateFormat.format(recTimestamp) + "')"
//					+ ", drWhlStrkaccumulatedchangein5secpeLtp=" + "(SELECT sum(drWhlStrkChangein5secPeLtp) FROM nexcorio_option_atm_movement_data"
//							+ " where f_main_instrument=2 and record_time > '" + postgresShortDateFormat.format(recTimestamp) + " 09:15:05' and record_time <= '" + postgresLongDateFormat.format(recTimestamp) + "')"

					+ " where id="+atmId;
			
//			String updateSql = "UPDATE nexcorio_option_atm_movement_data set "
//			+ "  accumulatedtop5eachtypechangein5secceiv=" + retMap.get("dr49ExOutlierCEAvgIv")
//			+ ", accumulatedtop5eachtypechangein5secpeiv=" + retMap.get("dr49ExOutlierPEAvgIv")
//			+ " where id="+atmId;
//			
			
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
	
	private void saveGreek(Map<String, Float> retMap, OptionGreek adjustedATMCEGreek, OptionGreek adjustedATMPEGreek) {
		Connection conn = null;
		try {
			conn = HDataSource.getConnection();
			Statement stmt = conn.createStatement();
			
			String updateSql = "UPDATE nexcorio_option_atm_movement_data set "
					
//					+ "  outlierCEMinIV=" + retMap.get("outlierCEMinIV")
//					+ ", outlierPEMinIV=" + retMap.get("outlierPEMinIV")
//					
//					+ ", outlierCEMaxIV=" + retMap.get("outlierCEMaxIV")
//					+ ", outlierPEMaxIV=" + retMap.get("outlierPEMaxIV")
//					
//					+ ", outlierCETotalIV=" + retMap.get("outlierCETotalIV")
//					+ ", outlierPETotalIV=" + retMap.get("outlierPETotalIV")
//					
//					+ ", outlierCEAvgIV=" + retMap.get("outlierCEAvgIV")
//					+ ", outlierPEAvgIV=" + retMap.get("outlierPEAvgIV")
//					
//					+ ", outlierCEMedianIV=" + retMap.get("outlierCEMedianIV")
//					+ ", outlierPEMedianIV=" + retMap.get("outlierPEMedianIV")
					
//					+ " minGammaExposure=" + retMap.get("minGammaExposure")
//					+ ",maxGammaExposure=" + retMap.get("maxGammaExposure")
//					+ ",netGammaExposure=" + retMap.get("netGammaExposure")
//					
//					+ ",minGammaExposureWithStrike=" + retMap.get("minGammaExposureWithStrike")
//					+ ",maxGammaExposureWithStrike=" + retMap.get("maxGammaExposureWithStrike")
//					+ ",netGammaExposureWithStrike=" + retMap.get("netGammaExposureWithStrike")
//					
//					+ ",minGammaExposureTopN=" + retMap.get("minGammaExposureTopN")
//					+ ",maxGammaExposureTopN=" + retMap.get("maxGammaExposureTopN")
//					+ ",netGammaExposureTopN=" + retMap.get("netGammaExposureTopN")
					
					
					+ " ceDelta1_2Count=" + retMap.get("ceDelta1_2Count")
					+ ",ceDelta2_8Count=" + retMap.get("ceDelta2_8Count")
					+ ",ceDelta8_9Count=" + retMap.get("ceDelta8_9Count")
					
					+ ",peDelta1_2Count=" + retMap.get("peDelta1_2Count")
					+ ",peDelta2_8Count=" + retMap.get("peDelta2_8Count")
					+ ",peDelta8_9Count=" + retMap.get("peDelta8_9Count")
					
					
//					+ " cumulativeCEAvgIVDiff=" + retMap.get("cumulativeCEAvgIVDiff")
//					+ ",cumulativePEAvgIVDiff=" + retMap.get("cumulativePEAvgIVDiff")
					
//					+ " minGameXpWithStrikeXoutlier=" + retMap.get("minGameXpWithStrikeXoutlier")
//					+ ",maxGameXpWithStrikeXoutlier=" + retMap.get("maxGameXpWithStrikeXoutlier")
//					+ ",netGameXpWithStrikeXoutlier=" + retMap.get("netGameXpWithStrikeXoutlier")
//					
//					+ ", minGammaExposureStrike="+ retMap.get("minGammaExposureStrike")
//					+ ", maxGammaExposureStrike="+ retMap.get("maxGammaExposureStrike")
//					
//					+ ", selectivestrike_avgcegamma=" + retMap.get("selective5AvgCEGamma")
//					+ ", selectivestrike_avgpegamma=" + retMap.get("selective5AvgPEGamma")
					
//					+ " wholestrikecedeltaoi=" + retMap.get("futureOI")
//					
//					+ "  changein5secceiv=" + retMap.get("changein5secCeIV")
//					+ ", changein5secpeiv=" + retMap.get("changein5secPeIV")
					//+ ", wholestrikepedeltaoi=" + retMap.get("top5PeGamma")
										
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
	
	public float getPriceFromTicks(String instrumentName) {
		float retVal = 0f;
		
		Connection conn = null;
		try {
			SimpleDateFormat postgresLongDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			String fetchSql = "select quote_time, last_traded_price from nexcorio_tick_data where trading_symbol = '" + instrumentName +"'"
					+( " and quote_time <='" + postgresLongDateFormat.format( recTimestamp.getTime() )+ "'") 
					+ " order by quote_time desc limit 1";
			fileLogTelegramWriter.write(fetchSql);
			ResultSet rs = stmt.executeQuery(fetchSql);
			while (rs.next()) {
				retVal = rs.getFloat("last_traded_price");
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
	
	private OptionGreek[] getATMGreeks(List<OptionGreek> ceOptionGreeks, List<OptionGreek> peOptionGreeks, float baseDelta) {
		
		OptionGreek[] returnGreeks = null;
		
		// First search CE matching required delta
		OptionGreek ceOptionGreek = null;
		float minDeltaGap = 1f;
		for(OptionGreek aGreek: ceOptionGreeks) {
			float deltaGap = Math.abs(Math.abs(aGreek.getDelta())-baseDelta);
			if (deltaGap < minDeltaGap) {
				minDeltaGap = deltaGap;
				ceOptionGreek = aGreek;
			}
		}
		
		// Next search PE matching required delta
		OptionGreek peOptionGreek = null;
		minDeltaGap = 1f;
		for(OptionGreek aGreek: peOptionGreeks) {
			float deltaGap = Math.abs(Math.abs(aGreek.getDelta())-baseDelta);
			if (deltaGap < minDeltaGap) {
				minDeltaGap = deltaGap;
				peOptionGreek = aGreek;
			}
		}
		
		returnGreeks = new OptionGreek[]{ceOptionGreek, peOptionGreek};
		return returnGreeks;
	}
	
	private OptionGreek[] getExactATMQuandrangle(List<OptionGreek> ceOptionGreeks, List<OptionGreek> peOptionGreeks, float baseDelta) {
		OptionGreek[] returnGreeks = null;
		try {
			float minDelta = 1f;
			OptionGreek lowerCEOptionGreek = null;
			for(OptionGreek aGreek: ceOptionGreeks) {
				if (baseDelta - Math.abs(aGreek.getDelta()) >= 0 ) {
					float deltaDiff = baseDelta - Math.abs(aGreek.getDelta());
					if (deltaDiff < minDelta) {
						minDelta = deltaDiff;
						lowerCEOptionGreek = aGreek;
					}
				}				
			}
			minDelta = 1f;
			OptionGreek upperCEOptionGreek = null;
			for(OptionGreek aGreek: ceOptionGreeks) {
				if (Math.abs(aGreek.getDelta())-baseDelta >= 0 ) {
					float deltaDiff = Math.abs(aGreek.getDelta()) - baseDelta;
					if (deltaDiff < minDelta) {
						minDelta = deltaDiff;
						upperCEOptionGreek = aGreek;
					}
				}				
			}
			minDelta = 1f;
			OptionGreek lowerPEOptionGreek = null;
			for(OptionGreek aGreek: peOptionGreeks) {
				if (baseDelta - Math.abs(aGreek.getDelta()) >= 0 ) {
					float deltaDiff = baseDelta - Math.abs(aGreek.getDelta());
					if (deltaDiff < minDelta) {
						minDelta = deltaDiff;
						lowerPEOptionGreek = aGreek;
					}
				}				
			}
			minDelta = 1f;
			OptionGreek upperPEOptionGreek = null;
			for(OptionGreek aGreek: peOptionGreeks) {
				if (Math.abs(aGreek.getDelta())-baseDelta >= 0 ) {
					float deltaDiff = Math.abs(aGreek.getDelta()) - baseDelta;
					if (deltaDiff < minDelta) {
						minDelta = deltaDiff;
						upperPEOptionGreek = aGreek;
					}
				}				
			}
//			print(lowerCEOptionGreek);
//			print(upperCEOptionGreek);
//			print(lowerPEOptionGreek);
//			print(upperPEOptionGreek);
			
			float adjustedCEATMLtp = getScaledValue(Math.abs(lowerCEOptionGreek.getDelta()), Math.abs(upperCEOptionGreek.getDelta()), lowerCEOptionGreek.getLtp(), upperCEOptionGreek.getLtp(), 0.5f);
			float adjustedCEATMIV  = getScaledValue(Math.abs(lowerCEOptionGreek.getDelta()), Math.abs(upperCEOptionGreek.getDelta()), lowerCEOptionGreek.getIv(),  upperCEOptionGreek.getIv(),  0.5f);
			float adjustedCEATMGamma  = getScaledValue(Math.abs(lowerCEOptionGreek.getDelta()), Math.abs(upperCEOptionGreek.getDelta()), lowerCEOptionGreek.getGamma(),  upperCEOptionGreek.getGamma(),  0.5f);
			float adjustedCEATMVega = getScaledValue(Math.abs(lowerCEOptionGreek.getDelta()), Math.abs(upperCEOptionGreek.getDelta()), lowerCEOptionGreek.getVega(),  upperCEOptionGreek.getVega(),  0.5f);
			float adjustedCEATMTheta = getScaledValue(Math.abs(lowerCEOptionGreek.getDelta()), Math.abs(upperCEOptionGreek.getDelta()), lowerCEOptionGreek.getTheta(),  upperCEOptionGreek.getTheta(),  0.5f);
			
			float adjustedPEATMLtp = getScaledValue(Math.abs(lowerPEOptionGreek.getDelta()), Math.abs(upperPEOptionGreek.getDelta()), lowerPEOptionGreek.getLtp(), upperPEOptionGreek.getLtp(), 0.5f);
			float adjustedPEATMIV  = getScaledValue(Math.abs(lowerPEOptionGreek.getDelta()), Math.abs(upperPEOptionGreek.getDelta()), lowerPEOptionGreek.getIv(),  upperPEOptionGreek.getIv(),  0.5f);
			float adjustedPEATMGamma  = getScaledValue(Math.abs(lowerPEOptionGreek.getDelta()), Math.abs(upperPEOptionGreek.getDelta()), lowerPEOptionGreek.getGamma(),  upperPEOptionGreek.getGamma(),  0.5f);
			float adjustedPEATMVega = getScaledValue(Math.abs(lowerPEOptionGreek.getDelta()), Math.abs(upperPEOptionGreek.getDelta()), lowerPEOptionGreek.getVega(),  upperPEOptionGreek.getVega(),  0.5f);
			float adjustedPEATMTheta = getScaledValue(Math.abs(lowerPEOptionGreek.getDelta()), Math.abs(upperPEOptionGreek.getDelta()), lowerPEOptionGreek.getTheta(),  upperPEOptionGreek.getTheta(),  0.5f);
			
			OptionGreek adjustedCEReturnGreek = new OptionGreek("DummyCE", adjustedCEATMIV, 0.5f,adjustedCEATMVega, adjustedCEATMTheta, adjustedCEATMGamma, adjustedCEATMLtp);
			OptionGreek adjustedPEReturnGreek = new OptionGreek("DummyPE", adjustedPEATMIV, 0.5f,adjustedPEATMVega, adjustedPEATMTheta, adjustedPEATMGamma, adjustedPEATMLtp);
			
			returnGreeks = new OptionGreek[]{adjustedCEReturnGreek, adjustedPEReturnGreek};
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return returnGreeks;
	}
	
	protected void print(OptionGreek optionGreekDto) {
		if (optionGreekDto!=null) {
			fileLogTelegramWriter.write( "[" + optionGreekDto.getTradingSymbol()+"@" + optionGreekDto.getLtp() + "] IV=" + optionGreekDto.getIv()+" Delta="+optionGreekDto.getDelta()+" Gamma="+optionGreekDto.getGamma()+" Vega="+optionGreekDto.getVega()+" Theta="+optionGreekDto.getTheta());
		}
	}
	
	private float getScaledValue(float lowerDelta, float upperDelta, float lowerLtp, float upperLtp, float targetValue) {
		
		float retVal =  lowerLtp + (targetValue - lowerDelta)*(upperLtp-lowerLtp)/(upperDelta-lowerDelta);
		
		return retVal;
	}
	protected OptionGreek getOptionGreeks(String optionName, int lagSecond) {
		
		if (optionName==null || optionName.equals("")) return null;
		
		OptionGreek retVal = null;
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			Calendar cal = Calendar.getInstance();
			cal.setTime(recTimestamp);
			if (lagSecond!=0) {
				cal.add(Calendar.SECOND, lagSecond);
			}
			
			SimpleDateFormat postgresLongDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

			
			String fetchSql = "select iv, delta, vega, theta, gamma, ltp, oi, underlying_value from nexcorio_option_greeks  where trading_symbol = '" + optionName + "'"
					+ " and f_main_instrument=" + this.mainInstrumentId
					+ " and quote_time <= '" + postgresLongDateFormat.format(cal.getTime()) + "'"
					+ " order by quote_time desc limit 1";
			//System.out.println("recTimestamp="+recTimestamp+" sql=" +fetchSql);
			
			ResultSet rs = stmt.executeQuery(fetchSql);
			while (rs.next()) {
				retVal = new OptionGreek(optionName, rs.getFloat("iv"), rs.getFloat("delta"), rs.getFloat("vega"), rs.getFloat("theta"), rs.getFloat("gamma"), rs.getFloat("ltp"), rs.getFloat("oi"));
				retVal.setUnderlyingValue(rs.getFloat("underlying_value"));
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
