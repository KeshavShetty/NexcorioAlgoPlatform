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
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.util.ApplicationConfig;
import com.nexcorio.algo.util.FileLogTelegramWriter;
import com.nexcorio.algo.util.KiteUtil;
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
	float instrumentLtp;
	List<String> optionnames;
	Timestamp recTimestamp;
	FileLogTelegramWriter fileLogTelegramWriter = null;
	long mainInstrumentId;
	
	public ATMThread(Long atmId, float underlyingInstrumentLtp, List<String> optionnames, Timestamp timestamp, long mainInstrumentId) {
		super();
		this.atmId = atmId;
		this.optionnames = optionnames;
		this.instrumentLtp = underlyingInstrumentLtp;
		this.recTimestamp = timestamp;
		this.mainInstrumentId = mainInstrumentId;
		Thread t = new Thread(this, atmId+"");
		t.setPriority(Thread.MAX_PRIORITY);
		t.start();
	}	
	
	@Override
	public void run() {
		Calendar cal = Calendar.getInstance();
		cal.setTime(recTimestamp);
		fileLogTelegramWriter = new FileLogTelegramWriter("NIFTY", this.atmId+"", cal);
		//System.out.println("Run reached");
		
		List<OptionGreek> ceOptionGreeks = new ArrayList<>();
		List<OptionGreek> peOptionGreeks = new ArrayList<>();
		
		List<OptionGreek> allGreeks = getOptionGreeks(optionnames, 0);
		
		for(OptionGreek aGreek : allGreeks ) {
			if (aGreek!=null) {
				if (aGreek.getTradingSymbol().endsWith("CE")) {
					ceOptionGreeks.add(aGreek);
				} else {
					peOptionGreeks.add(aGreek);
				}
			}
		}
		
		List<OptionGreek> prevCeOptionGreeks = new ArrayList<OptionGreek>();
		List<OptionGreek> prevPeOptionGreeks = new ArrayList<OptionGreek>();
		
		List<OptionGreek> allPrevGreeks = getOptionGreeks(optionnames, -400);
		for(OptionGreek aGreek : allPrevGreeks ) {
			if (aGreek!=null) {
				if (aGreek.getTradingSymbol().endsWith("CE")) prevCeOptionGreeks.add(aGreek);
				else prevPeOptionGreeks.add(aGreek);
			}
		}
		
		Map<String, OptionGreek> prevCeOptionGreeksMap = prevCeOptionGreeks.stream().collect(Collectors.toMap(OptionGreek::getTradingSymbol, item -> item));
		Map<String, OptionGreek> prevPeOptionGreeksMap = prevPeOptionGreeks.stream().collect(Collectors.toMap(OptionGreek::getTradingSymbol, item -> item));
		
		Map<String, Float> retMap = new HashMap<>();
		
		Collections.sort(allGreeks, new SortbyOiDesc());
		
		float prevCeOi = 0f;
		float prevPeOi = 0f;
		
		float curCeOi = 0f;
		float curPeOi = 0f;
		
		int recCount = 0;
		for(OptionGreek aGreek: allGreeks) {
			if (aGreek!=null) {
				if (recCount < 5) {
					if (aGreek.getTradingSymbol().endsWith("CE")) {
						curCeOi = curCeOi + aGreek.getOi();
						if (prevCeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null) prevCeOi = prevCeOi + prevCeOptionGreeksMap.get(aGreek.getTradingSymbol()).getOi();
					} else {
						curPeOi = curPeOi + aGreek.getOi();
						if (prevPeOptionGreeksMap.get(aGreek.getTradingSymbol())!=null)  prevPeOi = prevPeOi + prevPeOptionGreeksMap.get(aGreek.getTradingSymbol()).getOi();
					}
				} else break;
				recCount++;
			}
		}
		
		float rocInCeOi =  prevCeOi!=0 ? (curCeOi - prevCeOi)/prevCeOi :0f;
		float rocInPeOi =  prevPeOi!=0 ? (curPeOi - prevPeOi)/prevPeOi :0f;
		
		retMap.put("tmpaccmlcetheta", rocInCeOi);
		retMap.put("tmpaccmlpetheta", rocInPeOi);
		
		//saveGreek(retMap, adjustedATMCEGreek, adjustedATMPEGreek);
		saveAccumulatedChangeInIVGreek(retMap);
		
//		fileLogTelegramWriter.write("ceGreekDetails="+ceGreekDetails);
//		fileLogTelegramWriter.write("peGreekDetails="+peGreekDetails);
		
		//fileLogTelegramWriter.write("dr19WholeStrikeCEAvgIV="+retMap.get("dr19WholeStrikeCEAvgIV")+" dr19WholeStrikePEAvgIV="+retMap.get("dr19WholeStrikePEAvgIV"));
		
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
	
	private void saveAccumulatedChangeInIVGreek(Map<String, Float> retMap) {
		Connection conn = null;
		try {
			conn = HDataSource.getConnection();
			Statement stmt = conn.createStatement();
			
			SimpleDateFormat postgresLongDateFormat  = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			SimpleDateFormat postgresShortDateFormat = new SimpleDateFormat("yyyy-MM-dd");
			
			String updateSql = "UPDATE nexcorio_option_atm_movement_data set"
					

//				+ "  lowerStrikeCEAvgIv=" + retMap.get("lowerCEAvgIv")
//				+ ", lowerStrikePEAvgIv=" + retMap.get("lowerPEAvgIv")
//				
//				+ ", upperStrikeCEAvgIv=" + retMap.get("upperCEAvgIv")
//				+ ", upperStrikePEAvgIv=" + retMap.get("upperPEAvgIv")
				
//				+ "  fullOtm0x600CEGreeks=" + retMap.get("fullOtm0x600CEGreeks")
//				+ ", fullOtm0x600PEGreeks=" + retMap.get("fullOtm0x600PEGreeks")
//				
//				+ ", lowerOtm0x300CEGreeks=" + retMap.get("lowerOtm0x300CEGreeks")
//				+ ", lowerOtm0x300PEGreeks=" + retMap.get("lowerOtm0x300PEGreeks")
//					
//				+ ", upperOtm300x600CEGreeks=" + retMap.get("upperOtm300x600CEGreeks")
//				+ ", upperOtm300x600PEGreeks=" + retMap.get("upperOtm300x600PEGreeks")
//					
//				+ ", upperOtm150x300CEGreeks=" + retMap.get("upperOtm150x300CEGreeks")
//				+ ", upperOtm150x300PEGreeks=" + retMap.get("upperOtm150x300PEGreeks")
			
//				+ "  lowerstrikeceavgiv=" + retMap.get("lowerCEAvgIv")
//				+ ", lowerstrikepeavgiv=" + retMap.get("lowerPEAvgIv")
//				
//				+ ", upperstrikeceavgiv=" + retMap.get("upperCEAvgIv")
//				+ ", upperstrikepeavgiv=" + retMap.get("upperPEAvgIv")
//				
				+ "  tmpaccmlcetheta=" + retMap.get("tmpaccmlcetheta")
				+ ", tmpaccmlpetheta=" + retMap.get("tmpaccmlpetheta")
				
//				+ "  tmpchangecetheta=" + retMap.get("changeInCETheta")
//				+ ", tmpchangepetheta=" + retMap.get("changeInPETheta")
//				+ ", tmpaccmlcetheta=" + "(SELECT sum(tmpchangecetheta) FROM nexcorio_option_atm_movement_data"
//					+ " where f_main_instrument=2 and record_time > '" + postgresShortDateFormat.format(recTimestamp) + " 09:15:05' and record_time <= '" + postgresLongDateFormat.format(recTimestamp) + "')"
//				+ ", tmpaccmlpetheta=" + "(SELECT sum(tmpchangepetheta) FROM nexcorio_option_atm_movement_data"
//					+ " where f_main_instrument=2 and record_time > '" + postgresShortDateFormat.format(recTimestamp) + " 09:15:05' and record_time <= '" + postgresLongDateFormat.format(recTimestamp) + "')"
				
				

//					+ "  otm0_200CEChangeTheta=" + retMap.get("otm0_200CEAccmlTheta")
//					+ ", otm0_200PEChangeTheta=" + retMap.get("otm0_200PEAccmlTheta")
//					+ ", otm0_200CEAccmlTheta=" + "(SELECT sum(otm0_200CEChangeTheta) FROM nexcorio_option_atm_movement_data"
//						+ " where f_main_instrument=2 and record_time > '" + postgresShortDateFormat.format(recTimestamp) + " 09:15:05' and record_time <= '" + postgresLongDateFormat.format(recTimestamp) + "')"
//					+ ", otm0_200PEAccmlTheta=" + "(SELECT sum(otm0_200PEChangeTheta) FROM nexcorio_option_atm_movement_data"
//						+ " where f_main_instrument=2 and record_time > '" + postgresShortDateFormat.format(recTimestamp) + " 09:15:05' and record_time <= '" + postgresLongDateFormat.format(recTimestamp) + "')"
//					
//					+ ", otm200_400CEChangeTheta=" + retMap.get("otm200_400CEAccmlTheta")
//					+ ", otm200_400PEChangeTheta=" + retMap.get("otm200_400PEAccmlTheta")
//					+ ", otm200_400CEAccmlTheta=" + "(SELECT sum(otm200_400CEChangeTheta) FROM nexcorio_option_atm_movement_data"
//						+ " where f_main_instrument=2 and record_time > '" + postgresShortDateFormat.format(recTimestamp) + " 09:15:05' and record_time <= '" + postgresLongDateFormat.format(recTimestamp) + "')"
//					+ ", otm200_400PEAccmlTheta=" + "(SELECT sum(otm200_400PEChangeTheta) FROM nexcorio_option_atm_movement_data"
//						+ " where f_main_instrument=2 and record_time > '" + postgresShortDateFormat.format(recTimestamp) + " 09:15:05' and record_time <= '" + postgresLongDateFormat.format(recTimestamp) + "')"
//					
//					+ ", otm400_600CEChangeTheta=" + retMap.get("otm400_600CEAccmlTheta")
//					+ ", otm400_600PEChangeTheta=" + retMap.get("otm400_600PEAccmlTheta")
//					+ ", otm400_600CEAccmlTheta=" + "(SELECT sum(otm400_600CEChangeTheta) FROM nexcorio_option_atm_movement_data"
//						+ " where f_main_instrument=2 and record_time > '" + postgresShortDateFormat.format(recTimestamp) + " 09:15:05' and record_time <= '" + postgresLongDateFormat.format(recTimestamp) + "')"
//					+ ", otm400_600PEAccmlTheta=" + "(SELECT sum(otm400_600PEChangeTheta) FROM nexcorio_option_atm_movement_data"
//						+ " where f_main_instrument=2 and record_time > '" + postgresShortDateFormat.format(recTimestamp) + " 09:15:05' and record_time <= '" + postgresLongDateFormat.format(recTimestamp) + "')"
					
					+ " where id="+atmId;			
			
			System.out.println(recTimestamp + " " + updateSql);
			
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
	
	protected List<OptionGreek> getOptionGreeks(List<String> optionNames, int lagSecond) {
		
		List<OptionGreek> retList = new ArrayList<OptionGreek>();
		
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			Calendar cal = Calendar.getInstance();
			cal.setTime(recTimestamp);
			if (lagSecond!=0) {
				cal.add(Calendar.SECOND, lagSecond);
			}
			Date upto =  cal.getTime();
			
			cal.add(Calendar.SECOND, -300);
			Date fromTime =  cal.getTime();
			SimpleDateFormat postgresLongDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

			
			String fetchSql = "WITH RankedRows AS"
					+ " ("
					+ " SELECT "
					+ " trading_symbol, iv, delta, vega, theta, gamma, ltp, oi, underlying_value,"
					+ " ROW_NUMBER() OVER (PARTITION BY trading_symbol ORDER BY quote_time DESC, id DESC) AS rank"
					+ " FROM nexcorio_option_greeks"
					+ " WHERE trading_symbol IN (" + "'" + String.join("','", optionnames) + "'" + ")"
					+ " AND quote_time <= '" + postgresLongDateFormat.format(upto)  + "'"
					+ " AND quote_time >= '" + postgresLongDateFormat.format(fromTime)  + "'"
					+ " )"
					+ " SELECT trading_symbol, iv, delta, vega, theta, gamma, ltp, oi, underlying_value FROM RankedRows WHERE rank = 1";
			
			//System.out.println("fetchSql="+fetchSql);
			
			//System.out.println("recTimestamp="+recTimestamp+" sql=" +fetchSql);
			
			ResultSet rs = stmt.executeQuery(fetchSql);
			while (rs.next()) {
				OptionGreek retVal = new OptionGreek(rs.getString("trading_symbol"), rs.getFloat("iv"), rs.getFloat("delta"), rs.getFloat("vega"), rs.getFloat("theta"), rs.getFloat("gamma"), rs.getFloat("ltp"), rs.getFloat("oi"));
				retVal.setUnderlyingValue(rs.getFloat("underlying_value"));
				retList.add(retVal);
			}
			rs.close();
			stmt.close();
			
//			for(String aOptionName:optionNames) {
//				retList.add(getOptionGreeks(aOptionName, lagSecond));
//			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (conn!=null) conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return retList;
	}
	
	private int getStrike(String optionname) {
		return KiteUtil.getStrike(optionname);
	}
	
	public static void main(String[] args) {
		LinkedHashMap<String, Integer> sqlFields = new LinkedHashMap<>();
		int idx = 0;
		sqlFields.put("record_time", idx++);
		sqlFields.put("instrumentLtp", idx++);
		sqlFields.put("celtp", idx++);
		sqlFields.put("peltp", idx++);
		
		Iterator<String> iter = sqlFields.keySet().iterator();
		while(iter.hasNext()) {
			String aKkey = iter.next();
			System.out.println(aKkey + " " + sqlFields.get(aKkey));
		}
		
		
	}
}
