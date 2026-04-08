package com.nexcorio.algo.junk;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import com.nexcorio.algo.dto.OptionGreek;
import com.nexcorio.algo.util.db.HDataSource;

public class Top5OIMLThread {

	Long id;
	String optionname;
	float underlyingInstrumentLtp;
	Timestamp entryTime;
	float profit = 0f;
	List<String> optionnames;
	//FileLogTelegramWriter fileLogTelegramWriter = null;
	
	private SimpleDateFormat postgresLongDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private SimpleDateFormat postgresShortDateFormat = new SimpleDateFormat("yyyy-MM-dd");
	
	public Top5OIMLThread(List<String> optionnames, Long id, String optionname, Timestamp entryTime, float profit) { 
		super();
		this.id = id;
		this.optionname = optionname;
		this.entryTime = entryTime;
		this.profit = profit;
		this.optionnames = optionnames;
		
//		Thread t = new Thread(this, id+"");
//		t.setPriority(Thread.MAX_PRIORITY);
//		t.start();
	}

//	@Override
//	public void run() {
//		processRecords();
//	}
	
	public void processRecords() {
		
		Connection conn = null;
		try {			
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			List<OptionGreek> optionGreeks = new ArrayList<OptionGreek>();
			for(String optionname:optionnames ) {
				OptionGreek aGreek = getOptionGreeks(optionname, entryTime);
				if (aGreek!=null) {
					optionGreeks.add(aGreek);
				}
			}
			
			Collections.sort(optionGreeks, new SortbyOI());
			int recProcessed=0;
			float prevOI = 0f;
			
			float indexAt = getPriceFromTicks("NIFTY", entryTime);
			float vixAt = getPriceFromTicks("VIX", entryTime);
			
			List<OptionGreek> ceOptionGreeks = new ArrayList<OptionGreek>();
			List<OptionGreek> peOptionGreeks = new ArrayList<OptionGreek>();
			
			StringBuffer insertDataSql = new StringBuffer();
			insertDataSql.append(id);
			for(OptionGreek aGreek: optionGreeks) {
				if (aGreek.getOi()*aGreek.getLtp()/10000000>10) {
					recProcessed++;
					
					String tradingSymbol = aGreek.getTradingSymbol();
					int oiType = 0;
					
					float distance4mIndex = 0f;
					if (tradingSymbol.endsWith("CE")) {
						oiType = 0;
						distance4mIndex = aGreek.getStrike() - indexAt;
						ceOptionGreeks.add(aGreek);
					} else {
						oiType = 1;
						distance4mIndex = indexAt - aGreek.getStrike();
						peOptionGreeks.add(aGreek);
					}
					insertDataSql.append("," + oiType+"," + distance4mIndex + "," + aGreek.getGamma() + "," + Math.abs(aGreek.getDelta())+","+aGreek.getOi());
//					if (recProcessed!=1) {
//						insertDataSql.append("," + (prevOI!=0?aGreek.getOi()/prevOI:0));
//					}
					prevOI = aGreek.getOi();
				}
				if (recProcessed>=5) break;
			}
//			   "OI1Type,OI1DistanceFromStrike,OI1Gamma,OI1Delta"
//			+ ",OI2Type,OI2DistanceFromStrike,OI2Gamma,OI2Delta,OI2OIRatioWithPrev"
//			+ ",OI3Type,OI3DistanceFromStrike,OI3Gamma,OI3Delta,OI3OIRatioWithPrev"
			
			if (ceOptionGreeks.size()>1) {
				insertDataSql.append("," + (ceOptionGreeks.get(0).getStrike()-ceOptionGreeks.get(1).getStrike()));
			} else {
				insertDataSql.append(",2000");
			}
			
			if (peOptionGreeks.size()>1) {
				insertDataSql.append("," + (peOptionGreeks.get(1).getStrike()-peOptionGreeks.get(0).getStrike()));
			} else {
				insertDataSql.append(",2000");
			}
			
			// Target column
			if (profit>0) {
				if (optionname.endsWith("CE")) insertDataSql.append(",0"); 
				else insertDataSql.append(",1");
			} else {
				if (optionname.endsWith("CE")) insertDataSql.append(",1"); 
				else insertDataSql.append(",0");
			}
			
			//System.out.println(insertDataSql);
			
			String insertSql = "INSERT INTO tmp_ml_topoi (entry_time, id,OI1Type,OI1DistanceFromStrike,OI1Gamma,OI1Delta,oi1oi,OI2Type,OI2DistanceFromStrike,OI2Gamma,OI2Delta,oi2oi,OI3Type,OI3DistanceFromStrike,OI3Gamma,OI3Delta,oi3oi,OI4Type,OI4DistanceFromStrike,OI4Gamma,OI4Delta,oi4oi,OI5Type,OI5DistanceFromStrike,OI5Gamma,OI5Delta,oi5oi,sucCEStrikeDistance,sucPEStrikeDistance,Target)"
					+ " VALUES('" + postgresLongDateFormat.format(entryTime) + "'," + insertDataSql + ")";
			System.out.println(new Date() + " " + insertSql);
					
			stmt.execute(insertSql);
			
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
		//System.out.println(optionname+" " + entryTime +" Done");
	}
	
	private OptionGreek getOptionGreeks(String optionName, Timestamp entryTime) {
		
		if (optionName==null || optionName.equals("")) return null;
		
		OptionGreek retVal = null;
		Connection conn = null;
		try {
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			String fetchSql = "select iv, delta, vega, theta, gamma, ltp, oi from nexcorio_option_greeks  where trading_symbol = '" + optionName + "'"
					+ " and quote_time <='" + postgresLongDateFormat.format(entryTime)+ "'" 
					+ " and f_main_instrument=2"
					+ " order by quote_time desc limit 1";
			//fileLogTelegramWriter.write("In getOptionGreeks fetchSql="+fetchSql);
			ResultSet rs = stmt.executeQuery(fetchSql);
			while (rs.next()) {
				retVal = new OptionGreek(optionName, rs.getFloat("iv"), rs.getFloat("delta"), rs.getFloat("vega"), rs.getFloat("theta"), rs.getFloat("gamma"), rs.getFloat("ltp"), rs.getFloat("oi"));
			}
			rs.close();
			stmt.close();
			//System.out.println("retVal="+retVal);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (conn!=null) conn.close();
			} catch (SQLException e) {
			}
		}
		return retVal;
	}
	
	public float getPriceFromTicks(String instrumentName, Timestamp entryTime) {
		float retVal = 0f;
		
		Connection conn = null;
		try {
			SimpleDateFormat postgresLongDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			
			conn = HDataSource.getReadOnlyConnection();
			Statement stmt = conn.createStatement();
			
			String fetchSql = "select quote_time, last_traded_price from nexcorio_tick_data where trading_symbol = '" + instrumentName +"'"
					+ " and quote_time <='" + postgresLongDateFormat.format(entryTime)+ "'"
					+ " order by quote_time desc limit 1";
			//fileLogTelegramWriter.write(fetchSql);
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
