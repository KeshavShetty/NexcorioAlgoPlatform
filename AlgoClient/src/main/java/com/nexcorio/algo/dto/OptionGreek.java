package com.nexcorio.algo.dto;

import java.sql.Timestamp;

import com.nexcorio.algo.util.KiteUtil;

/**
 * 
 * @author Keshav Shetty
 *
 */
public class OptionGreek {

	private String tradingSymbol;
	
	private float iv;
	private float delta;
	private float vega; 
	private float theta;
	private float gamma;
	private float ltp;
	private float oi;
	private float changeInIv=0f;
	private long volumeTradedToday;
	
	private Timestamp quoteTime;
	
	private int strike=0; 
	
	private float underlyingValue;
	
	private float timevalue=0;
	
	public OptionGreek(String tradingSymbol, float iv, float delta, float vega, float theta, float gamma) {
		super();
		this.tradingSymbol = tradingSymbol;
		this.iv = iv;
		this.delta = delta;
		this.vega = vega;
		this.theta = theta;
		this.gamma = gamma;
	}
	
	public OptionGreek(String tradingSymbol, float iv, float delta, float vega, float theta, float gamma, float ltp) {
		super();
		this.tradingSymbol = tradingSymbol;
		this.iv = iv;
		this.delta = delta;
		this.vega = vega;
		this.theta = theta;
		this.gamma = gamma;
		this.ltp = ltp;
	}
	
	public OptionGreek(String tradingSymbol, float iv, float delta, float vega, float theta, float gamma, float ltp, float oi) {
		super();
		this.tradingSymbol = tradingSymbol;
		this.iv = iv;
		this.delta = delta;
		this.vega = vega;
		this.theta = theta;
		this.gamma = gamma;
		this.ltp = ltp;
		this.oi = oi;
	}
	
	public OptionGreek() {
		this.iv = 0;
		this.delta = 0;
		this.vega = 0;
		this.theta = 0;
		this.gamma = 0;
	}

	public String getTradingSymbol() {
		return tradingSymbol;
	}
	
	public void setTradingSymbol(String tradingSymbol) {
		this.tradingSymbol = tradingSymbol;
	}
	
	public float getIv() {
		return iv;
	}
	
	public void setIv(float iv) {
		this.iv= iv;
	}
	
	public float getDelta() {
		return delta;
	}
	
	public void setDelta(float delta) {
		this.delta = delta;
	}
	
	public float getVega() {
		return vega;
	}
	
	public void setVega(float vega) {
		this.vega = vega;
	}
	
	public float getTheta() {
		return theta;
	}
	
	public void setTheta(float theta) {
		this.theta = theta;
	}
	
	public float getGamma() {
		return gamma;
	}
	
	public void setGamma(float gamma) {
		this.gamma = gamma;
	}

	public float getUnderlyingValue() {
		return underlyingValue;
	}

	public void setUnderlyingValue(float underlyingValue) {
		this.underlyingValue = underlyingValue;
	}

	public float getLtp() {
		return ltp;
	}

	public void setLtp(float ltp) {
		this.ltp = ltp;
	}

	public float getOi() {
		return oi;
	}

	public void setOi(float oi) {
		this.oi = oi;
	}

	public float getChangeInIv() {
		return changeInIv;
	}

	public void setChangeInIv(float changeInIv) {
		this.changeInIv = changeInIv;
	}

	public int getStrike() {
		if (strike==0) {
			strike = KiteUtil.getStrike(this.tradingSymbol);
		}
		return strike;
	}

	public void setStrike(int strike) {
		this.strike = strike;
	}

	public long getVolumeTradedToday() {
		return volumeTradedToday;
	}

	public void setVolumeTradedToday(long volumeTradedToday) {
		this.volumeTradedToday = volumeTradedToday;
	}

	public float getTimevalue() {
		if (timevalue==0) {
			if (tradingSymbol.endsWith("CE")) {
				timevalue = getStrike()>underlyingValue?ltp: ltp - (underlyingValue - getStrike());
			} else {
				timevalue = underlyingValue>getStrike()?ltp: ltp - (getStrike() - underlyingValue);
			}
		}
		return timevalue;
	}

	public void setTimevalue(float timevalue) {
		this.timevalue = timevalue;
	}

	public Timestamp getQuoteTime() {
		return quoteTime;
	}

	public void setQuoteTime(Timestamp quoteTime) {
		this.quoteTime = quoteTime;
	}
	
	
}

