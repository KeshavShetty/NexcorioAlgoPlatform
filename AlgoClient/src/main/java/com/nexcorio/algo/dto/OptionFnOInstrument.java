package com.nexcorio.algo.dto;

import java.util.Date;

/**
 * 
 * @author Keshav Shetty
 *
 */

public class OptionFnOInstrument {

	private Long id;
	
	private String tradingSymbol;
	
	private Long fMainInstrument;
	
	private Long zerodhaInstrumentToken;
	
	private String exchange;
	
	private Integer strike;
	
	private Date expiryDate;

	public OptionFnOInstrument(Long id, String tradingSymbol, Long fMainInstrument, Long zerodhaInstrumentToken,
			String exchange, Integer strike, Date expiryDate) {
		super();
		this.id = id;
		this.tradingSymbol = tradingSymbol;
		this.fMainInstrument = fMainInstrument;
		this.zerodhaInstrumentToken = zerodhaInstrumentToken;
		this.exchange = exchange;
		this.strike = strike;
		this.expiryDate = expiryDate;
	}
	
	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getTradingSymbol() {
		return tradingSymbol;
	}

	public void setTradingSymbol(String tradingSymbol) {
		this.tradingSymbol = tradingSymbol;
	}

	public Long getfMainInstrument() {
		return fMainInstrument;
	}

	public void setfMainInstrument(Long fMainInstrument) {
		this.fMainInstrument = fMainInstrument;
	}

	public Long getZerodhaInstrumentToken() {
		return zerodhaInstrumentToken;
	}

	public void setZerodhaInstrumentToken(Long zerodhaInstrumentToken) {
		this.zerodhaInstrumentToken = zerodhaInstrumentToken;
	}

	public String getExchange() {
		return exchange;
	}

	public void setExchange(String exchange) {
		this.exchange = exchange;
	}

	public Integer getStrike() {
		return strike;
	}

	public void setStrike(Integer strike) {
		this.strike = strike;
	}

	public Date getExpiryDate() {
		return expiryDate;
	}

	public void setExpiryDate(Date expiryDate) {
		this.expiryDate = expiryDate;
	}
	
	

}
