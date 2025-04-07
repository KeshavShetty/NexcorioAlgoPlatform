package com.nexcorio.algo.dto;

/**
 * 
 * @author Keshav Shetty
 *
 */

public class MainInstruments {

	private Long id;
	
	private String name;
	
	private String shortName;
	
	private Long zerodhaInstrumentToken;
	
	private String exchange;
	
	private String instrumentType;
	
	private Integer lotSize;
	
	private Integer orderFreezingQuantity;
	
	private Integer expiryDay;
	
	private Integer gapBetweenStrikes;
	
	private Float straddleMargin;
	
	private int noOfFutureExpiryData;
	
	private int noOfOptionsExpiryData;
	
	private int noOfOptionsStrikePoints;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getShortName() {
		return shortName;
	}

	public void setShortName(String shortName) {
		this.shortName = shortName;
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

	public String getInstrumentType() {
		return instrumentType;
	}

	public void setInstrumentType(String instrumentType) {
		this.instrumentType = instrumentType;
	}

	public Integer getLotSize() {
		return lotSize;
	}

	public void setLotSize(Integer lotSize) {
		this.lotSize = lotSize;
	}

	public Integer getOrderFreezingQuantity() {
		return orderFreezingQuantity;
	}

	public void setOrderFreezingQuantity(Integer orderFreezingQuantity) {
		this.orderFreezingQuantity = orderFreezingQuantity;
	}

	public Integer getExpiryDay() {
		return expiryDay;
	}

	public void setExpiryDay(Integer expiryDay) {
		this.expiryDay = expiryDay;
	}

	public Integer getGapBetweenStrikes() {
		return gapBetweenStrikes;
	}

	public void setGapBetweenStrikes(Integer gapBetweenStrikes) {
		this.gapBetweenStrikes = gapBetweenStrikes;
	}

	public Float getStraddleMargin() {
		return straddleMargin;
	}

	public void setStraddleMargin(Float straddleMargin) {
		this.straddleMargin = straddleMargin;
	}

	public int getNoOfFutureExpiryData() {
		return noOfFutureExpiryData;
	}

	public void setNoOfFutureExpiryData(int noOfFutureExpiryData) {
		this.noOfFutureExpiryData = noOfFutureExpiryData;
	}

	public int getNoOfOptionsExpiryData() {
		return noOfOptionsExpiryData;
	}

	public void setNoOfOptionsExpiryData(int noOfOptionsExpiryData) {
		this.noOfOptionsExpiryData = noOfOptionsExpiryData;
	}

	public int getNoOfOptionsStrikePoints() {
		return noOfOptionsStrikePoints;
	}

	public void setNoOfOptionsStrikePoints(int noOfOptionsStrikePoints) {
		this.noOfOptionsStrikePoints = noOfOptionsStrikePoints;
	}

}
