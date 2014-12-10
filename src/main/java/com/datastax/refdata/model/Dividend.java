package com.datastax.refdata.model;

import java.util.Date;

public class Dividend {

	private String exchange;
	private String symbol;
	private Date date;
	private double dividend;
	
	public Dividend(String exchange, String symbol, Date date, double dividend) {
		super();
		this.exchange = exchange;
		this.symbol = symbol;
		this.date = date;
		this.dividend = dividend;
	}
	public String getExchange() {
		return exchange;
	}
	public String getSymbol() {
		return symbol;
	}
	public Date getDate() {
		return date;
	}
	public double getDividend() {
		return dividend;
	}
	@Override
	public String toString() {
		return "Dividend [exchange=" + exchange + ", symbol=" + symbol + ", date=" + date + ", dividend=" + dividend
				+ "]";
	}	
}
