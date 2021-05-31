package et.debran.jBatchProcessing.model;

import java.math.BigDecimal;
import java.util.List;

import yahoofinance.histquotes.HistoricalQuote;

public class Company {
	private String id;
	private String name;
	private String symbol;
	private String status;
	private List<HistoricalQuote> stockHistory;
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	
	public String getSymbol() {
		return symbol;
	}
	public void setSymbol(String symbol) {
		this.symbol = symbol;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public List<HistoricalQuote> getStockHistory() {
		return stockHistory;
	}
	public void setStockHistory(List<HistoricalQuote> stockHistory) {
		this.stockHistory = stockHistory;
	}
	
}
