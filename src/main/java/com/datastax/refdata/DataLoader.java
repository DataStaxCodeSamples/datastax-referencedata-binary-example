package com.datastax.refdata;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVReader;

import com.datastax.refdata.model.Dividend;
import com.datastax.refdata.model.HistoricData;

public class DataLoader {

	private static Logger logger = LoggerFactory.getLogger(DataLoader.class);
	private DateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");

	private static final CharSequence DAILY_PRICES = "daily_prices";
	private static final CharSequence DIVIDENDS = "dividends";

	private BlockingQueue<List<HistoricData>> queueHistoricData;
	private BlockingQueue<List<Dividend>> queueDividend;

	public DataLoader(BlockingQueue<List<HistoricData>> queueHistoricData, BlockingQueue<List<Dividend>> queueDividend) {
		this.queueHistoricData = queueHistoricData;
		this.queueDividend = queueDividend;
	}

	public void startProcessingData() {

		// Process all the files from the csv directory
		File cvsDir = new File(".", "src/main/resources/csv");

		File[] files = cvsDir.listFiles(new FileFilter() {
			public boolean accept(File file) {
				return file.isFile();
			}
		});

		for (File file : files) {

			try {
				if (file.getName().contains(DAILY_PRICES)) {
					this.processDailyPricesFile(file);
				} else if (file.getName().contains(DIVIDENDS)) {
					//this.processDividendsFile(file);
				}
			} catch (FileNotFoundException e) {
				logger.warn("Could not process file : " + file.getAbsolutePath(), e);
			} catch (IOException e) {
				logger.warn("Could not process file : " + file.getAbsolutePath(), e);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private void processDailyPricesFile(File file) throws IOException, InterruptedException {

		CSVReader reader = new CSVReader(new FileReader(file.getAbsolutePath()), CSVReader.DEFAULT_SEPARATOR,
				CSVReader.DEFAULT_QUOTE_CHARACTER, 1);
		String[] items = null;
		String lastSymbol = null;
		String exchange = null;

		List<HistoricData> list = new ArrayList<HistoricData>();

		while ((items = reader.readNext()) != null) {

			exchange = items[0].trim();
			String symbol = items[1].trim();

			// Flush after every new symbol
			if (!symbol.equalsIgnoreCase(lastSymbol) && lastSymbol != null) {
				logger.info("Flushing " + exchange + "-" + lastSymbol);

				this.queueHistoricData.put(new ArrayList<HistoricData>(list));
				list = new ArrayList<HistoricData>();
			}

			Date date;
			try {
				date = dateFormatter.parse(items[2]);
			} catch (ParseException e) {
				logger.warn("Could not parse date " + items[2] + " continuing");
				continue;
			}
			double open = Double.parseDouble(items[3]);
			double high = Double.parseDouble(items[4]);
			double low = Double.parseDouble(items[5]);
			double close = Double.parseDouble(items[6]);
			int volume = Integer.parseInt(items[7]);
			double adjClose = Double.parseDouble(items[8]);

			lastSymbol = symbol;

			HistoricData historicData = new HistoricData(exchange, symbol, date, open, high, low, close, volume,
					adjClose);
			list.add(historicData);
		}

		if (exchange != null && lastSymbol != null) {
			logger.info("Flushing " + exchange + "-" + lastSymbol);
			this.queueHistoricData.put(new ArrayList<HistoricData>(list));
			list = new ArrayList<HistoricData>();
		}
		reader.close();
	}

	private void processDividendsFile(File file) throws IOException, InterruptedException {

		CSVReader reader = new CSVReader(new FileReader(file.getAbsolutePath()), CSVReader.DEFAULT_SEPARATOR,
				CSVReader.DEFAULT_QUOTE_CHARACTER, 1);
		String[] items = null;
		String lastSymbol = null;
		String exchange = null;

		List<Dividend> list = new ArrayList<Dividend>();

		while ((items = reader.readNext()) != null) {

			exchange = items[0].trim();
			String symbol = items[1].trim();

			// Flush after every new symbol
			if (!symbol.equalsIgnoreCase(lastSymbol) && lastSymbol != null) {
				logger.info("Flushing Dividend " + exchange + "-" + lastSymbol);
				this.queueDividend.put(new ArrayList<Dividend>(list));
				list = new ArrayList<Dividend>();
			}

			Date date;
			try {
				date = dateFormatter.parse(items[2]);
			} catch (ParseException e) {
				logger.warn("Could not parse date " + items[2] + " continuing");
				continue;
			}
			double dividendValue = Double.parseDouble(items[3]);

			lastSymbol = symbol;

			Dividend dividend = new Dividend(exchange, symbol, date, dividendValue);
			list.add(dividend);
		}

		if (exchange != null && lastSymbol != null) {
			logger.info("Flushing Dividend " + exchange + "-" + lastSymbol);
			this.queueDividend.put(new ArrayList<Dividend>(list));
			list = new ArrayList<Dividend>();
		}
		reader.close();
	}
}
