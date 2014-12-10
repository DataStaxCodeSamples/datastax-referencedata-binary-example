package com.datastax.refdata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.utils.PropertyHelper;
import com.datastax.demo.utils.Timer;

public class Read {
	
	private static Logger logger = LoggerFactory.getLogger(Main.class);
	
	public Read() {

		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");
		String exchange = PropertyHelper.getProperty("exchange", "AMEX");
		String symbol = PropertyHelper.getProperty("symbol", "IF");
		
		ReferenceDao dao = new ReferenceDao(contactPointsStr.split(","));
		Timer timer = new Timer();
		timer.start();
		dao.printHistoricData(exchange, symbol);
		timer.end();
		logger.info("Data read took " + timer.getTimeTakenMillis() + " ms.");
		
		System.exit(0);
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new Read();
	}
}
