package com.datastax.refdata;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.utils.PropertyHelper;
import com.datastax.demo.utils.Timer;
import com.datastax.refdata.model.Dividend;
import com.datastax.refdata.model.HistoricData;

public class Main {
	
	private static Logger logger = LoggerFactory.getLogger(Main.class);
	
	private AtomicLong TOTAL_POINTS = new AtomicLong(0);
	
	public Main() {

		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");
		String noOfThreadsStr = PropertyHelper.getProperty("noOfThreads", "5");
		
		ReferenceDao dao = new ReferenceDao(contactPointsStr.split(","));
		
		int noOfThreads = Integer.parseInt(noOfThreadsStr);
		
		//Create shared queue 
		BlockingQueue<List<HistoricData>> queueHistoricData = new ArrayBlockingQueue<List<HistoricData>>(10);
		BlockingQueue<List<Dividend>> queueDividend = new ArrayBlockingQueue<List<Dividend>>(10);
		
		ExecutorService executor = Executors.newFixedThreadPool(noOfThreads);
		Timer timer = new Timer();
		timer.start();
		
		for (int i = 0; i < noOfThreads; i++) {
			executor.execute(new HistoricDataWriter(dao, queueHistoricData));
			executor.execute(new DividendWriter(dao, queueDividend));
		}
		
		DataLoader dataLoader = new DataLoader (queueHistoricData, queueDividend);
		dataLoader.startProcessingData();
		
		while(!queueHistoricData.isEmpty() && !queueDividend.isEmpty() ){
			logger.info("Messages left to send " + (queueHistoricData.size() + queueDividend.size()));
			
			sleep(1);
		}		
		timer.end();
		logger.info("Data Loading took " + timer.getTimeTakenSeconds() + " secs. Total Points " + dao.getTotalPoints() + " (" + (dao.getTotalPoints()/timer.getTimeTakenSeconds()) + " a sec)");
		
		System.exit(0);
	}
	
	class HistoricDataWriter implements Runnable {

		private ReferenceDao dao;
		private BlockingQueue<List<HistoricData>> queue;

		public HistoricDataWriter(ReferenceDao dao, BlockingQueue<List<HistoricData>> queue) {
			this.dao = dao;
			this.queue = queue;
		}

		@Override
		public void run() {			
			while(true){				
				List<HistoricData> list = queue.poll();
								
				if (list!=null){
					try {
						this.dao.insertHistoricData(list);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}				
			}				
		}
	}
	
	class DividendWriter implements Runnable {

		private ReferenceDao dao;
		private BlockingQueue<List<Dividend>> queue;

		public DividendWriter(ReferenceDao dao, BlockingQueue<List<Dividend>> queue) {
			this.dao = dao;
			this.queue = queue;	
		}

		@Override
		public void run() {			
			while(true){				
				List<Dividend> list = queue.poll();
				
				if (list!=null){
					this.dao.insertDividend(list);
				}				
			}				
		}
	}
	
	private void sleep(int seconds) {
		try {
			Thread.sleep(seconds * 1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new Main();
	}
}



