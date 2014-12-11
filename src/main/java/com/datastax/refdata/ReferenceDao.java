package com.datastax.refdata;

import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.nio.LongBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.refdata.model.Dividend;
import com.datastax.refdata.model.HistoricData;

public class ReferenceDao {

	private static Logger logger = LoggerFactory.getLogger(ReferenceDao.class);
	private AtomicLong TOTAL_POINTS = new AtomicLong(0);
	private Session session;
	private static String keyspaceName = "datastax_referencedata_binary_demo";
	private static String tableNameHistoric = keyspaceName + ".historic_data";
	private static String tableNameDividends = keyspaceName + ".dividends";
	private static String tableNameMetaData = keyspaceName + ".exchange_metadata";

	private static final String INSERT_INTO_HISTORIC = "Insert into " + tableNameHistoric
			+ " (exchange,symbol,dates,closes,opens,highs,lows,adj_closes,volumes) values (?,?,?,?,?,?,?,?,?);";

	private static final String INSERT_INTO_DIVIDENDS = "Insert into " + tableNameDividends
			+ " (exchange,symbol,date,dividend) values (?,?,?,?);";
	private static final String INSERT_INTO_METADATA = "Insert into " + tableNameMetaData
			+ " (exchange,symbol,last_updated_date) values (?,?,?);";

	private PreparedStatement insertStmtHistoric;
	private PreparedStatement insertStmtDividend;
	private PreparedStatement insertStmtMetaData;

	public ReferenceDao(String[] contactPoints) {

		Cluster cluster = Cluster.builder().addContactPoints(contactPoints).build();
		this.session = cluster.connect();

		this.insertStmtHistoric = session.prepare(INSERT_INTO_HISTORIC);
		this.insertStmtDividend = session.prepare(INSERT_INTO_DIVIDENDS);
		this.insertStmtMetaData = session.prepare(INSERT_INTO_METADATA);
		
		this.insertStmtHistoric.setConsistencyLevel(ConsistencyLevel.ONE);
		this.insertStmtDividend.setConsistencyLevel(ConsistencyLevel.ONE);
		this.insertStmtMetaData.setConsistencyLevel(ConsistencyLevel.ONE);
	}

	
	@SuppressWarnings("unused")
	public void printHistoricData (String exchange, String symbol){
		ResultSet result = session.execute("select * from " + tableNameHistoric + " where exchange = ? and symbol = ?", exchange, symbol);		
		Row row = result.one();
		
		int counter = 0;
		
		LongBuffer dates = row.getBytes("dates").asLongBuffer();		
		DoubleBuffer closes = row.getBytes("closes").asDoubleBuffer();
		DoubleBuffer highs = row.getBytes("highs").asDoubleBuffer();
		DoubleBuffer lows = row.getBytes("lows").asDoubleBuffer();
		DoubleBuffer opens = row.getBytes("opens").asDoubleBuffer();
		DoubleBuffer volumes = row.getBytes("volumes").asDoubleBuffer();
		DoubleBuffer adjCloses = row.getBytes("adj_closes").asDoubleBuffer();
		
		
		while (dates.hasRemaining()){
			Date date = new Date(dates.get()); 
			Double d = closes.get();
			counter++;
		}
				
		logger.info ("Finished " + counter);
	} 
	
	public void insertHistoricData(List<HistoricData> list) throws Exception{
		BoundStatement boundStmt = new BoundStatement(this.insertStmtHistoric);
		List<ResultSetFuture> results = new ArrayList<ResultSetFuture>();

		HistoricData mostRecent = null;

		ByteBuffer dates = ByteBuffer.allocate(list.size()*8);
		ByteBuffer closes = ByteBuffer.allocate(list.size()*8);
		ByteBuffer opens = ByteBuffer.allocate(list.size()*8);
		ByteBuffer highs = ByteBuffer.allocate(list.size()*8);
		ByteBuffer lows = ByteBuffer.allocate(list.size()*8);
		ByteBuffer adjCloses = ByteBuffer.allocate(list.size()*8);
		ByteBuffer volumes = ByteBuffer.allocate(list.size()*8);
		
		for (HistoricData historicData : list) {
			mostRecent=historicData;
			
			dates.putLong(historicData.getDate().getTime());
			closes.putDouble(historicData.getClose());
			opens.putDouble(historicData.getOpen());
			highs.putDouble(historicData.getHigh());
			lows.putDouble(historicData.getLow());
			adjCloses.putDouble(historicData.getAdjClose());
			volumes.putDouble(historicData.getVolume());
			
			TOTAL_POINTS.incrementAndGet();
		}
		
		logger.info ("dates-" + dates.position() + " prices-" + closes.position());
		session.execute(boundStmt.bind(mostRecent.getExchange(), mostRecent.getSymbol(), dates.flip(), closes.flip(),
				opens.flip(), highs.flip(), lows.flip(), adjCloses.flip(), volumes.flip()));
		
		//Wait till we have everything back.
		boolean wait = true;
		while (wait) {
			// start with getting out, if any results are not done, wait is
			// true.
			wait = false;
			for (ResultSetFuture result : results) {
				if (!result.isDone()) {
					wait = true;
					break;
				}
			}
		}
		return;
	}
	
	public void insertDividend(List<Dividend> list) {
		BoundStatement boundStmt = new BoundStatement(this.insertStmtDividend);
		List<ResultSetFuture> results = new ArrayList<ResultSetFuture>();

		for (Dividend dividend: list) {

			boundStmt.setString("exchange", dividend.getExchange());
			boundStmt.setString("symbol", dividend.getSymbol());
			boundStmt.setDate("date", dividend.getDate());
			boundStmt.setDouble("dividend", dividend.getDividend());

			results.add(session.executeAsync(boundStmt));
		}

		//Wait till we have everything back.
		boolean wait = true;
		while (wait) {
			wait = false;
			for (ResultSetFuture result : results) {
				if (!result.isDone()) {
					wait = true;
					break;
				}
			}
		}
		return;
	}
	
	public long getTotalPoints(){
		return TOTAL_POINTS.get();
	}
}
