create keyspace if not exists datastax_referencedata_binary_demo WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1' };

use datastax_referencedata_binary_demo; 

CREATE TABLE if not exists historic_data ( 
	exchange text,
	symbol text,
	dates blob,
	ticks blob,
	PRIMARY KEY (exchange, symbol)
);

truncate historic_data;
