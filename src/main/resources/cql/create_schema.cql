create keyspace if not exists datastax_referencedata_binary_demo WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1' };

use datastax_referencedata_binary_demo; 

CREATE TABLE if not exists historic_data ( 
	exchange text,
	symbol text,
	dates blob,
	closes blob,
	opens blob,
	highs blob,
	lows blob,
	adj_closes blob,
	volumes blob,
	PRIMARY KEY (exchange, symbol)
);

CREATE TABLE if not exists dividends ( 
	exchange text,
	symbol text,
	date timestamp,
	dividend double,
	PRIMARY KEY ((exchange, symbol), date)
) WITH CLUSTERING ORDER BY (date DESC);

CREATE TABLE if not exists exchange_metadata(
	exchange text,
	symbol text,
	last_updated_date timestamp,
	PRIMARY KEY (exchange, symbol)
);

truncate historic_data;
truncate dividends;
truncate exchange_metadata;

