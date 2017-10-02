# SQLBatchInsert - A multiple SQL Insert Go library (with Insert IDs)

## Author
http://richard.warburton.it/

## Warning
This library is really new, things may change.

## Quick Start
See example_test.go for how to do a batch insert.

## Documentation
This library is written to allow me to do lots of MySQL inserts
within Google's AppEngine Standard's 60 second time limit.

http://godoc.org/github.com/krolaw/sqlbatchinsert

Returning correct Insert IDs relies on the following behaviours:

1. "If you insert multiple rows using a single INSERT statement,
	LAST_INSERT_ID() returns the value generated for the first inserted row
	only." - https://dev.mysql.com/doc/refman/5.6/en/information-functions.html#function_last-insert-id

2. "If the only statements executing are 'simple inserts' where the number
	of rows to be inserted is known ahead of time, there are no gaps in the
	numbers generated for a single statement, except for “mixed-mode inserts” - 
	https://dev.mysql.com/doc/refman/5.7/en/innodb-auto-increment-handling.html

So it should work reliably for MySQL's InnoDB engine, YMMV for others.

