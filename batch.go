// SQL Batch Insert Library which can return insert ids
//
// Author: http://richard.warburton.it/
//
// Copyright: 2017 Skagerrak Software - http://www.skagerraksoftware.com/ - See LICENSE file
//
// Returning correct Insert IDs relies on the following behaviours:
//
// 1. "If you insert multiple rows using a single INSERT statement,
// LAST_INSERT_ID() returns the value generated for the first inserted row
// only." - https://dev.mysql.com/doc/refman/5.6/en/information-functions.html#function_last-insert-id
//
// 2. "If the only statements executing are 'simple inserts' where the number
// of rows to be inserted is known ahead of time, there are no gaps in the
// numbers generated for a single statement, except for “mixed-mode inserts”
// - https://dev.mysql.com/doc/refman/5.7/en/innodb-auto-increment-handling.html
//
// So it should work reliably for MySQL's InnoDB engine, YMMV for others.
package sqlbatchinsert

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

const DefaultStmtMaxLength = 1 << 20 // 1MiB

var ErrInsertLargerThanMaxLength = errors.New("Single Insert larger than max length permitted.")

type dbMode int

const (
	MySQL dbMode = iota
	PostgreSQL
)

type Config struct {
	StmtMaxLength int // Uses DefaultStmtMaxLength if unset.
	Tx            *sql.Tx
	// Whether insert ids need to be returned, mysql "true", postgresql field name to return
	DB              dbMode // default mysql
	QuoteColumnsOff bool
	mutex           sync.Mutex
}

// NewBatchInsert
func (c *Config) NewBatchInsert(table string, colNames []string, returnId string) *BatchInsert {
	var ticks, qEnd string

	switch c.DB {
	case MySQL:
		if !c.QuoteColumnsOff {
			ticks = "`"
		}
	case PostgreSQL:
		if !c.QuoteColumnsOff {
			ticks = "\""
		}
		if returnId != "" {
			qEnd = " RETURNING " + returnId // TODO quotes and escaping
		}
	default:
		panic("Invalid DB")
	}

	qStart := "INSERT INTO " + ticks + table + ticks + " (" + ticks + strings.Join(colNames, ticks+","+ticks) + ticks + ") VALUES " // TODO escaping

	qLength := c.StmtMaxLength
	if qLength == 0 {
		qLength = DefaultStmtMaxLength
	}
	b := make([]byte, 0, 2*len(qStart))
	b = b[:len(qStart)]
	copy(b, qStart)
	var ids []int64
	if returnId != "" {
		ids = make([]int64, 0)
	}
	return &BatchInsert{
		qStartLen: len(qStart),
		qEnd:      []byte(qEnd),
		qLen:      qLength,
		tx:        c.Tx,
		query:     b,
		ids:       ids,
		dbMode:    c.DB,
		mutex:     &c.mutex,
	}
}

type BatchInsert struct {
	qStartLen int
	qEnd      []byte
	qLen      int
	tx        *sql.Tx
	query     []byte
	ids       []int64
	dbMode    dbMode
	buf       []byte
	mutex     *sync.Mutex
}

type SQLField string // Unescaped field - for values that have already been escaped

// Insert buffers the record and if the record + buffer > StmtMaxSize
// pushes the buffer to the SQL server,
func (b *BatchInsert) Insert(ctx context.Context, values ...interface{}) error {
	//buf := make([]byte, 0, cap(b.query)-b.qStartLen-len(b.qEnd))
	b.buf = b.buf[:0]

	if len(b.query) != b.qStartLen {
		b.buf = append(b.buf, ',', '(')
	} else {
		b.buf = append(b.buf, '(')
	}

	for _, arg := range values {
		myArg := arg
	tryAgain:
		switch v := myArg.(type) {
		case nil:
			b.buf = append(b.buf, "NULL"...)
		case SQLField:
			b.buf = append(b.buf, v...)
		case int:
			b.buf = strconv.AppendInt(b.buf, int64(v), 10)
		case int8:
			b.buf = strconv.AppendInt(b.buf, int64(v), 10)
		case int16:
			b.buf = strconv.AppendInt(b.buf, int64(v), 10)
		case int32:
			b.buf = strconv.AppendInt(b.buf, int64(v), 10)
		case int64:
			b.buf = strconv.AppendInt(b.buf, v, 10)

		case uint:
			b.buf = strconv.AppendInt(b.buf, int64(v), 10)
		case uint8:
			b.buf = strconv.AppendInt(b.buf, int64(v), 10)
		case uint16:
			b.buf = strconv.AppendInt(b.buf, int64(v), 10)
		case uint32:
			b.buf = strconv.AppendInt(b.buf, int64(v), 10)
		case uint64:
			b.buf = strconv.AppendInt(b.buf, int64(v), 10) // Dangerous?

		case float32:
			b.buf = strconv.AppendFloat(b.buf, float64(v), 'g', -1, 64)
		case float64:
			b.buf = strconv.AppendFloat(b.buf, v, 'g', -1, 64)
		case bool:
			if b.dbMode == MySQL {
				if v {
					b.buf = append(b.buf, '1')
				} else {
					b.buf = append(b.buf, '0')
				}
			} else {
				if v {
					b.buf = append(b.buf, []byte{'t', 'r', 'u', 'e'}...)
				} else {
					b.buf = append(b.buf, []byte{'f', 'a', 'l', 's', 'e'}...)
				}
			}

		case time.Time:
			b.buf = append(b.buf, []byte(v.Format("'2006-01-02 15:04:05.000000'"))...)

		case []byte:
			if v == nil {
				b.buf = append(b.buf, "NULL"...)
			} else {
				if b.dbMode == MySQL {
					// https://dev.mysql.com/doc/refman/5.7/en/mysql-real-escape-string.html
					// Strictly speaking, MySQL requires only that backslash and the quote character used to quote the string in the query be escaped
					b.buf = append(b.buf, '\'')
					b.buf = append(b.buf, bytes.Replace(bytes.Replace(v, []byte{'\''}, []byte{'\'', '\''}, -1), []byte{'\''}, []byte{'\\', '\''}, -1)...)
					b.buf = append(b.buf, '\'')
				} else {
					data := make([]byte, hex.EncodedLen(len(v))+6)
					copy(data, []byte{'E', '\'', '\\', '\\', 'x'})
					hex.Encode(data[5:], v)
					data[len(data)-1] = '\''

					b.buf = append(b.buf, data...)
				}
			}
		case string:
			b.buf = append(b.buf, '\'')
			if b.dbMode == MySQL {
				b.buf = append(b.buf, bytes.Replace(bytes.Replace([]byte(v), []byte{'\''}, []byte{'\'', '\''}, -1), []byte{'\''}, []byte{'\\', '\''}, -1)...)
			} else {
				b.buf = append(b.buf, []byte(strings.Replace(v, "'", "''", -1))...)
			}
			b.buf = append(b.buf, '\'')
		case sql.NullBool:
			if v.Valid {
				myArg = v.Bool
				goto tryAgain
			}
			b.buf = append(b.buf, "NULL"...)

		case sql.NullInt64:
			if v.Valid {
				myArg = v.Int64
				goto tryAgain
			}
			b.buf = append(b.buf, "NULL"...)

		case sql.NullString:
			if v.Valid {
				myArg = v.String
				goto tryAgain
			}
			b.buf = append(b.buf, "NULL"...)

		case sql.NullFloat64:
			if v.Valid {
				myArg = v.Float64
				goto tryAgain
			}
			b.buf = append(b.buf, "NULL"...)

		default:
			return fmt.Errorf("%v (%T) not supported by sqlbatchinsert", v, v)
		}
		b.buf = append(b.buf, ',')
	}

	b.buf[len(b.buf)-1] = ')'

	if len(b.query)+len(b.buf)+len(b.qEnd) > b.qLen {
		if len(b.buf)+b.qStartLen+len(b.qEnd) > b.qLen {
			return ErrInsertLargerThanMaxLength
		}
		if err := b.flush(ctx); err != nil {
			return err
		}
		b.query = append(b.query, b.buf[1:]...) // strip off lead comma
		return nil
	}

	b.query = append(b.query, b.buf...)
	//log.Infof(ctx, "Add: %s", buf)
	return nil
}

func (b *BatchInsert) flush(ctx context.Context) error {

	// log.Infof(ctx, "Flush: %s", b.query)
	b.query = append(b.query, b.qEnd...)

	//fmt.Printf("%s\n", b.query)

	b.mutex.Lock()
	defer b.mutex.Unlock()

	if len(b.qEnd) != 0 { // PostgreSQL
		res, err := b.tx.QueryContext(ctx, string(b.query))
		if err != nil {
			fmt.Printf("%s\n", b.query)
			return err
		}
		defer res.Close()

		for res.Next() {
			var id int64
			if err := res.Scan(&id); err != nil {
				return err
			}
			b.ids = append(b.ids, id)
		}

	} else { // MySQL
		res, err := b.tx.ExecContext(ctx, string(b.query))
		if err != nil {
			return err
		}
		if b.ids != nil {

			lid, err := res.LastInsertId()
			if err != nil {
				return err
			}
			count, err := res.RowsAffected()
			if err != nil {
				return err
			}
			for i := int64(0); i < count; i++ {
				b.ids = append(b.ids, lid+i)
			}
		}
	}
	b.query = b.query[:b.qStartLen]
	return nil
}

var ErrNoRowsInserted = errors.New("No rows inserted")

// Flush sends up any remaining inserts and if requested by the Config, the
// inserted record ids.
func (b *BatchInsert) Flush(ctx context.Context) ([]int64, error) {
	if len(b.query) == b.qStartLen {
		return nil, ErrNoRowsInserted
	}
	err := b.flush(ctx)
	if b.ids == nil {
		return nil, err
	}
	ids := b.ids
	b.ids = make([]int64, 0)
	return ids, err
}
