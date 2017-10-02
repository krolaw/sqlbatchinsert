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
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const DefaultStmtMaxLength = 1 << 20 // 1MiB

var ErrInsertLargerThanMaxLength = errors.New("Single Insert larger than max length permitted.")

type Config struct {
	StmtMaxLength int // Uses DefaultStmtMaxLength if unset.
	Tx            *sql.Tx
	ReturnIds     bool // Whether insert ids need to be returned
}

// NewBatchInsert
func (c *Config) NewBatchInsert(table string, colNames []string) *BatchInsert {
	qStart := "INSERT INTO `" + table + "` (`" + strings.Join(colNames, "`,`") + "`) VALUES "
	qLength := c.StmtMaxLength
	if qLength == 0 {
		qLength = DefaultStmtMaxLength
	}
	b := make([]byte, 0, qLength)
	b = b[:len(qStart)]
	copy(b, qStart)
	var ids []int64
	if c.ReturnIds {
		ids = make([]int64, 0)
	}
	return &BatchInsert{
		qStartLen: len(qStart),
		tx:        c.Tx,
		query:     b,
		ids:       ids,
	}
}

type BatchInsert struct {
	qStartLen int
	tx        *sql.Tx
	query     []byte
	ids       []int64
}

type SQLField string // Unescaped field - for values that have already been escaped

// Insert buffers the record and if the record + buffer > StmtMaxSize
// pushes the buffer to the SQL server,
func (b *BatchInsert) Insert(ctx context.Context, values ...interface{}) error {
	buf := make([]byte, 0, len(b.query)-b.qStartLen)

	if len(b.query) != b.qStartLen {
		buf = append(buf, ',', '(')
	} else {
		buf = append(buf, '(')
	}

	for _, arg := range values {
		switch v := arg.(type) {
		case nil:
			buf = append(buf, "NULL"...)
		case SQLField:
			buf = append(buf, v...)
		case int:
			buf = strconv.AppendInt(buf, int64(v), 10)
		case int8:
			buf = strconv.AppendInt(buf, int64(v), 10)
		case int16:
			buf = strconv.AppendInt(buf, int64(v), 10)
		case int32:
			buf = strconv.AppendInt(buf, int64(v), 10)
		case int64:
			buf = strconv.AppendInt(buf, v, 10)
		case float32:
			buf = strconv.AppendFloat(buf, float64(v), 'g', -1, 64)
		case float64:
			buf = strconv.AppendFloat(buf, v, 'g', -1, 64)
		case bool:
			if v {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		// case time.Time: TODO?

		case []byte:
			if v == nil {
				buf = append(buf, "NULL"...)
			} else {
				// https://dev.mysql.com/doc/refman/5.7/en/mysql-real-escape-string.html
				// Strictly speaking, MySQL requires only that backslash and the quote character used to quote the string in the query be escaped
				buf = append(buf, '\'')
				buf = append(buf, bytes.Replace(bytes.Replace(v, []byte{'\''}, []byte{'\'', '\''}, -1), []byte{'\''}, []byte{'\\', '\''}, -1)...)
				buf = append(buf, '\'')
			}
		case string:
			buf = append(buf, '\'')
			buf = append(buf, bytes.Replace(bytes.Replace([]byte(v), []byte{'\''}, []byte{'\'', '\''}, -1), []byte{'\''}, []byte{'\\', '\''}, -1)...)
			buf = append(buf, '\'')
		default:
			return fmt.Errorf("%v (%T) not supported by sqlbatchinsert", v, v)
		}
		buf = append(buf, ',')
	}

	buf[len(buf)-1] = ')'

	if len(buf)+b.qStartLen > cap(b.query) {
		return ErrInsertLargerThanMaxLength
	}

	if len(b.query)+len(buf) > cap(b.query) {
		if err := b.flush(ctx); err != nil {
			return err
		}
	}

	b.query = append(b.query, buf...)
	//log.Infof(ctx, "Add: %s", buf)
	return nil
}

func (b *BatchInsert) flush(ctx context.Context) error {
	// log.Infof(ctx, "Flush: %s", b.query)
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
	b.query = b.query[:b.qStartLen]
	return nil
}

// Flush sends up any remaining inserts and if requested by the Config, the
// inserted record ids.
func (b *BatchInsert) Flush(ctx context.Context) ([]int64, error) {
	err := b.flush(ctx)
	if b.ids == nil {
		return nil, err
	}
	ids := b.ids
	b.ids = make([]int64, 0)
	return ids, err
}
