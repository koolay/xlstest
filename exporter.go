package main

import (
	"database/sql"
	"encoding/binary"
	"fmt"
	"log"
	"strings"
	"time"

	ee "github.com/eaciit/hoboexcel"
	"github.com/gofrs/uuid"
	"github.com/jmoiron/sqlx"
)

var nullValue = ""

type XLSExportor struct {
	SQL        string
	OutputPath string
	writer     Writer
}

type SQLColumnConverter interface {
	Convert(colType *sql.ColumnType, val interface{}) string
}

type MysqlColumnConverter struct {
}

func NewXLSExportor(sql, outputPath string, writer Writer) *XLSExportor {
	return &XLSExportor{
		SQL:        sql,
		OutputPath: outputPath,
		writer:     writer,
	}
}

func (sc MysqlColumnConverter) Convert(colType *sql.ColumnType, val interface{}) string {

	var result string
	if val == nil {
		return result
	}

	tn := colType.DatabaseTypeName()

	if tn == "CHAR" || tn == "NCHAR" || tn == "NVARCHAR" || tn == "NVARCHAR2" {
		result = strings.TrimRight(string(val.([]byte)), " ")
	} else if tn == "DATE" || tn == "DATETIME" || tn == "TIMESTAMP" {
		tm := val.(time.Time)
		zeroTm := time.Time{}
		if tm == zeroTm {
			result = ""
		} else {
			result = tm.Format("2006-01-02 15:04:05")
		}
	} else if tn == "BINARY" {
		result = fmt.Sprintf("%d", binary.BigEndian.Uint64(val.([]byte)))
	} else if tn == "MONEY" || tn == "DECIMAL" || tn == "FLOAT" || tn == "DOUBLE" {
		result = string(val.([]byte))
	} else if tn == "UNIQUEIDENTIFIER" {
		if uid, err := uuid.FromBytes(val.([]byte)); err == nil {
			result = uid.String()
		}
	} else if _, ok := colType.Nullable(); ok && val == nil {
		result = nullValue
	} else {
		result = string(val.([]byte))
	}
	return result
}

type Writer interface {
	NextRow() []string
}

type XLSWriter struct {
	curRow       int
	maxRow       int
	rows         *sqlx.Rows
	colConverter SQLColumnConverter
}

func NewXLSWriter(rows *sqlx.Rows, colConverter SQLColumnConverter, maxRow int) *XLSWriter {
	return &XLSWriter{
		maxRow:       maxRow,
		rows:         rows,
		colConverter: colConverter,
	}
}

func (exp *XLSExportor) Export() error {
	// fetcher := exp.writer. ExcelFetcher{Rows: rows, CurRow: 1, MaxRow: 1000000}
	ee.Export(exp.OutputPath, exp.writer)
	return nil
}

func (w *XLSWriter) NextRow() []string {
	if w.maxRow > 0 && w.curRow >= w.maxRow {
		return nil
	}

	if w.rows.Next() {
		w.curRow++

		columns, err := w.rows.Columns()
		if err != nil {
			log.Fatal(err)
		}
		colLength := len(columns)
		colTypes, err := w.rows.ColumnTypes()
		if err != nil {
			log.Fatal(err)
		}

		xlsVals := make([]string, colLength)
		dataRow, err := w.rows.SliceScan()
		if err != nil {
			log.Fatal(err)
		}

		for i := 0; i < colLength; i++ {
			colValue := dataRow[i]
			xlsVals[i] = w.colConverter.Convert(colTypes[i], colValue)
		}
		return xlsVals
	}
	return nil
}
