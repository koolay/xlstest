package main

import (
	"time"
	"log"
	"fmt"
	"strings"
	"encoding/binary"
	"database/sql"

	"github.com/gofrs/uuid"
	"github.com/jmoiron/sqlx"
	ee "github.com/eaciit/hoboexcel"
)

type XLSExportor struct {
	SQL string
	OutputPath string
	MaxRows  int
	writer Writer
}

type SQLColumnConverter interface {
	Convert(colType *sql.ColumnType, val interface{}) string 
}

type MysqlColumnConverter struct {
}

func NewXLSExportor(sql, outputPath string, maxRows int, writer Writer) *XLSExportor {
	return &XLSExportor{
		SQL: sql,
		OutputPath: outputPath,
		MaxRows: maxRows,
		writer: writer,
	}
}

func Convert(colType *sql.ColumnType, val interface{}) string {
	
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
		// result = store.NullValue
	} else {
		result = string(val.([]byte))
	}
	return result
}

type Writer interface {
	NextRow() []string
}

type XLSWriter struct {
	curRow int
	maxRow int
	Rows   *sqlx.Rows
	colConverter SQLColumnConverter
}

func (exp XLSExportor) Export()  error {
	// fetcher := exp.writer. ExcelFetcher{Rows: rows, CurRow: 1, MaxRow: 1000000}
	ee.Export(exp.OutputPath, exp.writer)
	return nil
}

func (w XLSWriter) NextRow() []string {
	
	if w.curRow <= w.maxRow {
		w.curRow++
		if w.Rows.Next() {

			columns, err := w.Rows.Columns()
			if err != nil {
				log.Fatal(err)
			}
			colLength := len(columns)
			colTypes, err := w.Rows.ColumnTypes()
			if err != nil {
				log.Fatal(err)
			}

			xlsVals := make([]string, colLength)
			dataRow, err := w.Rows.SliceScan()
			if err != nil {
				log.Fatal(err)
			}

			for i := 0; i < colLength; i++ {
				colValue := dataRow[i]
				xlsVals[i] = w.colConverter.Convert(colTypes[i], colValue)
			}
			return xlsVals
		}
	}
	return nil
}
