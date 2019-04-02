package main

import (
	"database/sql"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	"context"

	"github.com/plandem/xlsx"
	"github.com/satori/go.uuid"

	ee "github.com/eaciit/hoboexcel"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

var (
	dbURL     = "root:dev@tcp(localhost:3306)/xlstest?parseTime=true"
	filename  = "xlstest.xlsx"
	nullValue = ""
)

type Log struct {
	ID           int64  `json:"id,omitempty" db:"id"`
	PageURL      string `json:"page_url,omitempty" db:"page_url"`
	ProductName  string `json:"product_name,omitempty" db:"product_name"`
	APPID        string `json:"appid,omitempty" db:"appid"`
	Proto        string `json:"proto,omitempty" db:"proto"`
	Copyright    string `json:"copyright,omitempty" db:"copyright"`
	BuildVersion string `json:"build_version,omitempty" db:"build_version"`
	Author       string `json:"author,omitempty" db:"author"`
	UserID       int64  `json:"user_id,omitempty" db:"user_id"`
	CreatedAt    string `json:"created_at,omitempty" db:"created_at"`
	UpdatedAt    string `json:"updated_at,omitempty" db:"updated_at"`
}

func conn() (*sqlx.DB, error) {

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := sqlx.ConnectContext(ctx, "mysql", dbURL)
	if err != nil {
		return nil, err
	}
	conn.SetMaxOpenConns(10)
	conn.SetMaxIdleConns(0)
	return conn, err
}

type ExcelFetcher struct {
	CurRow int
	MaxRow int
	Rows   *sqlx.Rows
}

func (f *ExcelFetcher) NextRow() []string {

	if f.CurRow <= f.MaxRow {
		if f.Rows.Next() {

			f.CurRow++
			columns, err := f.Rows.Columns()
			if err != nil {
				log.Fatal(err)
			}
			colLength := len(columns)
			colTypes, err := f.Rows.ColumnTypes()
			if err != nil {
				log.Fatal(err)
			}

			xlsVals := make([]string, colLength)
			dataRow, err := f.Rows.SliceScan()
			if err != nil {
				log.Fatal(err)
			}

			for i := 0; i < colLength; i++ {
				colValue := dataRow[i]
				xlsVals[i] = f.StringColumnValue(colTypes[i], colValue)
			}

			return xlsVals
		}
	}
	return nil

}

func (f *ExcelFetcher) StringColumnValue(colType *sql.ColumnType, val interface{}) string {

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

func exporterLowMemory(db *sqlx.DB) error {
	sql := "select * from logs"
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Second)
	defer cancel()

	rows, err := db.QueryxContext(ctx, sql)
	if err != nil {
		return err
	}

	defer rows.Close()
	fetcher := ExcelFetcher{Rows: rows, CurRow: 1, MaxRow: 1000000}
	ee.Export(filename, &fetcher)
	return nil
}

func exporter(db *sqlx.DB) error {

	sql := "select * from logs"
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	result, err := db.QueryxContext(ctx, sql)
	if err != nil {
		return err
	}

	defer result.Close()

	var colLength = 11

	xl := xlsx.New()
	defer xl.Close()
	sheet := xl.AddSheet("logs")

	for colIndex := 0; colIndex < colLength; colIndex++ {
		sheet.InsertCol(colIndex)
	}

	var i int
	for result.Next() {
		if i > 500000 {
			break
		}
		cols, err := result.SliceScan()
		if err != nil {
			return err
		}

		row := sheet.InsertRow(i)
		for ci, colVal := range cols {
			row.Cell(ci).SetValue(colVal)
		}

		i += 1
	}

	if i > 0 {
		err = xl.SaveAs("./new_file.xlsx")
		if err != nil {
			return err
		}
	}

	return nil

}

func initData(db *sqlx.DB) error {

	logInfo := &Log{
		PageURL:      "http://bing.com?id=aaf_afsd_adsf",
		ProductName:  "I'd check to see what that library you're using requires",
		APPID:        "xlstest_1",
		Proto:        "xls",
		Copyright:    "xlstest",
		BuildVersion: "1.0.1",
		Author:       "steeven",
		UserID:       1,
		CreatedAt:    "2019-03-12 13:30:22",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3600*time.Second)
	defer cancel()

	wg := sync.WaitGroup{}
	st := time.Now()

	for i := 0; i < 10000; i++ {
		go func() {
			wg.Add(1)

			vals := []interface{}{}
			sql := "insert into logs(appid, author, build_version, copyright, created_at, page_url, product_name, proto, user_id) values"

			for i := 0; i < 20; i++ {
				sql += "(?,?,?,?,?,?,?,?,?),"
				vals = append(vals, logInfo.APPID, logInfo.Author, logInfo.BuildVersion, logInfo.Copyright, logInfo.CreatedAt,
					logInfo.PageURL, logInfo.ProductName, logInfo.Proto, logInfo.UserID)
			}

			sql = sql[0 : len(sql)-1]
			stmt, err := db.DB.Prepare(sql)
			if err != nil {
				log.Fatal(err)
			}

			_, err = stmt.ExecContext(ctx, vals...)
			if err != nil {
				log.Fatal(err)
			}

			defer wg.Done()
		}()
	}

	wg.Wait()
	fmt.Println(time.Since(st).Seconds())

	return nil
}

func main() {

	db, err := conn()
	if err != nil {
		log.Fatal(err)
	}

	// CPU Profile
	cpuProfile, err := os.Create("cpu.prof")
	if err != nil {
		log.Fatal(err)
	}
	pprof.StartCPUProfile(cpuProfile)
	defer pprof.StopCPUProfile()

	st := time.Now()
	fmt.Println("start export ..")
	err = exporterLowMemory(db)
	fmt.Println("total take: ", time.Now().Sub(st).Seconds())
	if err != nil {
		log.Fatal(err)
	}

	// Memory Profile
	runtime.GC()
	memProfile, err := os.Create("example-mem.prof")
	if err != nil {
		log.Fatal(err)
	}
	defer memProfile.Close()
	if err := pprof.WriteHeapProfile(memProfile); err != nil {
		log.Fatal(err)
	}

}
