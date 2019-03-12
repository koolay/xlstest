package main

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"

	"context"

	"github.com/plandem/xlsx"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

var (
	dbURL = "root:dev@tcp(localhost:3306)/xlstest"
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

	var err error

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

	var sql = `
		insert into logs(appid, author, build_version, copyright, created_at, page_url, product_name, proto, user_id) values(
		:appid, :author, :build_version, :copyright, :created_at, :page_url, :product_name, :proto, :user_id)
	`

	for i := 0; i < 1000; i++ {
		go func() {
			wg.Add(1)
			for ii := 0; ii < 1000; ii++ {
				_, err = db.NamedExecContext(ctx, sql, logInfo)
				if err != nil {
					log.Fatal(err)
				}
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
	err = exporter(db)
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
