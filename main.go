package main

import (
	"fmt"
	"log"
	"time"

	"context"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

var (
	dbURL = "root:dev@tcp(localhost:3306)/xlstest"
)

type Log struct {
	ID           int64  `json:"id,omitempty" db:"id"`
	PageURL      string `json:"page_url,omitempty" db:"index_page_url"`
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

func main() {
	db, err := conn()
	if err != nil {
		log.Fatal(err)
	}
	logInfo := &Log{
		PageURL:      "http://bing.com",
		ProductName:  "I'd check to see what that library you're using requires",
		APPID:        "xlstest_1",
		Proto:        "xls",
		Copyright:    "xlstest",
		BuildVersion: "1.0.1",
		Author:       "steeven",
		UserID:       1,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for i := 0; i < 100; i++ {
		go func() {
			for ii := 0; ii < 100; ii++ {
				_, err = db.NamedExecContext(ctx, "insert into logs()", logInfo)
				if err != nil {
					log.Fatal(err)
				}
			}
		}()
	}

	fmt.Println("vim-go")
}
