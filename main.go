package main

import (
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
)

// RFC3339Plus is time.RFC3339 but with a + sign instead of Z.
const RFC3339Plus = "2006-01-02T15:04:05+07:00"

var (
	csvFolder = flag.String("csv-folder", "./csv", "Folder to the CSV files")
	// database values
	dbUsername = flag.String("db-username", "suicord", "DB username")
	dbHost     = flag.String("db-host", "localhost", "DB host")
	dbPassword = flag.String("db-password", "suicord", "DB password")
	dbName     = flag.String("db-name", "suicord", "DB name")
)

// Exits on error.
func checkErr(err error, params ...interface{}) {
	if err != nil {
		log.Fatalf("%+v", errors.Wrap(err, fmt.Sprint(params...)))
	}
}

func parseCsv(file string) (header []string, rows [][]string) {
	f, err := os.Open(file)
	checkErr(err, file)
	defer f.Close()

	reader := csv.NewReader(f)
	reader.FieldsPerRecord = -1

	header, err = reader.Read()
	checkErr(err, file)
	rawRows, err := reader.ReadAll()
	checkErr(err, file)
	for _, row := range rawRows {
		if len(row) != len(header) {
			log.Printf("> reading csv: row %v missing rows, skipping", row)
			continue
		}
		rows = append(rows, row)
	}
	return header, rows
}

func createTable(db *sql.DB, tableName string, headers []string, rows [][]string) {
	if len(rows) == 0 {
		log.Printf("> no rows, cannot figure out table structure. skipping")
		return
	}
	row := rows[0]
	var columns []string
	var colTypes []string
	for i, header := range headers {
		col := row[i]
		colType := "text" // default to text
		// If we can parse as int, make it a bigint
		if _, err := strconv.Atoi(col); err == nil && !strings.HasPrefix(header, "pct" /*percentile*/) {
			colType = "bigint"
		} else if _, err := strconv.ParseFloat(col, 64); err == nil {
			colType = "double"
		} else if _, err := time.Parse(RFC3339Plus, col); err == nil {
			colType = "datetime"
		}
		columns = append(columns, header+" "+colType+" not null")
		colTypes = append(colTypes, colType)
	}
	// Drop old table
	if _, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)); err != nil {
		checkErr(err)
	}
	// Create table command!
	cols := strings.Join(columns, ", ")
	cmd := fmt.Sprintf(`CREATE TABLE %s (id bigint not null auto_increment primary key, %s) CHARACTER SET utf8`, tableName, cols)
	if _, err := db.Exec(cmd); err != nil {
		checkErr(err, tableName, cmd)
	}
	log.Printf("> table %s created (%s)", tableName, cols)

	// Fill table with data
	for _, row := range rows {
		var args []interface{}
		var marks []string
		for i, col := range row {
			marks = append(marks, "?")
			switch colTypes[i] {
			case "text":
				args = append(args, col)
			case "bigint":
				num, err := strconv.Atoi(col)
				checkErr(err, row, col)
				args = append(args, num)
			case "double":
				num, err := strconv.ParseFloat(col, 64)
				checkErr(err, row, col)
				args = append(args, num)
			case "datetime":
				num, err := time.Parse(RFC3339Plus, col)
				checkErr(err, row, col)
				args = append(args, num)
			}
		}
		_, err := db.Exec(fmt.Sprintf(`INSERT INTO %s(%s) VALUES (%s)`, tableName, strings.Join(headers, ", "), strings.Join(marks, ", ")), args...)
		checkErr(err, tableName, row, args)
	}
	log.Printf("> %d rows inserted", len(rows))
}

func main() {
	flag.Parse()

	// Build database config
	cfg := mysql.NewConfig()
	cfg.User = *dbUsername
	cfg.Passwd = *dbPassword
	cfg.Net = "tcp"
	cfg.Addr = *dbHost
	cfg.DBName = *dbName
	cfg.Collation = "utf8_unicode_ci"

	// Connect to db
	db, err := sql.Open("mysql", cfg.FormatDSN())
	checkErr(err)
	defer db.Close()
	log.Println("Connected to db")

	// Parse all csv files
	files, err := ioutil.ReadDir(*csvFolder)
	checkErr(err)
	for _, file := range files {
		filename := file.Name()
		if !strings.HasSuffix(filename, ".csv") {
			continue
		}
		tableName := filename[:len(filename)-len(".csv")]
		tableName = strings.ReplaceAll(tableName, "-", "_")
		path := filepath.Join(*csvFolder, filename)

		log.Println("Handling", tableName)
		headers, rows := parseCsv(path)
		createTable(db, tableName, headers, rows)
		log.Println("Done", tableName)
	}
}
