// +build sqlite_trace trace

package sqlite

import (
	"database/sql"
	"fmt"
	"log"
	"testing"

	sqlite3 "github.com/mattn/go-sqlite3"
)

func init() {
	sql.Register("sqlite3_tracing",
		&sqlite3.SQLiteDriver{
			ConnectHook: TraceHook(nil),
		})
}

func TestTrace(t *testing.T) {
	db, err := sql.Open("sqlite3_tracing", ":memory:")
	if err != nil {
		fmt.Printf("Failed to open database: %#+v\n", err)
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}

	dbSetup(db)

	dbDoInsert(db)
	dbDoInsertPrepared(db)
	dbDoSelect(db)
	dbDoSelectPrepared(db)
}

// 'DDL' stands for "Data Definition Language":

// Note: "INTEGER PRIMARY KEY NOT NULL AUTOINCREMENT" causes the error
// 'near "AUTOINCREMENT": syntax error'; without "NOT NULL" it works.
const tableDDL = `CREATE TABLE t1 (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 note VARCHAR NOT NULL
)`

// 'DML' stands for "Data Manipulation Language":

const insertDML = "INSERT INTO t1 (note) VALUES (?)"
const selectDML = "SELECT id, note FROM t1 WHERE note LIKE ?"

const textPrefix = "bla-1234567890-"
const noteTextPattern = "%Prep%"

const nGenRows = 4 // Number of Rows to Generate (for *each* approach tested)

func dbSetup(db *sql.DB) {
	var err error

	_, err = db.Exec("DROP TABLE IF EXISTS t1")
	if err != nil {
		log.Panic(err)
	}
	_, err = db.Exec(tableDDL)
	if err != nil {
		log.Panic(err)
	}
}

func dbDoInsert(db *sql.DB) {
	const Descr = "DB-Exec"
	for i := 0; i < nGenRows; i++ {
		result, err := db.Exec(insertDML, textPrefix+Descr)
		if err != nil {
			log.Panic(err)
		}

		resultDoCheck(result, Descr, i)
	}
}

func dbDoInsertPrepared(db *sql.DB) {
	const Descr = "DB-Prepare"

	stmt, err := db.Prepare(insertDML)
	if err != nil {
		log.Panic(err)
	}
	defer stmt.Close()

	for i := 0; i < nGenRows; i++ {
		result, err := stmt.Exec(textPrefix + Descr)
		if err != nil {
			log.Panic(err)
		}

		resultDoCheck(result, Descr, i)
	}
}

func resultDoCheck(result sql.Result, callerDescr string, callIndex int) {
	lastID, err := result.LastInsertId()
	if err != nil {
		log.Panic(err)
	}
	nAffected, err := result.RowsAffected()
	if err != nil {
		log.Panic(err)
	}

	log.Printf("Exec result for %s (%d): ID = %d, affected = %d\n", callerDescr, callIndex, lastID, nAffected)
}

func dbDoSelect(db *sql.DB) {
	const Descr = "DB-Query"

	rows, err := db.Query(selectDML, noteTextPattern)
	if err != nil {
		log.Panic(err)
	}
	defer rows.Close()

	rowsDoFetch(rows, Descr)
}

func dbDoSelectPrepared(db *sql.DB) {
	const Descr = "DB-Prepare"

	stmt, err := db.Prepare(selectDML)
	if err != nil {
		log.Panic(err)
	}
	defer stmt.Close()

	rows, err := stmt.Query(noteTextPattern)
	if err != nil {
		log.Panic(err)
	}
	defer rows.Close()

	rowsDoFetch(rows, Descr)
}

func rowsDoFetch(rows *sql.Rows, callerDescr string) {
	var nRows int
	var id int64
	var note string

	for rows.Next() {
		err := rows.Scan(&id, &note)
		if err != nil {
			log.Panic(err)
		}
		log.Printf("Row for %s (%d): id=%d, note=%q\n",
			callerDescr, nRows, id, note)
		nRows++
	}
	if err := rows.Err(); err != nil {
		log.Panic(err)
	}
	log.Printf("Total %d rows for %s.\n", nRows, callerDescr)
}
