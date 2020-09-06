package sqlite

import (
	"database/sql"
	"database/sql/driver"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	sqlite3 "github.com/mattn/go-sqlite3"
)

const (
	badPath = "/path/does/not/exist/database.db"

	querySelect = "select id,name,kind,modified from structs"
	queryBad    = "c e n'est pas une sql query"
	queryCreate = `create table if not exists structs (
    id integer not null primary key,
    name text,
    kind int,
    data blob,
    modified   DATETIME DEFAULT CURRENT_TIMESTAMP
);`
)

var (
	testFile     = "test.db"
	testout      = ioutil.Discard
	echoCommands = false
)

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Verbose() {
		testout = os.Stdout
		echoCommands = true
	}
	defer os.Remove(testFile)
	os.Exit(m.Run())
}

func memDB(t *testing.T) *sql.DB {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func structDb(t *testing.T) *sql.DB {
	db := memDB(t)
	prepare(db)
	return db
}

func prepare(db *sql.DB) {
	if _, err := db.Exec(queryCreate); err != nil {
		panic(err)
	}
	const query = "insert into structs(name, kind, data) values(?,?,?)"
	_, _ = db.Exec(query, "abc", 23, "what ev er")
	_, _ = db.Exec(query, "def", 69, "m'kay")
	_, _ = db.Exec(query, "hij", 42, "meaning of life")
	_, _ = db.Exec(query, "klm", 2, "of a kind")
}

func TestFuncs(t *testing.T) {
	//db, err := NewOptions(":memory:").Functions(ipFuncs...).Driver("funky").Open()
	db, err := Open(":memory:", WithFunctions(ipFuncs...), WithDriver("funky"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const create = `create table iptest ( ip int )`
	const ins = `
insert into iptest values(atoip('127.0.0.1'));
insert into iptest values(atoip('192.168.1.1'));
`
	if _, err = db.Exec(create); err != nil {
		t.Fatalf("%q: %s\n", err, create)
	}

	if _, err = db.Exec(ins); err != nil {
		t.Fatalf("%q: %s\n", err, ins)
	}

	const testIP = "192.168.1.1"
	var ipv4 string

	if err := row(db, []interface{}{&ipv4}, "select iptoa(ip) as ipv4 from iptest where ipv4 = ?", testIP); err != nil {
		t.Fatal(err)
	}

	if ipv4 != testIP {
		t.Errorf("expected: %s but got: %s\n", testIP, ipv4)
	}

	var ip32 int32
	if err := row(db, []interface{}{&ip32}, "select atoip('8.8.8') as ipv4"); err != nil {
		t.Fatal(err)
	} else {
		if ip32 != -1 {
			t.Fatalf("expected: %d but got: %d\n", -1, ip32)
		}
	}
}

func TestSqliteBadHook(t *testing.T) {
	const badDriver = "badhook"
	_, err := Open(":memory:", WithDriver(badDriver), WithQuery(queryBad))

	if err == nil {
		t.Fatal("expected error for bad hook")
	} else {
		t.Logf("got expected error: %v\n", err)
	}
}

func simpleQuery(db *sql.DB) error {
	var one int
	dest := []interface{}{&one}
	if err := row(db, dest, "select 1", nil); err != nil {
		return err
	}
	if one != 1 {
		return fmt.Errorf("expected: %d but got %d", 1, one)
	}
	return nil
}

type unknownStruct struct{}

func TestSqliteFuncsBad(t *testing.T) {
	u := &unknownStruct{}
	badFuncs := []FuncReg{
		{"", u, true},
	}
	const driver = "badfunc"
	const query = "select 1"
	sqlInit(driver, query, nil, badFuncs...)
	db, err := sql.Open(driver, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	if err := simpleQuery(db); err == nil {
		t.Fatal("expected error for bad func")
	} else {
		t.Logf("got expected error: %v\n", err)
	}
}

func TestSqliteBadPath(t *testing.T) {
	sqlInit(DefaultDriver, "", nil)
	_, err := Open(badPath)
	if err == nil {
		t.Fatal("expected error for bad path")
	} else {
		t.Logf("got expected error: %v\n", err)
	}
}

func TestVersion(t *testing.T) {
	_, i, _ := Version()
	if i < 3017000 {
		t.Errorf("old version: %d\n", i)
	} else {
		t.Log(i)
	}
}

func TestBackup(t *testing.T) {
	db, err := Open(testFile)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	prepare(db)
	if err := Backup(db, "test_backup.db"); err != nil {
		t.Fatal(err)
	}
}

func TestBackupBadDir(t *testing.T) {
	db, err := Open(testFile)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	prepare(db)
	if err := backup(db, "/this/path/does/not/exist/test_backup.db", 1024, testout); err == nil {
		t.Fatal("expected backup error")
	} else {
		t.Log(err)
	}
}

func TestFile(t *testing.T) {
	db := memDB(t)
	if err := os.Chdir("sql"); err != nil {
		t.Fatal(err)
	}
	fmt.Fprintf(testout, "V is for: %t\n", testing.Verbose())
	if err := File(db, "test.sql", testing.Verbose(), testout); err != nil {
		t.Fatal(err)
	}
	limit := 3
	var total int64
	dest := []interface{}{&total}
	if err := row(db, dest, "select total from summary where country=? limit ?", "USA", limit); err != nil {
		t.Fatal(err)
	}
	if total != 3 {
		t.Fatalf("expected count of: %d but got %d\n", limit, total)
	}
	db.Close()
}

func TestFileDoesNotExit(t *testing.T) {
	db := memDB(t)
	if err := File(db, "this_file_does_not_exist.sql", testing.Verbose(), testout); err == nil {
		t.Fatal("expected error for missing file")
	} else {
		t.Log(err)
	}
}

func TestFileReadMissing(t *testing.T) {
	db := memDB(t)
	if err := File(db, "sql/test3.sql", testing.Verbose(), testout); err == nil {
		t.Fatal("expected error for missing file")
	} else {
		t.Log(err)
	}
}

func TestFileBadExec(t *testing.T) {
	db := memDB(t)
	if err := File(db, "sql/test4.sql", testing.Verbose(), testout); err == nil {
		t.Fatal("expected error for invalid sql")
	} else {
		t.Log(err)
	}
}

func TestPragmas(t *testing.T) {
	db := memDB(t)
	Pragmas(db, testout)
}

func TestCommandsBadQuery(t *testing.T) {
	db := memDB(t)
	query := "select asdf xyz m'kay;\n"
	if err := Commands(db, query, false, nil); err == nil {
		t.Fatal("expected error for bad query")
	} else {
		t.Log(err)
	}
}

func TestCommandsReadMissingFile(t *testing.T) {
	db := memDB(t)
	cmd := `.read /this/file/does/not/exist.sql`
	if err := Commands(db, cmd, false, nil); err == nil {
		t.Fatal("expected error for reading command file")
	} else {
		t.Log(err)
	}
}

func TestCommandsTrigger(t *testing.T) {
	db := structDb(t)
	const (
		query1 = `create table if not exists inserted (id integer, msg text)`
		query2 = `
CREATE TRIGGER structs_insert AFTER INSERT ON structs 
BEGIN
    insert or replace into inserted (id) values(NEW.id);
    insert or replace into inserted (msg) values('ack!');
END;
`
	)
	if _, err := db.Exec(query1); err != nil {
		t.Fatal(err)
	}
	if err := Commands(db, query2, testing.Verbose(), nil); err != nil {
		t.Fatal(err)
	}
}

func TestDataVersion(t *testing.T) {
	db := structDb(t)

	i, err := DataVersion(db)
	if err != nil {
		t.Fatal(err)
	}
	if i < 1 {
		t.Fatalf("expected version to be greater than zero but instead is: %d\n", i)
	}
}

func TestConnQueryOk(t *testing.T) {
	name := "connQuery01"
	query := "select 23;"

	fn := func(columns []string, row int, values []driver.Value) error {
		return nil
	}
	drvr := &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			return connQuery(conn, fn, query)
		},
	}
	sql.Register(name, drvr)
	_, err := sql.Open(name, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
}

func TestConnQueryBad(t *testing.T) {
	name := "connQuery02"
	fn := func(columns []string, row int, values []driver.Value) error {
		return nil
	}
	drvr := &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			return connQuery(conn, fn, queryBad)
		},
	}
	sql.Register(name, drvr)
	db, _ := sql.Open(name, ":memory:")
	_, err := db.Query(querySelect)
	if err == nil {
		t.Fatal("expected error but got none")
	} else {
		t.Log("got expected error:", err)
	}
}

func TestConnQueryFuncBad(t *testing.T) {
	file := "test.db"
	os.Remove(file)
	db, err := Open(file)
	if err != nil {
		t.Fatal(err)
	}
	prepare(db)
	Close(db)

	name := "connQuery03"
	fn := func(columns []string, row int, values []driver.Value) error {
		return fmt.Errorf("function had an error")
	}
	drvr := &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			return connQuery(conn, fn, querySelect)
		},
	}
	sql.Register(name, drvr)
	db, _ = sql.Open(name, file)

	if _, err = db.Query(querySelect); err == nil {
		t.Fatal("expected error but got none")
	} else {
		t.Log("got expected error:", err)
	}
}

func TestOpenBadFile(t *testing.T) {
	if _, err := Open("/path/does/:mem not/ory: exist/:memory:/abc123"); err == nil {
		t.Fatal("expected error but got none")
	} else {
		t.Log("got expected error:", err)
	}
}

func TestSqliteCreate(t *testing.T) {
	db, err := Open(testFile)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	sql := `
	create table foo (id integer not null primary key, name text);
	delete from foo;
	`
	_, err = db.Exec(sql)
	if err != nil {
		t.Fatalf("%q: %s\n", err, sql)
	}

	_, err = db.Exec("insert into foo(id, name) values(1, 'foo'), (2, 'bar'), (3, 'baz')")
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("select id, name from foo")
	if err != nil {
		t.Fatal(err)
	}

	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatal(err)
		}
		t.Log(id, name)
	}
	rows.Close()
}

func TestMissingDB(t *testing.T) {
	_, err := Open("this_path_does_not_exist", WithExists(true))
	if err == nil {
		t.Error("should have had error for missing file")
	}
}
