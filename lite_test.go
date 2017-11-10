package sqlite

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"sync"
	"testing"

	sqlite3 "github.com/mattn/go-sqlite3"
	"github.com/paulstuart/dbutil"
)

const (
	badPath = "/path/does/not/exist/database.db"

	querySelect = "select id,name,kind,modified from structs"
	querySingle = "select id,name,kind,modified from structs limit 1"
	queryBad    = "c e n'est pas une sql query"
	queryCreate = `create table if not exists structs (
    id integer not null primary key,
    name text,
    kind int,
    data blob,
    modified   DATETIME DEFAULT CURRENT_TIMESTAMP
);`
	hammerTime = `
drop table if exists hammer;

create table hammer (
	id integer primary key,
	worker int,
	counter int,
	ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);

.tables

PRAGMA cache_size= 10485760;

PRAGMA journal_mode = WAL;

PRAGMA synchronous = FULL;

`
	hammerInsert = `insert into hammer (worker, counter) values (?,?)`
)

func hammer(t *testing.T, workers, count int) {
	db := getHammerDB(t, "file::memory:?cache=shared")
	hammerDb(t, db, workers, count)
	Close(db)
}

func hammerDb(t *testing.T, db *sql.DB, workers, count int) {
	var wg sync.WaitGroup
	queue := make(chan int, count)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(worker int) {
			t.Log("start worker:", worker)
			for cnt := range queue {
				if _, err := db.Exec(hammerInsert, worker, cnt); err != nil {
					t.Errorf("worker:%d count:%d, error:%s\n", worker, cnt, err.Error())
				}
			}
			wg.Done()
		}(i)
	}
	for i := 0; i < count; i++ {
		queue <- i
	}
	close(queue)
	wg.Wait()
}

func TestHammer(t *testing.T) {
	hammer(t, 8, 10000)
}

func getHammerDB(t *testing.T, name string) *sql.DB {
	if name == "" {
		name = "hammer.db"
	}
	db, err := Open(name)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprintln(testout, "TESTOUT")
	if err := Commands(db, hammerTime, false, testout); err != nil {
		t.Fatal(err)
	}
	return db
}

var (
	testFile = "test.db"
	testout  = ioutil.Discard
)

func init() {
	os.Remove(testFile)
	if testing.Verbose() {
		testout = os.Stdout
	}
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
	db.Exec(query, "abc", 23, "what ev er")
	db.Exec(query, "def", 69, "m'kay")
	db.Exec(query, "hij", 42, "meaning of life")
	db.Exec(query, "klm", 2, "of a kind")
}
func TestFuncs(t *testing.T) {
	db, err := Open(":memory:", ConfigFuncs(ipFuncs...), ConfigDriverName("funky"))
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

	if err := dbutil.Row(db, []interface{}{&ipv4}, "select iptoa(ip) as ipv4 from iptest where ipv4 = ?", testIP); err != nil {
		t.Fatal(err)
	}

	if ipv4 != testIP {
		t.Errorf("expected: %s but got: %s\n", testIP, ipv4)
	}

	var ip32 int32
	if err := dbutil.Row(db, []interface{}{&ip32}, "select atoip('8.8.8') as ipv4"); err != nil {
		t.Fatal(err)
	} else {
		if ip32 != -1 {
			t.Fatalf("expected: %d but got: %d\n", -1, ip32)
		}
	}
}

func TestSqliteBadHook(t *testing.T) {
	const badDriver = "badhook"
	_, err := Open(":memory:", ConfigDriverName(badDriver), ConfigHook(queryBad))

	if err == nil {
		t.Fatal("expected error for bad hook")
	} else {
		t.Logf("got expected error: %v\n", err)
	}
}

func simpleQuery(db *sql.DB) error {
	var one int
	dest := []interface{}{&one}
	if err := dbutil.Row(db, dest, "select 1", nil); err != nil {
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
	const hook = "select 1"
	sqlInit(driver, hook, badFuncs...)
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
	sqlInit(DefaultDriver, "")
	_, err := Open(badPath)
	if err == nil {
		t.Fatal("expected error for bad path")
	} else {
		t.Logf("got expected error: %v\n", err)
	}
}

func TestSqliteBadURI(t *testing.T) {
	sqlInit(DefaultDriver, "")
	_, err := Open("test.db ! % # mode ro bad=")
	if err == nil {
		t.Fatal("expected error for bad uri")
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
	if err := File(db, "test.sql", true, testout); err != nil {
		t.Fatal(err)
	}
	limit := 3
	var total int64
	dest := []interface{}{&total}
	if err := dbutil.Row(db, dest, "select total from summary where country=? limit ?", "USA", limit); err != nil {
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
	query := "select asdf xyz m'kay;"
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
			return ConnQuery(conn, fn, query)
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
			return ConnQuery(conn, fn, queryBad)
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
			return ConnQuery(conn, fn, querySelect)
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

func errLogger(t *testing.T) chan error {
	e := make(chan error, 4096)
	go func() {
		for err := range e {
			t.Error(err)
		}
	}()
	return e
}

func TestServerWrite(t *testing.T) {
	db := getHammerDB(t, "")
	r := make(chan ServerQuery, 4096)
	w := make(chan ServerAction, 4096)
	e := errLogger(t)
	go Server(db, r, w)
	batter(t, w, 10, 100000)
	close(r)
	close(w)
	close(e)
	Close(db)
}

func TestServerRead(t *testing.T) {
	db := fakeHammer(t, 10, 1000)
	r := make(chan ServerQuery, 4096)
	e := errLogger(t)
	go Server(db, r, nil)
	butter(t, r, 2, 10)
	close(r)
	close(e)
	Close(db)
}

func TestServerBadQuery(t *testing.T) {
	db := fakeHammer(t, 10, 1000)
	r := make(chan ServerQuery, 4096)
	go Server(db, r, nil)
	ec := make(chan error)
	r <- ServerQuery{
		Query: queryBad,
		Args:  nil,
		//Reply: nullStream,
		Error: ec,
	}
	close(r)
	err := <-ec
	if err == nil {
		t.Fatal("expected missing args error")
	} else {
		t.Log(err)
	}
	Close(db)
}

func batter(t *testing.T, w chan ServerAction, workers, count int) {

	var wg sync.WaitGroup

	response := func(affected, last int64, err error) {
		//	t.Logf("aff:%d last:%d err:%v\n", affected, last, err)
		wg.Done()
	}

	queue := make(chan int, 4096)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(worker int) {
			t.Logf("worker:%d\n", worker)
			for cnt := range queue {
				wg.Add(1)
				w <- ServerAction{
					Query:    hammerInsert,
					Args:     []interface{}{worker, cnt},
					Callback: response,
				}
			}
			wg.Done()
			t.Logf("done:%d\n", worker)
		}(i)
	}
	for i := 0; i < count; i++ {
		queue <- i
	}
	close(queue)
	wg.Wait()
	t.Log("battered")
}

func butter(t *testing.T, r chan ServerQuery, workers, count int) {

	limit := 100
	var wg sync.WaitGroup

	ec := make(chan error, count)
	var tally int
	replies := func(columns []string, row int, values []interface{}) error {
		if row == 0 {
			t.Logf("columns: %v\n", columns)
		}
		t.Logf("row:%d values:%v\n", row, values)
		tally++
		return nil
	}

	go func() {
		for err := range ec {
			if err != nil {
				t.Fatal(err)
			}
			wg.Done()
		}
	}()

	query := "select * from hammer limit ?"
	queue := make(chan int, 4096)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(worker int) {
			t.Logf("worker:%d\n", worker)
			for _ = range queue {
				wg.Add(1)
				r <- ServerQuery{
					Query: query,
					Args:  []interface{}{limit},
					Reply: replies,
					Error: ec,
				}
			}
			wg.Done()
			t.Logf("done:%d\n", worker)
		}(i)
	}
	for i := 0; i < count; i++ {
		queue <- i
	}
	close(queue)
	wg.Wait()
	limit *= count
	if tally != limit {
		t.Errorf("expected %d rows but got back %d\n", limit, tally)
	}
	t.Log("buttered")
}

func fakeHammer(t *testing.T, workers, count int) *sql.DB {
	db := getHammerDB(t, "")
	for i := 0; i < count; i++ {
		worker := rand.Int() % workers
		if _, err := db.Exec(hammerInsert, worker, i); err != nil {
			t.Fatalf("worker:%d count:%d, error:%s\n", worker, i, err.Error())
		}
	}
	return db
}

func cachedDB(t *testing.T) *sql.DB {
	db, err := Open("file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	return db
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

func TestSqliteDelete(t *testing.T) {
	db, _ := Open(testFile)
	cnt, err := dbutil.Update(db, "delete from foo where id=?", 13)
	if err != nil {
		t.Fatal("DELETE ERROR: ", err)
	}
	t.Log("DELETED: ", cnt)
	db.Close()
}

func TestSqliteInsert(t *testing.T) {
	db, _ := Open(testFile)
	cnt, err := dbutil.Update(db, "insert into foo (id,name) values(?,?)", 13, "bakers")
	if err != nil {
		t.Log("INSERT ERROR: ", err)
	}
	t.Log("INSERTED: ", cnt)
	db.Close()
}

func TestSqliteUpdate(t *testing.T) {
	db, _ := Open(testFile)
	cnt, err := dbutil.Update(db, "update foo set id=23 where id > ? and name like ?", "3", "bi%")
	if err != nil {
		t.Log("UPDATE ERROR: ", err)
	} else {
		t.Log("UPDATED: ", cnt)
	}
	db.Close()
}

func TestMissingDB(t *testing.T) {
	_, err := Open("this_path_does_not_exist", ConfigFailIfMissing(true))
	if err == nil {
		t.Error("should have had error for missing file")
	}
}
