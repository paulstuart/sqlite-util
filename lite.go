package sqlite

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"

	sqlite3 "github.com/mattn/go-sqlite3"
	"github.com/paulstuart/dbutil"
	"github.com/pkg/errors"
)

var (
	rmu, imu sync.Mutex
)

// N/A, impacts db, or multi-column -- ignore for now
//collation_list
//database_list
//foreign_key_check
//foreign_key_list
//quick_check
//wal_checkpoint

const (
	// DefaultDriver is the default driver name to be registered
	DefaultDriver = "sqlite"

	pragmaList = `
	application_id
	auto_vacuum
	automatic_index
	busy_timeout
	cache_size
	cache_spill
	cell_size_check
	checkpoint_fullfsync
	compile_options
	data_version
	defer_foreign_keys
	encoding
	foreign_keys
	freelist_count
	fullfsync
	journal_mode
	journal_size_limit
	legacy_file_format
	locking_mode
	max_page_count
	mmap_size
	page_count
	page_size
	query_only
	read_uncommitted
	recursive_triggers
	reverse_unordered_selects
	schema_version
	secure_delete
	soft_heap_limit
	synchronous
	temp_store
	threads
	user_version
	wal_autocheckpoint
	`
)

var (
	pragmas    = strings.Fields(pragmaList)
	commentC   = regexp.MustCompile(`(?s)/\*.*?\*/`)
	commentSQL = regexp.MustCompile(`\s*--.*`)
	readline   = regexp.MustCompile(`(\.[a-z]+( .*)*)`)

	registry    = make(map[string]*sqlite3.SQLiteConn)
	initialized = make(map[string]struct{})
)

func register(file string, conn *sqlite3.SQLiteConn) {
	file, _ = filepath.Abs(file)
	if len(file) > 0 {
		rmu.Lock()
		registry[file] = conn
		rmu.Unlock()
	}
}

func registered(file string) *sqlite3.SQLiteConn {
	rmu.Lock()
	conn := registry[file]
	rmu.Unlock()
	return conn
}

func toIPv4(ip int64) string {
	a := (ip >> 24) & 0xFF
	b := (ip >> 16) & 0xFF
	c := (ip >> 8) & 0xFF
	d := ip & 0xFF

	return fmt.Sprintf("%d.%d.%d.%d", a, b, c, d)
}

func fromIPv4(ip string) int64 {
	octets := strings.Split(ip, ".")
	if len(octets) != 4 {
		return -1
	}
	a, _ := strconv.ParseInt(octets[0], 10, 64)
	b, _ := strconv.ParseInt(octets[1], 10, 64)
	c, _ := strconv.ParseInt(octets[2], 10, 64)
	d, _ := strconv.ParseInt(octets[3], 10, 64)
	return (a << 24) + (b << 16) + (c << 8) + d
}

// SqliteFuncReg contains the fields necessary to register a custom Sqlite function
type SqliteFuncReg struct {
	Name string
	Impl interface{}
	Pure bool
}

// ipFuncs are functions to convert ipv4 to and from int32
var ipFuncs = []SqliteFuncReg{
	{"iptoa", toIPv4, true},
	{"atoip", fromIPv4, true},
}

// The only way to get access to the sqliteconn, which is needed to be able to generate
// a backup from the database while it is open. This is a less than satisfactory approach
// because there's no way to have multiple instances open associate the connection with the DSN
//
// Since our use case is to normally have one instance open this should be workable for now
func sqlInit(name, hook string, funcs ...SqliteFuncReg) {
	imu.Lock()
	defer imu.Unlock()

	if _, ok := initialized[name]; ok {
		return
	}
	initialized[name] = struct{}{}

	drvr := &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			for _, fn := range funcs {
				if err := conn.RegisterFunc(fn.Name, fn.Impl, fn.Pure); err != nil {
					return err
				}
			}
			if filename, err := ConnFilename(conn); err == nil {
				register(filename, conn)
			} else {
				return errors.Wrapf(err, "couldn't get filename for connection: %+v", conn)
			}

			if len(hook) > 0 {
				if _, err := conn.Exec(hook, nil); err != nil {
					return errors.Wrapf(err, "connection hook failed: %s", hook)
				}
			}

			return nil
		},
	}
	sql.Register(name, drvr)
}

// Filename returns the filename of the DB
func Filename(db *sql.DB) string {
	var seq, name, file string
	dbutil.Row(db, []interface{}{&seq, &name, &file}, "PRAGMA database_list")
	return file
}

// ConnFilename returns the filename of the connection
func ConnFilename(conn *sqlite3.SQLiteConn) (string, error) {
	var filename string
	fn := func(cols []string, row int, values []driver.Value) error {
		if len(values) < 3 {
			return fmt.Errorf("only got %d values", len(values))
		}
		if values[2] == nil {
			return fmt.Errorf("nil values")
		}
		filename = string(values[2].([]uint8))
		return nil
	}
	err := ConnQuery(conn, fn, "PRAGMA database_list")
	return filename, err
}

// Close cleans up the database before closing
func Close(db *sql.DB) {
	db.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
	db.Close()
}

// Backup backs up the open database
func Backup(db *sql.DB, dest string) error {
	return backup(db, dest, 1024, ioutil.Discard)
}

func backup(db *sql.DB, dest string, step int, w io.Writer) error {
	os.Remove(dest)

	destDb, err := Open(dest)
	if err != nil {
		return err
	}
	defer destDb.Close()
	err = destDb.Ping()

	fromDB := Filename(db)
	toDB := Filename(destDb)

	from := registered(fromDB)
	to := registered(toDB)

	bk, err := to.Backup("main", from, "main")
	if err != nil {
		return err
	}

	defer bk.Finish()
	for {
		fmt.Fprintf(w, "pagecount: %d remaining: %d\n", bk.PageCount(), bk.Remaining())
		done, err := bk.Step(step)
		if done || err != nil {
			break
		}
	}
	return err
}

// Pragmas lists all relevant Sqlite pragmas
func Pragmas(db *sql.DB, w io.Writer) {
	for _, pragma := range pragmas {
		row := db.QueryRow("PRAGMA " + pragma)
		var value string
		row.Scan(&value)
		fmt.Fprintf(w, "pragma %s = %s\n", pragma, value)
	}
}

// File emulates ".read FILENAME"
func File(db *sql.DB, file string, echo bool, w io.Writer) error {
	out, err := ioutil.ReadFile(file)
	if err != nil {
		return err
	}
	return Commands(db, string(out), echo, w)
}

func startsWith(data, sub string) bool {
	return strings.HasPrefix(strings.ToUpper(strings.TrimSpace(data)), strings.ToUpper(sub))
}

func listTables(db *sql.DB, w io.Writer) error {
	q := `
SELECT name FROM sqlite_master
WHERE type='table'
ORDER BY name
`
	return dbutil.PrintTable(db, w, true, q)
}

// Commands emulates the client reading a series of commands
// TODO: is this available in the C api?
func Commands(db *sql.DB, buffer string, echo bool, w io.Writer) error {
	if w == nil {
		w = os.Stdout
	}
	// strip comments
	clean := commentC.ReplaceAll([]byte(buffer), []byte{})
	clean = commentSQL.ReplaceAll(clean, []byte{})

	// .read, et al gets a fake ';' to split on
	//clean = readline.ReplaceAll(clean, []byte("${1};"))

	//lines := strings.Split(string(clean), ";")
	lines := strings.Split(string(clean), "\n")
	multiline := "" // triggers are multiple lines
	trigger := false
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if 0 == len(line) {
			continue
		}
		if echo {
			fmt.Println("CMD>", line)
		}
		switch {
		case strings.HasPrefix(line, ".echo "):
			echo, _ = strconv.ParseBool(line[6:])
			continue
		case strings.HasPrefix(line, ".read "):
			name := strings.TrimSpace(line[6:])
			if err := File(db, name, echo, w); err != nil {
				return errors.Wrapf(err, "read file: %s", name)
			}
			continue
		case strings.HasPrefix(line, ".print "):
			str := strings.TrimSpace(line[7:])
			str = strings.Trim(str, `"`)
			str = strings.Trim(str, "'")
			fmt.Println(str)
			continue
		case strings.HasPrefix(line, ".tables"):
			if err := listTables(db, w); err != nil {
				return errors.Wrapf(err, "table error")
			}
			continue
		case startsWith(line, "CREATE TRIGGER"):
			multiline = line
			trigger = true
			continue
		case startsWith(line, "END;"):
			line = multiline + "\n" + line
			multiline = ""
			trigger = false
		case trigger:
			multiline += "\n" + line // restore our 'split' transaction
			continue
		}
		if len(multiline) > 0 {
			multiline += "\n" + line // restore our 'split' transaction
		} else {
			multiline = line
		}
		if strings.Index(line, ";") < 0 {
			continue
		}
		if _, err := db.Exec(multiline); err != nil {
			return errors.Wrapf(err, "EXEC QUERY: %s FILE: %s", line, Filename(db))
		}
		multiline = ""
	}
	return nil
}

// ConnQuery executes a query on a driver connection
func ConnQuery(conn *sqlite3.SQLiteConn, fn func([]string, int, []driver.Value) error, query string, args ...driver.Value) error {
	rows, err := conn.Query(query, args)
	if err != nil {
		return err
	}
	defer rows.Close()

	cols := rows.Columns()
	cnt := 0
	for {
		buffer := make([]driver.Value, len(cols))
		if err = rows.Next(buffer); err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}
		if err = fn(cols, cnt, buffer); err != nil {
			break
		}
		cnt++
	}
	return err
}

// DataVersion returns the version number of the schema
func DataVersion(db *sql.DB) (int64, error) {
	var version int64
	return version, dbutil.Row(db, []interface{}{&version}, "PRAGMA data_version")
}

// Version returns the version of the sqlite library used
// libVersion string, libVersionNumber int, sourceID string) {
func Version() (string, int, string) {
	return sqlite3.Version()
}

// SQLConfig represents the sqlite configuration options
type SQLConfig struct {
	failIfMissing bool
	hook          string
	driver        string
	funcs         []SqliteFuncReg
}

// ConfigFunc processes an SQLConfig
type ConfigFunc func(*SQLConfig)

// ConfigDriverName specifies the driver name to use
func ConfigDriverName(name string) ConfigFunc {
	return func(c *SQLConfig) {
		c.driver = name
	}
}

//ConfigFailIfMissing requires the database to exist before opening
func ConfigFailIfMissing(fail bool) ConfigFunc {
	return func(c *SQLConfig) {
		c.failIfMissing = fail
	}
}

// ConfigHook specifies the connection hook query to run
func ConfigHook(hook string) ConfigFunc {
	return func(c *SQLConfig) {
		c.hook = hook
	}
}

// ConfigFuncs specifies the sqlite functions to register
func ConfigFuncs(funcs ...SqliteFuncReg) ConfigFunc {
	return func(c *SQLConfig) {
		c.funcs = funcs
	}
}

// Open returns a db struct for the given file
func Open(file string, opts ...ConfigFunc) (*sql.DB, error) {
	config := &SQLConfig{driver: DefaultDriver}
	for _, opt := range opts {
		opt(config)
	}
	sqlInit(config.driver, config.hook, config.funcs...)
	if strings.Index(file, ":memory:") < 0 {
		full, err := url.Parse(file)
		if err != nil {
			return nil, errors.Wrapf(err, "parse file: %s", file)
		}
		filename := full.Path
		os.Mkdir(path.Dir(filename), 0777)
		if !config.failIfMissing {
			if _, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666); err != nil {
				return nil, errors.Wrapf(err, "os file: %s", file)
			}
		} else if _, err := os.Stat(filename); os.IsNotExist(err) {
			return nil, err
		}
	}
	db, err := sql.Open(config.driver, file)
	if err != nil {
		return db, errors.Wrapf(err, "sql file: %s", file)
	}
	return db, db.Ping()
}

// ServerAction represents an async write request to database
type ServerAction struct {
	Query    string
	Args     []interface{}
	Callback func(int64, int64, error)
}

// ServerQuery represents an async read request to database
type ServerQuery struct {
	Query string
	Args  []interface{}
	Reply dbutil.RowFunc
	Error chan error
}

// Server provides serialized access to the database
func Server(db *sql.DB, r chan ServerQuery, w chan ServerAction) {
	var wg sync.WaitGroup
	wg.Add(1)
	s := dbutil.NewStreamer(db)
	go func() {
		for q := range r {
			err := s.Stream(q.Reply, q.Query, q.Args...)

			if q.Error != nil {
				// use goroutine so we don't block on sending errors
				go func() {
					if err != nil {
						err = errors.Wrapf(err, "stream error")
					}
					q.Error <- err
				}()
			}
		}
		wg.Done()
	}()
	wg.Add(1)
	go func() {
		for q := range w {
			q.Callback(dbutil.Exec(db, q.Query, q.Args...))
		}
		wg.Done()
	}()
	wg.Wait()
}
