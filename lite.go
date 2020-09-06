package sqlite

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"

	sqlite3 "github.com/mattn/go-sqlite3"
	"github.com/paulstuart/dbutil"
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

	registry    = make(map[string]*sqlite3.SQLiteConn)
	initialized = make(map[string]struct{})
)

// Hook is an SQLite connection hook
type Hook func(*sqlite3.SQLiteConn) error

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

// FuncReg contains the fields necessary to register a custom Sqlite function
type FuncReg struct {
	Name string
	Impl interface{}
	Pure bool
}

// ipFuncs are functions to convert ipv4 to and from int32
var ipFuncs = []FuncReg{
	{"iptoa", toIPv4, true},
	{"atoip", fromIPv4, true},
}

// The only way to get access to the sqliteconn, which is needed to be able to generate
// a backup from the database while it is open. This is a less than satisfactory approach
// because there's no way to have multiple instances open associate the connection with the DSN
//
// Since our use case is to normally have one instance open this should be workable for now
func sqlInit(driverName, query string, hook Hook, funcs ...FuncReg) {
	imu.Lock()
	defer imu.Unlock()

	if _, ok := initialized[driverName]; ok {
		return
	}
	initialized[driverName] = struct{}{}

	drvr := &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			for _, fn := range funcs {
				if err := conn.RegisterFunc(fn.Name, fn.Impl, fn.Pure); err != nil {
					return fmt.Errorf("failed to register %q: %w", fn.Name, err)
				}
			}
			if filename, err := connFilename(conn); err == nil {
				register(filename, conn)
			} else {
				return fmt.Errorf("couldn't get filename for connection: %+v, error: %w", conn, err)
			}

			if query != "" {
				if _, err := conn.Exec(query, nil); err != nil {
					return fmt.Errorf("connection query failed: %s -- %w", query, err)
				}
			}

			if hook != nil {
				return hook(conn)
			}
			return nil
		},
	}
	sql.Register(driverName, drvr)
}

// Filename returns the filename of the DB
func Filename(db *sql.DB) string {
	var seq, name, file string
	_ = dbutil.Row(db, []interface{}{&seq, &name, &file}, "PRAGMA database_list")
	return file
}

// connFilename returns the filename of the connection
func connFilename(conn *sqlite3.SQLiteConn) (string, error) {
	var filename string
	fn := func(cols []string, row int, values []driver.Value) error {
		if len(values) < 3 {
			return fmt.Errorf("only got %d values", len(values))
		}
		if values[2] == nil {
			return fmt.Errorf("nil values")
		}
		filename = string(values[2].(string))
		return nil
	}
	return filename, connQuery(conn, fn, "PRAGMA database_list")
}

// Close cleans up the database before closing
func Close(db *sql.DB) {
	if _, err := db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		log.Printf("error executing WAL checkpoint: %v\n", err)
	}
	if err := db.Close(); err != nil {
		log.Printf("error closing database: %v\n", err)
	}
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

	if err = destDb.Ping(); err != nil {
		return err
	}

	from := registered(Filename(db))
	to := registered(Filename(destDb))
	bk, err := to.Backup("main", from, "main")
	if err != nil {
		return err
	}

	defer func() {
		berr := bk.Finish()
		if err != nil {
			err = berr
		}
	}()

	for {
		fmt.Fprintf(w, "pagecount: %d remaining: %d\n", bk.PageCount(), bk.Remaining())
		var done bool
		done, err = bk.Step(step)
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
		_ = row.Scan(&value)
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
	return dbutil.NewStreamer(db, q).Table(w, true, nil)
}

// Commands emulates the client reading a series of commands
func Commands(db *sql.DB, buffer string, echo bool, w io.Writer) error {
	if w == nil {
		w = os.Stdout
	}
	// strip comments
	clean := commentC.ReplaceAll([]byte(buffer), []byte{})
	clean = commentSQL.ReplaceAll(clean, []byte{})

	lines := strings.Split(string(clean), ";\n")
	multiline := "" // triggers are multiple lines
	trigger := false
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if 0 == len(line) {
			continue
		}
		switch {
		case strings.HasPrefix(line, ".echo "):
			echo, _ = strconv.ParseBool(line[6:])
			continue
		case strings.HasPrefix(line, ".read "):
			name := strings.TrimSpace(line[6:])
			if err := File(db, name, echo, w); err != nil {
				return fmt.Errorf("read file: %s, error: %w", name, err)
			}
			continue
		case strings.HasPrefix(line, ".print "):
			str := strings.TrimSpace(line[7:])
			str = strings.Trim(str, `"`)
			str = strings.Trim(str, "'")
			fmt.Fprintln(w, str)
			continue
		case strings.HasPrefix(line, ".tables"):
			if err := listTables(db, w); err != nil {
				return fmt.Errorf("table error: %w", err)
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
		if strings.Contains(line, ";") {
			continue
		}
		if echo {
			fmt.Println("CMD> ", multiline)
		}
		if startsWith(multiline, "SELECT") {
			if err := dbutil.NewStreamer(db, multiline).Table(w, false, nil); err != nil {
				return fmt.Errorf("SELECT QUERY: %s FILE: %s ERROR: %w", line, Filename(db), err)
			}
		} else if _, err := db.Exec(multiline); err != nil {
			return fmt.Errorf("EXEC QUERY: %s FILE: %s ERROR: %w", line, Filename(db), err)
		}
		multiline = ""
	}
	return nil
}

// connQuery executes a query on a driver connection
func connQuery(conn *sqlite3.SQLiteConn, fn func([]string, int, []driver.Value) error, query string, args ...driver.Value) error {
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

// Config represents the sqlite configuration options
type Config struct {
	fail   bool
	query  string
	driver string
	hook   Hook
	funcs  []FuncReg
}

type Optional func(*Config)

// FailIfMissing will cause open to fail if file does not already exist
func WithExists(fail bool) Optional {
	return func(c *Config) {
		c.fail = fail
	}
}

// WithQuery adds an sql query to execute for each new connection
func WithQuery(query string) Optional {
	return func(c *Config) {
		c.query = query
	}
}

// WithHook adds an sql query to execute for each new connection
func WithHook(hook Hook) Optional {
	return func(c *Config) {
		c.hook = hook
	}
}

//WithDriver sets the driver name used
func WithDriver(driver string) Optional {
	return func(c *Config) {
		c.driver = driver
	}
}

// Functions registers custom functions
func WithFunctions(functions ...FuncReg) Optional {
	return func(c *Config) {
		c.funcs = functions
	}
}

/*
// NewOptions returns an Options
func NewOptions(file string) *Options {
	return &Options{file: file, config: sqlConfig{driver: DefaultDriver}}
}
*/
// open returns a db handler for the given file
func open(file string, config *Config) (*sql.DB, error) {
	if config == nil {
		config = &Config{driver: DefaultDriver}
	}
	sqlInit(config.driver, config.query, config.hook, config.funcs...)
	if !strings.Contains(file, ":memory:") {
		filename := file
		filename = strings.TrimPrefix(filename, "file:")
		filename = strings.TrimPrefix(filename, "//")
		if i := strings.Index(filename, "?"); i > 0 {
			filename = filename[:i]
		}

		// create directory if necessary
		dirName := path.Dir(filename)
		if _, err := os.Stat(dirName); os.IsNotExist(err) {
			if err := os.Mkdir(dirName, 0777); err != nil {
				return nil, err
			}
		}

		if !config.fail {
			if _, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666); err != nil {
				return nil, fmt.Errorf("os file: %s, error: %w", file, err)
			}
		} else if _, err := os.Stat(filename); os.IsNotExist(err) {
			return nil, err
		}
	}
	db, err := sql.Open(config.driver, file)
	if err != nil {
		return db, fmt.Errorf("sql file: %s, error: %w", file, err)
	}
	return db, db.Ping()
}

// Open returns a db handler for the given file
func Open(file string, opts ...Optional) (*sql.DB, error) {
	config := new(Config)
	for _, opt := range opts {
		opt(config)
	}
	return open(file, config)
}

// Server provides marshaled writes to the sqlite database
type Server struct {
	db *sql.DB
	mu sync.RWMutex
}

// NewServer returns a server
func NewServer(db *sql.DB) *Server {
	return &Server{db: db}
}

// Exec executes a writeable statement
func (s *Server) Exec(query string, args ...interface{}) (last int64, affected int64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return dbutil.Exec(s.db, query, args...)
}

// Stream returns query results to the given function
func (s *Server) Stream(fn dbutil.StreamFunc, query string, args ...interface{}) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return dbutil.NewStreamer(s.db, query, args...).Stream(fn)
}
