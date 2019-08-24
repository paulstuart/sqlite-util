package sqlite

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"io/ioutil"
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
func sqlInit(name, hook string, funcs ...FuncReg) {
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
			if filename, err := connFilename(conn); err == nil {
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
			fmt.Fprintln(w, str)
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
		if startsWith(multiline, "SELECT") {
			if err := dbutil.NewStreamer(db, line).Table(w, false, nil); err != nil {
				return errors.Wrapf(err, "SELECT QUERY: %s FILE: %s", line, Filename(db))
			}
		} else if _, err := db.Exec(multiline); err != nil {
			return errors.Wrapf(err, "EXEC QUERY: %s FILE: %s", line, Filename(db))
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

// sqlConfig represents the sqlite configuration options
type sqlConfig struct {
	fail   bool
	hook   string
	driver string
	funcs  []FuncReg
}

type Opener struct {
	file   string
	config sqlConfig
}

// FailIfMissing will cause open to fail if file does not already exist
func (o *Opener) FailIfMissing(fail bool) *Opener {
	o.config.fail = fail
	return o
}

// Hook adds an sql query to execute for each new connection
func (o *Opener) Hook(hook string) *Opener {
	o.config.hook = hook
	return o
}

// Driver sets the driver name used
func (o *Opener) Driver(driver string) *Opener {
	o.config.driver = driver
	return o
}

// Functions registers custom functions
func (o *Opener) Functions(functions ...FuncReg) *Opener {
	o.config.funcs = functions
	return o
}

// Open returns a DB connection
func (o *Opener) Open() (*sql.DB, error) {
	return open(o.file, &o.config)
}

// NewOpener returns an Opener
func NewOpener(file string) *Opener {
	return &Opener{file: file, config: sqlConfig{driver: DefaultDriver}}
}

// open returns a db handler for the given file
func open(file string, config *sqlConfig) (*sql.DB, error) {
	if config == nil {
		config = &sqlConfig{driver: DefaultDriver}
	}
	sqlInit(config.driver, config.hook, config.funcs...)
	if strings.Index(file, ":memory:") < 0 {
		filename := file
		if strings.HasPrefix(filename, "file:") {
			filename = filename[5:]
		}
		if strings.HasPrefix(filename, "//") {
			filename = filename[2:]
		}
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

// Open returns a db handler for the given file
func Open(file string) (*sql.DB, error) {
	return open(file, nil)
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
