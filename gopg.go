package main

import (
	"bytes"
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/bmizerany/pq"
	"io/ioutil"
	"log"
	"path"
	"regexp"
	"strconv"
)

var (
	scriptsFolder string = "./scripts"
	cs            ConnectionString
	help          bool
)

const (
	versionInsert string = "INSERT INTO version(id, script) values($1, $2);"
)

type Upgrade struct {
	Id      int
	Script  string
	Content []byte
}

type ConnectionString struct {
	Host     string
	User     string
	Password string
	DBName   string
	Port     int
	Options  string
}

func (c ConnectionString) String() string {
	var buffer bytes.Buffer

	if c.Host != "" {
		buffer.WriteString(fmt.Sprintf("host=%v ", c.Host))
	}
	if c.User != "" {
		buffer.WriteString(fmt.Sprintf("user=%v ", c.User))
	}
	if c.Password != "" {
		buffer.WriteString(fmt.Sprintf("password=%v ", c.Password))
	}
	if c.DBName != "" {
		buffer.WriteString(fmt.Sprintf("dbname=%v ", c.DBName))
	}
	if c.Port != 0 {
		buffer.WriteString(fmt.Sprintf("port=%d ", c.Port))
	}
	if c.Options != "" {
		buffer.WriteString(c.Options)
	}
	return buffer.String()
}

func init() {
	flag.StringVar(&cs.DBName, "dbname", "postgres", "Name of the database to manage")
	flag.StringVar(&cs.Host, "host", "/var/run/postgresql", "Address of the DB host.")
	flag.StringVar(&cs.Options, "options", "sslmode=disable", "Extra options to pass to postgresql.")
	flag.StringVar(&cs.Password, "password", "", "Password to use when connecting to the database.")
	flag.StringVar(&cs.User, "user", "postgres", "User name to use when connecting to the database.")
	flag.IntVar(&cs.Port, "port", 5432, "The port the host is listening on.")
	flag.StringVar(&scriptsFolder, "path", "./", "The path containing the upgrade scripts.")
	flag.BoolVar(&help, "help", false, "Shows this help message.")
}

func main() {
	flag.Parse()
	if help {
		flag.Usage()
		return
	}
	init, ver := initDb(cs)
	if init {
		log.Print("Initialization complete.")
		log.Print("Database is at version ", ver)

		fc := make(chan Upgrade)
		sc := make(chan Upgrade)

		GetUpgradeScripts(scriptsFolder, ver, fc)
		ExecuteUpgradeScript(cs, fc, sc)

		for u := range sc {
			log.Print("Completed: ", u.Script)
			log.Print("Database is at version ", u.Id)
		}
	}
}

func ExecuteUpgradeScript(cs ConnectionString, r chan Upgrade, s chan Upgrade) {
	go func() {
		if db, ok := getDb(cs); ok {
			defer db.Close()

			for u := range r {
				log.Print("Running: ", u.Script)

				tx, err := db.Begin()

				ex := func(e error) bool {
					if e != nil {
						log.Print(e)
						log.Print("Upgrade failed on: ", u.Script)
						if tx != nil {
							tx.Rollback()
						}
						close(s)
						return true
					}
					return false
				}
				if ex(err) {
					return
				}

				if _, err = tx.Exec(versionInsert, u.Id, u.Script); ex(err) {
					return
				}

				if _, err = tx.Exec(string(u.Content)); ex(err) {
					return
				}

				tx.Commit()
				s <- u
			}
			close(s)
		}
	}()
}

func GetUpgradeScripts(dir string, ver int, s chan Upgrade) {
	go func() {
		ex := func(e error) bool {
			if e != nil {
				log.Print(e)
				close(s)
				return true
			}
			return false
		}

		if files, err := ioutil.ReadDir(dir); ex(err) {
			return
		} else {
			re := regexp.MustCompile("^\\d+")
			for _, f := range files {
				if m := re.FindString(f.Name()); m != "" {
					if v, err := strconv.Atoi(m); ex(err) {
						return
					} else if v > ver {
						log.Print("Preparing: ", f.Name())
						if b, err := ioutil.ReadFile(path.Join(dir, f.Name())); ex(err) {
							return
						} else {
							s <- Upgrade{Id: v, Script: f.Name(), Content: b}
						}
					}
				}
			}
			close(s)
		}
	}()
}

func initDb(cs ConnectionString) (bool, int) {
	log.Print("Initializing database...")
	if db, ok := getDb(cs); ok {
		defer db.Close()

		if err := db.Ping(); err != nil {
			if err.Error() == fmt.Sprintf("pq: database %q does not exist", cs.DBName) {
				css := ConnectionString{
					Host:     cs.Host,
					User:     cs.User,
					DBName:   "postgres",
					Port:     cs.Port,
					Options:  cs.Options,
					Password: cs.Password,
				}

				if createDb(css, cs.DBName) {
					log.Print("Database created.")
					return initDb(cs)
				}

				return false, 0
			} else {
				log.Print(err)
				return false, 0
			}
		} else {
			log.Print("Creating version table if not exists.")
			_, err := db.Exec(`CREATE TABLE IF NOT EXISTS Version(
				Id integer not null primary key,
				Script varchar(255),
				UpgradeDate timestamp not null default current_timestamp);`)
			if err != nil {
				log.Print(err)
				return false, 0
			}
			if v, err := getVersion(db); err != nil {
				log.Print(err)
				return false, 0
			} else {
				return true, v
			}
		}
	}
	return false, 0
}

func createDb(cs ConnectionString, dbname string) bool {
	log.Print("Creating database...")
	if db, ok := getDb(cs); ok {
		defer db.Close()

		if err := db.Ping(); err != nil {
			log.Print(err)
			return false
		}

		_, err := db.Exec(fmt.Sprintf("CREATE DATABASE %v", dbname))
		if err != nil {
			log.Print(err)
			return false
		}

		return true
	}
	return false
}

func getDb(cs ConnectionString) (*sql.DB, bool) {
	db, err := sql.Open("postgres", cs.String())
	if err != nil {
		log.Print(err)
		return nil, false
	}
	return db, true
}

func getVersion(db *sql.DB) (int, error) {
	r := db.QueryRow("SELECT COALESCE(MAX(id),0) FROM version;")
	var v int

	if err := r.Scan(&v); err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		} else {
			return 0, err
		}
	}
	return v, nil
}
