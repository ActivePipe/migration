package migration

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
)

type Logger interface {
	Printf(format string, v ...interface{})
}

var Log Logger = log.New(os.Stdout, "", log.LstdFlags)

type Migration interface {
	Version() int
	Migrate(ctx context.Context, conn *sql.DB) error
}

type Definition struct {
	ID int
	Up string
}

func (s *Definition) Version() int {
	return s.ID
}

func (s *Definition) Migrate(ctx context.Context, conn *sql.DB) error {
	if _, err := conn.ExecContext(ctx, s.Up); err != nil {
		return err
	}
	return nil
}

func MustMigrate(ctx context.Context, dsn string, migrations []Migration) {
	if err := Migrate(ctx, dsn, migrations); err != nil {
		panic(err)
	}
}

func Migrate(ctx context.Context, dsn string, migrations []Migration) error {
	if err := createDBIfNotExists(ctx, dsn); err != nil {
		return err
	}

	conn, err := connect(dsn)
	if err != nil {
		return err
	}

	if err := createMigrationsTableIfNotExists(ctx, conn); err != nil {
		return err
	}

	if err := runMigrations(ctx, conn, migrations); err != nil {
		return err
	}

	return nil
}

func MustLoadSchema(ctx context.Context, dsn string, location string) {
	if err := LoadSchema(ctx, dsn, location); err != nil {
		panic(err)
	}
}

func LoadSchema(ctx context.Context, dsn string, location string) error {
	if err := createDBIfNotExists(ctx, dsn); err != nil {
		return err
	}

	conn, err := connect(dsn)
	if err != nil {
		return err
	}

	if err := createMigrationsTableIfNotExists(ctx, conn); err != nil {
		return err
	}

	// load the migrations table with necessary version information
	if _, err := os.Stat(location + "/_migrations.sql"); os.IsNotExist(err) {
		return nil
	}

	files, err := ioutil.ReadDir(location)
	if err != nil {
		return errors.Wrapf(err, "failed reading dir %q", location)
	}

	for _, file := range files {
		name := file.Name()

		if name[len(name)-4:] == ".sql" {
			schema, err := ioutil.ReadFile(fmt.Sprintf("%s/%s", location, name))
			if err != nil {
				return errors.Wrapf(err, "unable to read %q", name)
			}
			if _, err := conn.ExecContext(ctx, string(schema)); err != nil {
				return errors.Wrapf(err, "failed loading %q", name)
			}
		}
	}

	return nil
}

func MustDumpSchema(ctx context.Context, dsn string, location string) {
	if err := DumpSchema(ctx, dsn, location); err != nil {
		panic(err)
	}
}

func DumpSchema(ctx context.Context, dsn string, location string) error {
	conn, err := connect(dsn)
	if err != nil {
		return errors.Wrap(err, "unable to dump schema")
	}

	rows, err := conn.QueryContext(ctx, "SHOW TABLES")
	if err != nil {
		return errors.Wrap(err, "unable to show tables")
	}
	defer rows.Close()

	tables := []string{}

	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return errors.Wrap(err, "unable to scan table name")
		}

		if tableName != "_migrations" {
			tables = append(tables, tableName)
		}
	}

	for _, table := range tables {
		var tableName, createStatement string
		err := conn.QueryRowContext(ctx, fmt.Sprintf("SHOW CREATE TABLE %s", table)).Scan(&tableName, &createStatement)
		if err != nil {
			return errors.Wrapf(err, "failed showing create statement for table %q", table)
		}

		err = ioutil.WriteFile(fmt.Sprintf("%s/%s.sql", location, table), []byte(createStatement), 0644)
		if err != nil {
			return errors.Wrapf(err, "failed writing out create table statement for table %q", table)
		}
	}

	rowsVersions, err := conn.QueryContext(ctx, "SELECT id, created_at FROM _migrations ORDER BY id ASC")
	if err != nil {
		return errors.Wrap(err, "unable to select from _migrations table")
	}
	defer rowsVersions.Close()

	versions := ""
	for rowsVersions.Next() {
		var id int
		var createdAt time.Time
		if err := rowsVersions.Scan(&id, &createdAt); err != nil {
			return errors.Wrap(err, "unable to scan _migrations")
		}

		versions = versions + fmt.Sprintf("(%d, %q),\n", id, createdAt.Format("2006-01-02 15:04:05"))
	}

	if len(versions) > 0 {
		migrations := fmt.Sprintf("INSERT INTO _migrations (id, created_at) VALUES\n%s", versions[:len(versions)-2])
		if err := ioutil.WriteFile(fmt.Sprintf("%s/_migrations.sql", location), []byte(migrations), 0644); err != nil {
			return errors.Wrap(err, "failed writing out create table statement for _migrations")
		}
	}

	return nil
}

func runMigrations(ctx context.Context, conn *sql.DB, migrations []Migration) error {
	if err := validateMigrations(migrations); err != nil {
		return err
	}

	for _, migration := range migrations {
		alreadyExecuted, err := migrationAlreadyExecuted(ctx, conn, migration.Version())
		if err != nil {
			return err
		}

		if !alreadyExecuted {
			start := time.Now()
			err := migration.Migrate(ctx, conn)
			if err != nil {
				return errors.Wrapf(err, "failed executing migration %d", migration.Version())
			}
			timeTaken := time.Now().Sub(start)
			if err := markMigrationSuccessful(ctx, conn, migration.Version()); err != nil {
				return err
			}
			log.Printf("executed migration %d in %s", migration.Version(), timeTaken)
		} else {
			log.Printf("skipping migration %d as it has already been executed", migration.Version())
		}
	}
	return nil
}

func validateMigrations(migrations []Migration) error {
	versions := make([]int, len(migrations))

	for i, migration := range migrations {
		for _, version := range versions {
			if version == migration.Version() {
				return errors.Errorf("duplicate migration version %d", version)
			}
		}
		versions[i] = migration.Version()
	}

	return nil
}

func oneExists(ctx context.Context, conn *sql.DB, query string, args ...interface{}) (bool, error) {
	var val interface{}
	row := conn.QueryRowContext(ctx, query, args...)
	err := row.Scan(&val)

	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, err
	default:
		return true, nil
	}
}

func migrationAlreadyExecuted(ctx context.Context, conn *sql.DB, version int) (bool, error) {
	return oneExists(ctx, conn, "SELECT id FROM _migrations WHERE id = ?", version)
}

func markMigrationSuccessful(ctx context.Context, conn *sql.DB, version int) error {
	_, err := conn.ExecContext(ctx, "INSERT INTO _migrations (id, created_at) VALUES(?, ?)", version, time.Now())
	return err
}

func createMigrationsTableIfNotExists(ctx context.Context, conn *sql.DB) error {
	exists, err := migrationsTableExists(ctx, conn)
	if err != nil {
		return errors.Wrapf(err, "failed checking if table %q exists", "_migrations")
	}

	if !exists {
		log.Printf("table _migrations doesn't exist")
		_, err := conn.ExecContext(
			ctx,
			`CREATE TABLE _migrations (
				id INT NOT NULL,
				created_at DATETIME NOT NULL,
				PRIMARY KEY (id)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci`,
		)
		if err != nil {
			return errors.Wrapf(err, "failed creating table %q", "_migrations")
		}
		log.Printf("created _migrations table")
	}
	return nil
}

func migrationsTableExists(ctx context.Context, conn *sql.DB) (bool, error) {
	return oneExists(ctx, conn, `SHOW TABLES LIKE "_migrations"`)
}

func createDBIfNotExists(ctx context.Context, dsn string) error {
	parsed, err := mysql.ParseDSN(dsn)
	if err != nil {
		return errors.Wrap(err, "unable to parse dsn")
	}

	dbname := parsed.DBName

	if len(dbname) == 0 {
		return errors.Errorf("dsn missing database name")
	}

	parsed.DBName = ""

	conn, err := connect(parsed.FormatDSN())
	if err != nil {
		return err
	}
	defer conn.Close()

	dbExists, err := dbExists(ctx, conn, dbname)
	if err != nil {
		return errors.Wrapf(err, "failed checking if db %q exists", dbname)
	}

	if !dbExists {
		log.Printf("db %q doesn't exist", dbname)
		if err := createDB(ctx, conn, dbname); err != nil {
			return errors.Wrapf(err, "failed creating db %q", dbname)
		}
		log.Printf("created db %q", dbname)
	}

	return nil
}

func dbExists(ctx context.Context, conn *sql.DB, dbname string) (bool, error) {
	return oneExists(ctx, conn, fmt.Sprintf(`SHOW DATABASES LIKE %q`, dbname))
}

func createDB(ctx context.Context, conn *sql.DB, dbname string) error {
	_, err := conn.ExecContext(
		ctx,
		fmt.Sprintf(
			`CREATE DATABASE %s
			DEFAULT CHARACTER SET = utf8mb4
			DEFAULT COLLATE = utf8mb4_unicode_520_ci`,
			dbname,
		),
	)
	if err != nil {
		return err
	}
	return nil
}
