package migration_test

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/rbone/migration"
	"github.com/stretchr/testify/require"
)

func TestCreatesDatabaseIfNoneExists(t *testing.T) {
	dbname := "createdbtest"
	dropDB(dbname)
	require.False(t, dbExists(dbname))

	migrations := []migration.Migration{
		&migration.Definition{
			ID: 1,
			Up: `CREATE TABLE blarg ( id INT NOT NULL, PRIMARY KEY(id) ) ENGINE=InnoDB`,
		},
	}

	err := migration.Migrate(context.Background(), fullDSN(dbname), migrations)
	require.NoError(t, err)
	require.True(t, dbExists(dbname))
}

func TestRunsMigrationSuccesfully(t *testing.T) {
	dbname := "runmigrationtest"
	dropDB(dbname)
	require.False(t, dbExists(dbname))

	migrations := []migration.Migration{
		&migration.Definition{
			ID: 1,
			Up: `CREATE TABLE blarg ( id INT NOT NULL, PRIMARY KEY(id) ) ENGINE=InnoDB`,
		},
	}

	err := migration.Migrate(context.Background(), fullDSN(dbname), migrations)
	require.NoError(t, err)

	schema := showSchema(fullDSN(dbname), "blarg")
	require.Equal(t,
		"CREATE TABLE `blarg` (\n"+
			"  `id` int(11) NOT NULL,\n"+
			"  PRIMARY KEY (`id`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci",
		schema)
}

func TestMarksMigrationAsHavingBeenRunSuccessfully(t *testing.T) {
	dbname := "markmigrationtest"
	dropDB(dbname)
	require.False(t, dbExists(dbname))

	migrations := []migration.Migration{
		&migration.Definition{
			ID: 1,
			Up: `CREATE TABLE blarg ( id INT NOT NULL, PRIMARY KEY(id) ) ENGINE=InnoDB`,
		},
		&migration.Definition{
			ID: 2,
			Up: `CREATE TABLE gralb ( di INT NOT NULL, PRIMARY KEY(di) ) ENGINE=InnoDB`,
		},
	}

	err := migration.Migrate(context.Background(), fullDSN(dbname), migrations)
	require.NoError(t, err)

	versions := queryVersions(fullDSN(dbname))
	require.Equal(t, 2, len(versions))
	require.Equal(t, 1, versions[0].ID)
	require.WithinDuration(t, time.Now(), versions[0].CreatedAt, time.Second)
	require.Equal(t, 2, versions[1].ID)
	require.WithinDuration(t, time.Now(), versions[1].CreatedAt, time.Second)
}

func TestDumpSchema(t *testing.T) {
	dbname := "dumpschematest"
	dropDB(dbname)
	require.False(t, dbExists(dbname))

	dir := fmt.Sprintf("%s/dumpschematest", os.TempDir())

	must(os.RemoveAll(dir))
	must(os.MkdirAll(dir, os.ModeDir))

	migrations := []migration.Migration{
		&migration.Definition{
			ID: 1,
			Up: `CREATE TABLE blarg ( id INT NOT NULL, PRIMARY KEY(id) ) ENGINE=InnoDB`,
		},
		&migration.Definition{
			ID: 2,
			Up: `CREATE TABLE gralb ( di INT NOT NULL, PRIMARY KEY(di) ) ENGINE=InnoDB`,
		},
		&migration.Definition{
			ID: 3,
			Up: `ALTER TABLE blarg ADD COLUMN something VARCHAR(64)`,
		},
	}

	err := migration.Migrate(context.Background(), fullDSN(dbname), migrations)
	require.NoError(t, err)
	err = migration.DumpSchema(context.Background(), fullDSN(dbname), dir)
	require.NoError(t, err)

	files, err := ioutil.ReadDir(dir)
	require.NoError(t, err)
	require.Equal(t, 3, len(files))
	require.Equal(t, "_migrations.sql", files[0].Name())
	require.Equal(t, "blarg.sql", files[1].Name())
	require.Equal(t, "gralb.sql", files[2].Name())

	trackedMigrations, err := ioutil.ReadFile(dir + "/_migrations.sql")
	require.NoError(t, err)
	require.Regexp(t,
		regexp.MustCompile(`\AINSERT INTO _migrations \(id, created_at\) VALUES\n\(1, "\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d"\),\n\(2, "\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d"\),\n\(3, "\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d"\)\z`),
		string(trackedMigrations),
	)

	blarg, err := ioutil.ReadFile(dir + "/blarg.sql")
	require.NoError(t, err)
	require.Equal(t,
		"CREATE TABLE `blarg` (\n"+
			"  `id` int(11) NOT NULL,\n"+
			"  `something` varchar(64) COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,\n"+
			"  PRIMARY KEY (`id`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci",
		string(blarg),
	)

	gralb, err := ioutil.ReadFile(dir + "/gralb.sql")
	require.NoError(t, err)
	require.Equal(t,
		"CREATE TABLE `gralb` (\n"+
			"  `di` int(11) NOT NULL,\n"+
			"  PRIMARY KEY (`di`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci",
		string(gralb),
	)
}

func TestLoadSchema(t *testing.T) {
	dbname := "loadschematest"
	dropDB(dbname)
	require.False(t, dbExists(dbname))

	dir := fmt.Sprintf("%s/loadschematest", os.TempDir())

	must(os.RemoveAll(dir))
	must(os.MkdirAll(dir, os.ModeDir))

	migrations := []migration.Migration{
		&migration.Definition{
			ID: 1,
			Up: `CREATE TABLE blarg ( id INT NOT NULL, PRIMARY KEY(id) ) ENGINE=InnoDB`,
		},
		&migration.Definition{
			ID: 2,
			Up: `CREATE TABLE gralb ( di INT NOT NULL, PRIMARY KEY(di) ) ENGINE=InnoDB`,
		},
		&migration.Definition{
			ID: 3,
			Up: `ALTER TABLE blarg ADD COLUMN something VARCHAR(64)`,
		},
	}

	err := migration.Migrate(context.Background(), fullDSN(dbname), migrations)
	require.NoError(t, err)
	err = migration.DumpSchema(context.Background(), fullDSN(dbname), dir)
	require.NoError(t, err)

	dropDB(dbname)
	require.False(t, dbExists(dbname))

	err = migration.LoadSchema(context.Background(), fullDSN(dbname), dir)
	require.NoError(t, err)

	versions := queryVersions(fullDSN(dbname))
	require.Equal(t, 3, len(versions))
	require.Equal(t, 1, versions[0].ID)
	require.WithinDuration(t, time.Now(), versions[0].CreatedAt, time.Second*5)
	require.Equal(t, 2, versions[1].ID)
	require.WithinDuration(t, time.Now(), versions[1].CreatedAt, time.Second*5)
	require.Equal(t, 3, versions[2].ID)
	require.WithinDuration(t, time.Now(), versions[2].CreatedAt, time.Second*5)

	blarg := showSchema(fullDSN(dbname), "blarg")
	require.Equal(t,
		"CREATE TABLE `blarg` (\n"+
			"  `id` int(11) NOT NULL,\n"+
			"  `something` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci DEFAULT NULL,\n"+
			"  PRIMARY KEY (`id`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci",
		blarg)

	gralb := showSchema(fullDSN(dbname), "gralb")
	require.Equal(t,
		"CREATE TABLE `gralb` (\n"+
			"  `di` int(11) NOT NULL,\n"+
			"  PRIMARY KEY (`di`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci",
		gralb)

	migrations = append(migrations, &migration.Definition{
		ID: 4,
		Up: `ALTER TABLE blarg DROP COLUMN something`,
	})

	err = migration.Migrate(context.Background(), fullDSN(dbname), migrations)
	require.NoError(t, err)

	versions = queryVersions(fullDSN(dbname))
	require.Equal(t, 4, len(versions))
	require.Equal(t, 4, versions[3].ID)
	require.WithinDuration(t, time.Now(), versions[3].CreatedAt, time.Second*5)

	blarg = showSchema(fullDSN(dbname), "blarg")
	require.Equal(t,
		"CREATE TABLE `blarg` (\n"+
			"  `id` int(11) NOT NULL,\n"+
			"  PRIMARY KEY (`id`)\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci",
		blarg)
}

type version struct {
	ID        int
	CreatedAt time.Time
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func queryVersions(dsn string) []version {
	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	var versions []version

	rows, err := conn.Query("SELECT * FROM _migrations ORDER BY id ASC")
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		version := version{}
		if err := rows.Scan(&version.ID, &version.CreatedAt); err != nil {
			panic(err)
		}
		versions = append(versions, version)
	}

	return versions
}

func showSchema(dsn string, table string) string {
	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	var tableName, createStatement string
	err = conn.QueryRow(fmt.Sprintf("SHOW CREATE TABLE %s", table)).Scan(&tableName, &createStatement)
	if err != nil {
		panic(fmt.Errorf("failed checking if table %q exists: %s", table, err))
	}

	return createStatement
}

func partialDSN() string {
	return os.Getenv("DATABASE_DSN")
}

func fullDSN(dbname string) string {
	dsn, err := mysql.ParseDSN(partialDSN())
	if err != nil {
		panic(err)
	}

	dsn.DBName = fmt.Sprintf("migration_test_%s", dbname)
	return dsn.FormatDSN()
}

func dropDB(db string) {
	conn, err := sql.Open("mysql", partialDSN())
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	if _, err := conn.Exec(fmt.Sprintf(`DROP DATABASE IF EXISTS migration_test_%s`, db)); err != nil {
		panic(err)
	}
}

func dbExists(db string) bool {
	conn, err := sql.Open("mysql", partialDSN())
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	return oneExists(conn, fmt.Sprintf(`SHOW DATABASES LIKE %q`, "migration_test_"+db))
}

func oneExists(conn *sql.DB, query string, args ...interface{}) bool {
	var val interface{}
	row := conn.QueryRow(query, args...)
	err := row.Scan(&val)

	switch {
	case err == sql.ErrNoRows:
		return false
	case err != nil:
		panic(err)
	default:
		return true
	}
}
