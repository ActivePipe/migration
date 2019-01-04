package migration

import "database/sql"

func connect(dsn string) (*sql.DB, error) {
	return sql.Open("mysql", dsn)
}
