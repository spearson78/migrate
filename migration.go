package migrate

import "database/sql"

type Migration struct {
	Id        string
	Migration func(tx *sql.Tx) error
}
