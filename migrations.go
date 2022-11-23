package migrate

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/spearson78/fsql"
)

func Apply(db *sql.DB, migrations []Migration) (err error) {

	err = initializeSchema(db)
	if err != nil {
		return fmt.Errorf("initializeSchema: %w", err)
	}

	for _, migration := range migrations {

		err := applyDbChange(db, migration)
		if err != nil {
			return err
		}

	}

	return nil
}

func initializeSchema(db *sql.DB) error {

	//TODO: support different DB types

	//Ensure DB_CHANGELOG table exists
	row, err := fsql.QueryRow(db, "SELECT 1 FROM sqlite_master WHERE type='table' AND name='DB_CHANGELOG'")
	row.Scan()
	if errors.Is(err, sql.ErrNoRows) {
		_, err := fsql.Exec(db, "CREATE TABLE DB_CHANGELOG (ID TEXT PRIMARY KEY)")
		if err != nil {
			return err
		}
		return nil
	} else {
		return err
	}
}

func applyDbChange(db *sql.DB, m Migration) (err error) {

	tx, err := db.Begin()
	if err != nil {
		return Wrap(err, m.Id)
	}

	row, err := fsql.QueryRow(tx, "INSERT INTO DB_CHANGELOG (ID) VALUES(?)", m.Id)
	row.Scan()
	if errors.Is(err, sql.ErrNoRows) {
		err = Wrap(m.Migration(tx), m.Id)
	} else {
		//migration already applied
		err = nil
	}

	if err == nil {
		err = tx.Commit()
		err = Wrap(err, m.Id)
	} else {
		tx.Rollback()
	}

	return err
}
