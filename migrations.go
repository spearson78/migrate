package migrate

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/Southclaws/fault"
	"github.com/spearson78/fsql"
	"modernc.org/sqlite"
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
		return fault.Wrap(err, With(m.Id))
	}

	_, err = fsql.Exec(tx, "INSERT INTO DB_CHANGELOG (ID) VALUES(?)", m.Id)
	if err != nil {
		//Could not insert maybe DB error or constraint violation
		var sqliteError *sqlite.Error
		if errors.As(err, &sqliteError) && sqliteError.Code() == 1555 { //Primary key violation
			err = nil
		}
	} else {
		//The row was inserted apply the DB change
		err = m.Migration(tx)
	}

	if err == nil {
		err = tx.Commit()
	} else {
		tx.Rollback()
	}

	if err != nil {
		err = fault.Wrap(err, With(m.Id))
	}
	return err
}
