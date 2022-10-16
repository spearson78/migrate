package migrate

import (
	"database/sql"
	"fmt"
	"log"
)

func Apply(db *sql.DB, migrations []Migration) (err error) {

	err = initializeSchema(db)
	if err != nil {
		return err
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
	row := db.QueryRow("SELECT 1 FROM sqlite_master WHERE type='table' AND name='DB_CHANGELOG'")
	if row.Scan() == sql.ErrNoRows {
		_, err := db.Exec("CREATE TABLE DB_CHANGELOG (ID TEXT PRIMARY KEY)")
		if err != nil {
			return err
		}
	}

	return nil
}

func applyDbChange(db *sql.DB, m Migration) (err error) {

	tx, err := db.Begin()
	if err != nil {
		return
	}

	row := tx.QueryRow("INSERT INTO DB_CHANGELOG (ID) VALUES(?)", m.Id)
	err = row.Scan()
	if err == sql.ErrNoRows {
		log.Println("applying migration", m.Id)
		err = m.Migration(tx)
	} else {
		//migration already applied
		err = nil
	}

	if err == nil {
		tx.Commit()
	} else {
		tx.Rollback()
	}

	return fmt.Errorf("migration error id: %v err: %w", m.Id, err)
}
