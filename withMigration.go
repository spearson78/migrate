package migrate

import (
	"errors"
	"fmt"
)

type withMigration struct {
	wrapped       error
	migrationName string
}

func (e *withMigration) Error() string  { return fmt.Sprintf("<migration> : %v", e.wrapped) }
func (e *withMigration) Cause() error   { return e.wrapped }
func (e *withMigration) Unwrap() error  { return e.wrapped }
func (e *withMigration) String() string { return e.Error() }

func Wrap(err error, migrationName string) error {
	if err == nil {
		return nil
	}

	return &withMigration{
		wrapped:       err,
		migrationName: migrationName,
	}
}

func With[E any](sql string, migrationName string) func(error) error {
	return func(err error) error {
		return Wrap(err, migrationName)
	}
}

func Get(err error) (string, bool) {
	if err == nil {
		return "", false
	}

	var with *withMigration
	if errors.As(err, &with) {
		return with.migrationName, true
	}

	return "", false
}
