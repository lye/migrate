// Package migrate provides a simple method for maintaining versioned SQL 
// database upgrades.
//
// Internally, migrate will maintain a version table that stores the current
// schema version. The calling code, on startup, constructs a Schema object
// that describes how to build the desired database schema (via Schema.Update).
// These migrations are applied in the order given if the database version is
// less than the parameter passed to Schema.Update. 
//
// Migrations are all done by calling Schema.Install, and are all performed
// within the same transaction (though this may mean nothing if your RDBMS does
// not perform DDL updates transactionally, e.g. MySQL will screw you).
package migrate

import (
	"database/sql"
)

type migration struct {
	minVersion int
	up         func(int, *sql.Tx) error
}

// Schema represents an ordered list of (minVersion, closure) pairs that are
// applied to a database when Schema.Install is invoked.
type Schema struct {
	migrations []migration
}

func getDbVersion(db *sql.DB) (int, error) {
	rows, er := db.Query("SELECT version FROM version")
	if er != nil {
		if _, er = db.Exec("CREATE TABLE version(version INT)"); er != nil {
			return 0, er
		}

		if _, er = db.Exec("INSERT INTO version(version) VALUES(0)"); er != nil {
			return 0, er
		}

		return 0, nil
	}

	if !rows.Next() {
		rows.Close()

		if _, er = db.Exec("INSERT INTO version(version) VALUES(0)"); er != nil {
			return 0, er
		}

		return 0, nil
	}

	var version int

	if er = rows.Scan(&version); er != nil {
		return 0, er
	}

	rows.Close()
	return version, nil
}

func setDbVersion(tx *sql.Tx, version int) error {
	_, er := tx.Exec(`UPDATE version SET version = $1`, version)
	return er
}

// Update appends an update closure to the receiving Schema. Updates are applied
// in the order that they are added, but only if the minVersion is less than the
// database's current version. If the passed closure returns non-nil, the entire
// migration is aborted. The closure is passed the database's current version and
// a transaction in which to perform the migration.
func (s *Schema) Update(minVersion int, f func(int, *sql.Tx) error) {
	s.migrations = append(s.migrations, migration{
		minVersion: minVersion,
		up:         f,
	})
}

// Install goes through each update closure passed to Schema.Update and applies
// it if the database's version is less than the closure's minVersion.
func (s *Schema) Install(db *sql.DB, maxVersion int) (retEr error) {
	version, er := getDbVersion(db)
	if er != nil {
		return er
	}

	tx, er := db.Begin()
	if er != nil {
		return er
	}
	defer func() {
		if retEr != nil {
			tx.Rollback()

		} else {
			retEr = tx.Commit()
		}
	}()

	for _, migration := range s.migrations {
		if migration.minVersion > version {
			if er := migration.up(version, tx); er != nil {
				return er
			}
		}
	}

	if er := setDbVersion(tx, maxVersion); er != nil {
		return er
	}

	return nil
}
