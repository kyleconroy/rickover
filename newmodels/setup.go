package newmodels

import (
	"context"
	"errors"

	"github.com/kevinburke/rickover/models/db"
)

var DB *Queries

func Setup() (err error) {
	if !db.Connected() {
		return errors.New("jobs: no database connection, bailing")
	}

	if DB, err = Prepare(context.Background(), db.Conn); err != nil {
		return err
	}

	return
}
