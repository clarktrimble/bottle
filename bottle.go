package bottle

import (
	"context"
	"slices"
	"time"

	"github.com/pkg/errors"
	"go.etcd.io/bbolt"
)

// Todo: clean up any garbage in db on open

const (
	recordBucketName = "records"
	indexBucketName  = "index"
)

// Logger specifies a contextual, structured logger.
type Logger interface {
	Info(ctx context.Context, msg string, kv ...any)
	Error(ctx context.Context, msg string, err error, kv ...any)
}

type noopLogger struct{}

func (noopLogger) Info(context.Context, string, ...any) {}

func (noopLogger) Error(context.Context, string, error, ...any) {}

// Bottle provides bolt ttl'ish methods.
type Bottle struct {
	db      *bbolt.DB
	factory NewRecord
	logger  Logger
}

// New creates a Bottle instance.
func New(factory NewRecord, path string, lgr Logger) (bottle *Bottle, err error) {

	if factory == nil {
		err = errors.New("record factory is required")
		return
	}

	if lgr == nil {
		lgr = noopLogger{}
	}

	db, err := bbolt.Open(path, 0600, &bbolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		err = errors.Wrapf(err, "failed to open db file: %s", path)
		return
	}

	bottle = &Bottle{
		db:      db,
		factory: factory,
		logger:  lgr,
	}

	err = bottle.touchBuckets()
	if err != nil {
		_ = bottle.Close()
		bottle = nil
		return
	}

	return
}

// Close closes.
func (bottle *Bottle) Close() error {
	return bottle.db.Close()
}

// Upsert creates or overwrites a record.
func (bottle *Bottle) Upsert(ctx context.Context, record Record) (newRecord bool, err error) {

	err = bottle.db.Update(func(tx *bbolt.Tx) error {

		recordBucket, indexBucket, err := buckets(tx)
		if err != nil {
			return err
		}

		existing, err := bottle.get(recordBucket, idb(record))
		if err != nil {
			return err
		}

		switch {
		case existing == nil:
			newRecord = true
		case existing.Ts().After(record.Ts()):
			bottle.logger.Info(ctx, "ignoring old record", "record_id", record.Id())
			return nil
		default:
			err = indexBucket.Delete(idx(existing))
			if err != nil {
				err = errors.Wrapf(err, "failed to delete idx for id: %s", record.Id())
				return err
			}
		}

		err = add(recordBucket, indexBucket, record)
		return err
	})

	return
}

// Expire deletes old records.
func (bottle *Bottle) Expire(ctx context.Context, before time.Time) (expired []Record, err error) {

	// Todo: decide whether cutoff should be inclusive or strict before.

	err = bottle.db.Update(func(tx *bbolt.Tx) error {

		recordBucket, indexBucket, err := buckets(tx)
		if err != nil {
			return err
		}

		cursor := indexBucket.Cursor()

		// oldest to newest

		for key, id := cursor.First(); key != nil; key, id = cursor.Next() {

			data, finished, err := expire(recordBucket, cursor, key, id, before)
			if err != nil {
				// Todo: consider returning errors instead of logging and continuing.
				bottle.logger.Error(ctx, "error expiring record", err)
				continue
			}
			if finished {
				break
			}

			record, err := bottle.factory(data)
			if err != nil {
				// Todo: consider returning errors instead of logging and continuing.
				bottle.logger.Error(ctx, "error rehydrating expired record", err)
				continue
			}
			expired = append(expired, record)
		}

		return nil
	})

	return
}

// Since gets records touched at or after a time, oldest first.
func (bottle *Bottle) Since(ctx context.Context, after time.Time) (records []Record, err error) {

	records = []Record{}
	err = bottle.db.View(func(tx *bbolt.Tx) error {

		recordBucket, indexBucket, err := buckets(tx)
		if err != nil {
			return err
		}

		cursor := indexBucket.Cursor()
		for key, id := cursor.Seek(tsb(after)); key != nil; key, id = cursor.Next() {

			err = validateIdx(key, id)
			if err != nil {
				bottle.logger.Error(ctx, "error reading record index", err)
				continue
			}

			data := recordBucket.Get(id)
			if data == nil {
				bottle.logger.Error(ctx, "error reading record", errors.Errorf("no data for record with id: %s", id))
				continue
			}

			record, err := bottle.hydrate(data)
			if err != nil {
				bottle.logger.Error(ctx, "error rehydrating record", err)
				continue
			}
			records = append(records, record)
		}

		return nil
	})

	return
}

// Flush recreates buckets, deleting all records.
func (bottle *Bottle) Flush() (err error) {

	// Todo: rename ffs

	return bottle.db.Update(func(tx *bbolt.Tx) error {
		err := tx.DeleteBucket([]byte(recordBucketName))
		if err != nil {
			err = errors.Wrapf(err, "failed to delete record bucket")
			return err
		}

		err = tx.DeleteBucket([]byte(indexBucketName))
		if err != nil {
			err = errors.Wrapf(err, "failed to delete index bucket")
			return err
		}

		_, err = tx.CreateBucketIfNotExists([]byte(recordBucketName))
		if err != nil {
			err = errors.Wrapf(err, "failed to create record bucket")
			return err
		}

		_, err = tx.CreateBucketIfNotExists([]byte(indexBucketName))
		if err != nil {
			err = errors.Wrapf(err, "failed to create index bucket")
			return err
		}

		return nil
	})
}

// Get gets a record.
func (bottle *Bottle) Get(id []byte) (record Record, err error) {

	err = bottle.db.View(func(tx *bbolt.Tx) error {

		bucket, err := recordBucket(tx)
		if err != nil {
			return err
		}

		record, err = bottle.get(bucket, id)
		return err // hopefully nil!
	})

	return
}

// All gets all records.
func (bottle *Bottle) All() (records []Record, err error) {

	records = []Record{}
	err = bottle.db.View(func(tx *bbolt.Tx) error {

		bucket, err := recordBucket(tx)
		if err != nil {
			return err
		}

		cursor := bucket.Cursor()
		for key, data := cursor.First(); key != nil; key, data = cursor.Next() {

			record, err := bottle.hydrate(data)
			if err != nil {
				return err
			}
			records = append(records, record)
		}

		return nil
	})

	return
}

// unexported

func (bottle *Bottle) hydrate(data []byte) (Record, error) {
	return bottle.factory(slices.Clone(data))
}

func (bottle *Bottle) get(recordBucket *bbolt.Bucket, idb []byte) (record Record, err error) {

	data := recordBucket.Get(idb)
	if data == nil {
		return
	}

	record, err = bottle.hydrate(data)
	return
}

func add(recordBucket, indexBucket *bbolt.Bucket, record Record) (err error) {

	data, err := record.Encode()
	if err != nil {
		return
	}

	idData := idb(record)

	err = recordBucket.Put(idData, data)
	if err != nil {
		err = errors.Wrapf(err, "failed to put record, id: %s", record.Id())
		return
	}

	err = indexBucket.Put(idx(record), idData)
	err = errors.Wrapf(err, "failed to put index, id: %s", record.Id())
	return
}

func expire(bucket *bbolt.Bucket, cursor *bbolt.Cursor, key, id []byte, co time.Time) (expired []byte, fin bool, err error) {

	err = validateIdx(key, id)
	if err != nil {
		return
	}

	if after(key, co) {
		fin = true
		return
	}

	data := bucket.Get(id)
	if data == nil {
		err = errors.Errorf("no data for expired record with id: %s", id)
		return
	}
	expired = slices.Clone(data)

	err = bucket.Delete(id)
	if err != nil {
		err = errors.Wrapf(err, "failed to delete record, id: %x, index: %x", id, key)
		return
	}

	// Todo: maybe delete by id from idxBucket??
	err = cursor.Delete()
	if err != nil {
		err = errors.Wrapf(err, "failed to delete index: %x", key)
		return
	}

	return
}

func recordBucket(tx *bbolt.Tx) (bucket *bbolt.Bucket, err error) {

	bucket = tx.Bucket([]byte(recordBucketName))
	if bucket == nil {
		err = errors.Errorf("bucket not found: %s", recordBucketName)
	}
	return
}

func buckets(tx *bbolt.Tx) (record, index *bbolt.Bucket, err error) {

	record, err = recordBucket(tx)
	if err != nil {
		return
	}

	index = tx.Bucket([]byte(indexBucketName))
	if index == nil {
		err = errors.Errorf("bucket not found: %s", indexBucketName)
	}
	return
}

func (bottle *Bottle) touchBuckets() (err error) {

	return bottle.db.Update(func(tx *bbolt.Tx) error {

		_, err := tx.CreateBucketIfNotExists([]byte(recordBucketName))
		if err != nil {
			err = errors.Wrapf(err, "failed to create record bucket")
			return err
		}

		_, err = tx.CreateBucketIfNotExists([]byte(indexBucketName))
		if err != nil {
			err = errors.Wrapf(err, "failed to create index bucket")
			return err
		}

		return nil
	})
}
