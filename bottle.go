package bottle

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"go.etcd.io/bbolt"
)

// Todo: a-a-and unit test this mofo!!
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

// Bottle provides bolt ttl'ish methods.
type Bottle struct {
	db      *bbolt.DB
	factory NewRecord
	logger  Logger
}

// New creates a Bottle instance.
func New(factory NewRecord, path string, lgr Logger) (bottle *Bottle, err error) {

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
func (bottle *Bottle) Expire(ctx context.Context, before time.Time) (count int, err error) {

	err = bottle.db.Update(func(tx *bbolt.Tx) error {

		recordBucket, indexBucket, err := buckets(tx)
		if err != nil {
			return err
		}

		cursor := indexBucket.Cursor()

		// oldest to newest

		for key, id := cursor.First(); key != nil; key, id = cursor.Next() {

			finished, err := expire(recordBucket, cursor, key, id, before)
			if err != nil {
				bottle.logger.Error(ctx, "error expiring record", err)
				continue
			}
			if finished {
				break
			}

			count++
		}

		return nil
	})

	return
}

// Flush recreates buckets, deleting all records.
func (bottle *Bottle) Flush() (err error) {

	// Todo: all within a single tx plz

	err = bottle.deleteBuckets()
	if err != nil {
		return
	}

	err = bottle.touchBuckets()
	return
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

			record, err := bottle.factory(data)
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

func (bottle *Bottle) get(recordBucket *bbolt.Bucket, idb []byte) (record Record, err error) {

	data := recordBucket.Get(idb)
	if data == nil {
		return
	}

	record, err = bottle.factory(data)
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

func expire(bucket *bbolt.Bucket, cursor *bbolt.Cursor, key, id []byte, co time.Time) (fin bool, err error) {

	err = validateIdx(key, id)
	if err != nil {
		return
	}

	if after(key, co) {
		fin = true
		return
	}

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

func (bottle *Bottle) deleteBuckets() (err error) {

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

		return nil
	})
}
