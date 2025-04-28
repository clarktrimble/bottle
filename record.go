package bottle

import (
	"encoding/binary"
	"time"

	"github.com/pkg/errors"
)

// Record specifies a record interface.
type Record interface {
	Id() (id string)
	Ts() (timestamp time.Time)
	Encode() (data []byte, err error)
}

// NewRecord creates a record from data.
type NewRecord func(data []byte) (record Record, err error)

// unexported

func idb(record Record) []byte {
	return []byte(record.Id())
}

func tsm(record Record) int64 {
	return record.Ts().UnixMilli()
}

func idx(record Record) []byte {
	tsBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(tsBytes, uint64(tsm(record)))
	return append(tsBytes, idb(record)...)
}

func validateIdx(key, id []byte) error {

	if len(key) < 8 {
		err := errors.Errorf("malformed index key: %x with id: %x", key, id)
		return err
	}

	if len(id) == 0 {
		err := errors.Errorf("empty id for index key: %x", key)
		return err
	}

	return nil
}

func after(key []byte, cutoff time.Time) bool {

	ts := int64(binary.BigEndian.Uint64(key[0:8]))
	return ts > cutoff.UnixMilli()
}
