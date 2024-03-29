//go:build !lite

package blockstore

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/linxGnu/grocksdb"
	"go.firedancer.io/radiance/pkg/shred"
)

// MaxRoot returns the last known root slot.
func (d *DB) MaxRoot() (uint64, error) {
	opts := grocksdb.NewDefaultReadOptions()
	opts.SetVerifyChecksums(false)
	opts.SetFillCache(false)
	iter := d.DB.NewIteratorCF(opts, d.CfRoot)
	defer iter.Close()
	iter.SeekToLast()
	if !iter.Valid() {
		return 0, ErrNotFound
	}
	slot, ok := ParseSlotKey(iter.Key().Data())
	if !ok {
		return 0, fmt.Errorf("invalid key in root cf")
	}
	return slot, nil
}

// MinRoot returns the first known root slot.
func (d *DB) MinRoot() (uint64, error) {
	opts := grocksdb.NewDefaultReadOptions()
	opts.SetVerifyChecksums(false)
	opts.SetFillCache(false)
	iter := d.DB.NewIteratorCF(opts, d.CfRoot)
	defer iter.Close()
	iter.SeekToFirst()
	if !iter.Valid() {
		return 0, ErrNotFound
	}
	slot, ok := ParseSlotKey(iter.Key().Data())
	if !ok {
		return 0, fmt.Errorf("invalid key in root cf")
	}
	return slot, nil
}

func (d *DB) MinShred() (uint64, uint64, shred.Shred, error) {
	opts := grocksdb.NewDefaultReadOptions()
	opts.SetVerifyChecksums(false)
	opts.SetFillCache(false)
	iter := d.DB.NewIteratorCF(opts, d.CfDataShred)
	defer iter.Close()
	iter.SeekToFirst()
	for iter.Valid() {
		var curSlot, index uint64
		valid := iter.Valid()
		if valid {
			key := iter.Key().Data()
			if len(key) != 16 {
				iter.Next()
				continue
			}
			curSlot = binary.BigEndian.Uint64(key)
			index = binary.BigEndian.Uint64(key[8:])
		}
		s, err := parseShredAnyVersion(iter.Value().Data())
		if err != nil {
			iter.Next()
			continue
		}
		if !s.Ok() {
			iter.Next()
			continue
		}
		return curSlot, index, s, nil
	}
	return 0, 0, shred.Shred{}, ErrNotFound
}

func (d *DB) MaxShred() (uint64, uint64, shred.Shred, error) {
	opts := grocksdb.NewDefaultReadOptions()
	opts.SetVerifyChecksums(false)
	opts.SetFillCache(false)
	iter := d.DB.NewIteratorCF(opts, d.CfDataShred)
	defer iter.Close()
	iter.SeekToLast()
	for iter.Valid() {
		var curSlot, index uint64
		valid := iter.Valid()
		if valid {
			key := iter.Key().Data()
			if len(key) != 16 {
				iter.Prev()
				continue
			}
			curSlot = binary.BigEndian.Uint64(key)
			index = binary.BigEndian.Uint64(key[8:])
		}
		s, err := parseShredAnyVersion(iter.Value().Data())
		if err != nil {
			iter.Prev()
			continue
		}
		if !s.Ok() {
			iter.Prev()
			continue
		}
		return curSlot, index, s, nil
	}
	return 0, 0, shred.Shred{}, ErrNotFound
}

func parseShredAnyVersion(data []byte) (shred.Shred, error) {
	for _, revision := range []int{shred.RevisionV1, shred.RevisionV2} {
		s := shred.NewShredFromSerialized(data, revision)
		if s.Ok() {
			return s, nil
		}
	}
	return shred.Shred{}, fmt.Errorf("failed to deserialize shred")
}

// MaxMaybeRootedValidSlot returns the last valid slot, either rooted or having meta and entries.
func (d *DB) MaxMaybeRootedValidSlot() (uint64, error) {
	opts := grocksdb.NewDefaultReadOptions()
	opts.SetVerifyChecksums(false)
	opts.SetFillCache(false)
	iter := d.DB.NewIteratorCF(opts, d.CfMeta)
	defer iter.Close()
	iter.SeekToLast()
	for {
		if !iter.Valid() {
			return 0, ErrNotFound
		}
		slot, can := canRecover(d, iter)
		if can {
			return slot, nil
		}
		iter.Prev()
	}
	return 0, ErrNotFound
}

// MinMaybeRootedValidSlot returns the first valid slot, either rooted or having meta and entries.
func (d *DB) MinMaybeRootedValidSlot() (uint64, error) {
	opts := grocksdb.NewDefaultReadOptions()
	opts.SetVerifyChecksums(false)
	opts.SetFillCache(false)
	iter := d.DB.NewIteratorCF(opts, d.CfMeta)
	defer iter.Close()
	iter.SeekToFirst()
	for {
		if !iter.Valid() {
			return 0, ErrNotFound
		}
		slot, can := canRecover(d, iter)
		if can {
			return slot, nil
		}
		iter.Next()
	}
	return 0, ErrNotFound
}

func canRecover(db *DB, iter *grocksdb.Iterator) (uint64, bool) {
	slot, ok := ParseSlotKey(iter.Key().Data())
	if !ok {
		return 0, false
	}
	meta, err := ParseBincode[SlotMeta](iter.Value().Data())
	if err != nil {
		return 0, false
	}
	if meta.ParentSlot == math.MaxUint64 {
		return 0, false
	}
	// check if has entries
	_, err = getEntriesAnyVersion(db, meta)
	if err != nil {
		return 0, false
	}
	return slot, true
}

func getEntriesAnyVersion(db *DB, meta *SlotMeta) ([]Entries, error) {
	for _, revision := range []int{shred.RevisionV1, shred.RevisionV2} {
		entries, err := db.GetEntries(meta, revision)
		if err == nil {
			return entries, nil
		}
	}
	return nil, fmt.Errorf("no entries found for slot %d", meta.Slot)
}

// GetSlotMeta returns the shredding metadata of a given slot.
func (d *DB) GetSlotMeta(slot uint64) (*SlotMeta, error) {
	key := MakeSlotKey(slot)
	return GetBincode[SlotMeta](d.DB, d.CfMeta, key[:])
}
