// Package blockstore is a read-only client for the Solana blockstore database.
//
// For the reference implementation in Rust, see here:
// https://docs.rs/solana-ledger/latest/solana_ledger/blockstore/struct.Blockstore.html
//
// This package requires Cgo to access RocksDB (via grocksdb).
//
// # Compatibility
//
// We aim to support all Solana Rust versions since mainnet genesis.
// Test fixtures are added for each major revision.
package blockstore

import (
	"errors"
)

// Column families
const (
	// CfDefault is the default column family, which is required by RocksDB.
	CfDefault = "default"

	// CfMeta contains slot metadata (SlotMeta)
	//
	// Similar to a block header, but not cryptographically authenticated.
	CfMeta = "meta"

	// CfErasureMeta contains erasure coding metadata
	CfErasureMeta = "erasure_meta"

	// CfRoot is a single cell specifying the current root slot number
	CfRoot = "root"

	// CfDataShred contains ledger data.
	//
	// One or more shreds make up a single entry.
	// The shred => entry surjection is indicated by SlotMeta.EntryEndIndexes
	CfDataShred = "data_shred"

	// CfCodeShred contains FEC shreds used to fix data shreds
	CfCodeShred = "code_shred"

	// CfDeadSlots contains slots that have been marked as dead
	CfDeadSlots = "dead_slots"

	CfBlockHeight = "block_height"

	CfBankHash = "bank_hashes"

	// CfTxStatus contains transaction metadata (TransactionStatusMeta)
	CfTxStatus = "transaction_status"

	CfTxStatusIndex = "transaction_status_index"

	CfAddressSig = "address_signatures"

	CfTxMemos = "transaction_memos"

	CfRewards = "rewards"

	CfBlockTime = "blocktime"

	CfPerfSamples = "perf_samples"

	CfProgramCosts = "program_costs"

	CfOptimisticSlots = "optimistic_slots"
)

var (
	ErrNotFound         = errors.New("not found")
	ErrDeadSlot         = errors.New("dead slot")
	ErrInvalidShredData = errors.New("invalid shred data")
)
