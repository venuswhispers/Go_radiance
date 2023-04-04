//go:build !lite

package verifydata

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/VividCortex/ewma"
	"github.com/davecgh/go-spew/spew"
	"github.com/dustin/go-humanize"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/golang/protobuf/proto"
	"github.com/klauspost/compress/zstd"
	"github.com/linxGnu/grocksdb"
	"github.com/mattn/go-isatty"
	"github.com/spf13/cobra"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
	"go.firedancer.io/radiance/pkg/blockstore"
	"go.firedancer.io/radiance/third_party/solana_proto/confirmed_block"
	"golang.org/x/sync/errgroup"
	"k8s.io/klog/v2"
)

var Cmd = cobra.Command{
	Use:   "verify-data <rocksdb>",
	Short: "Verify ledger data integrity",
	Long: "Iterates through all data shreds and performs sanity checks.\n" +
		"Useful for checking the correctness of the Radiance implementation.\n" +
		"\n" +
		"Scans through the data-shreds column family with multiple threads (divide-and-conquer).",
	Args: cobra.ExactArgs(1),
}

var flags = Cmd.Flags()

var (
	flagWorkers  = flags.UintP("workers", "w", uint(runtime.NumCPU()), "Number of goroutines to verify with")
	flagMaxErrs  = flags.Uint32("max-errors", 100, "Abort after N errors")
	flagStatIvl  = flags.Duration("stat-interval", 5*time.Second, "Stats interval")
	flagDumpSigs = flags.Bool("dump-sigs", false, "Print first signature of each transaction")
)

// TODO add a progress bar :3

func init() {
	Cmd.Run = run
}

func formatTxMetadataKey(slot uint64, sig solana.Signature) []byte {
	key := make([]byte, 80)
	// the first 8 bytes are empty; fill them with zeroes
	copy(key[:8], []byte{0, 0, 0, 0, 0, 0, 0, 0})
	// then comes the signature
	copy(key[8:], sig[:])
	// then comes the slot
	binary.BigEndian.PutUint64(key[72:], slot)
	return key
}

func parseTxMeta(buf []byte) (*confirmed_block.TransactionStatusMeta, error) {
	var status confirmed_block.TransactionStatusMeta
	err := proto.Unmarshal(buf, &status)
	if err != nil {
		return nil, err
	}
	return &status, nil
}

func init() {
	spew.Config.DisableMethods = true
	spew.Config.DisablePointerMethods = true
	spew.Config.MaxDepth = 5
}

func run(c *cobra.Command, args []string) {
	// {
	// 	buf := []byte{0, 0, 0, 0, 6, 4, 180, 185}
	// 	slot := binary.BigEndian.Uint64(buf)

	// 	fmt.Println(slot)
	// 	return
	// }
	start := time.Now()

	workers := uint(1)
	rocksDB := args[0]
	db, err := blockstore.OpenReadOnly(rocksDB)
	if err != nil {
		klog.Exitf("Failed to open blockstore: %s", err)
	}
	defer db.Close()
	if false {
		// get list of all column families
		dbOpts := grocksdb.NewDefaultOptions()
		allCfNames, err := grocksdb.ListColumnFamilies(dbOpts, rocksDB)
		if err != nil {
			klog.Exitf("Failed to list column families: %s", err)
		}
		klog.Infof("Found %d column families", len(allCfNames))
		for _, cfName := range allCfNames {
			klog.Infof(" - %s", cfName)
		}
		if true {
			defer func() {
				klog.Infof("Done in %s", time.Since(start))
			}()
			totalSize := uint64(0)
			numItems := uint64(0)

			// create a file that will contain all the compressed metadata
			compressedMetaFile := NewWriterCounter()
			defer func() {
				// print file size
				size := uint64(compressedMetaFile.Count())
				klog.Infof("Compressed metadata file size: %d (%s)", size, humanize.Bytes(size))
			}()

			compressedMetaWriter, err := zstd.NewWriter(compressedMetaFile)
			if err != nil {
				panic(err)
			}

			// iterate through all column families
			{
				txStatusHandle := db.CfTxStatus
				klog.Infof("Iterating through column family %q", txStatusHandle.Name())

				numItemsPerColumnFamily := uint64(0)
				sizePerColumnFamilyRaw := uint64(0)
				sizePerColumnFamilyCompressed := uint64(0)
				timeCompressionPerColumnFamily := time.Duration(0)

				targetTxSignature := solana.MustSignatureFromBase58("5ottwGGNk8hPcYjgLf9EuXxy7xG48ZHvCqUaYhYbiZRs3ZPc9MxK5bccMeYf9aoQhcBup5ULqvQWgWEjvSqmesHM")

				// iterate through all keys
				iter := db.DB.NewIteratorCF(grocksdb.NewDefaultReadOptions(), txStatusHandle)
				defer iter.Close()
				iter.SeekToFirst()
				for iter.Valid() {
					numItems++

					if numItems%1_000_000 == 0 {
						fmt.Print(".")
					}
					key := iter.Key()
					value := iter.Value()
					numItemsPerColumnFamily += 1

					{
						// try to find the signature inside the key
						if bytes.Contains(key.Data(), targetTxSignature[:]) {
							klog.Infof("Found target signature in key: %s ||| %s", bin.FormatByteSlice(key.Data()), bin.FormatByteSlice(targetTxSignature[:]))

							status, err := parseTxMeta(value.Data())
							if err != nil {
								panic(err)
							}

							spew.Dump(status)

							// marshal to json
							jsonBytes, err := json.MarshalIndent(status, "", "  ")
							if err != nil {
								panic(err)
							}
							klog.Infof("JSON: %s", string(jsonBytes))
						}
					}

					size := uint64(len(key.Data()) + len(value.Data()))
					sizePerColumnFamilyRaw += size

					totalSize += size

					// if txStatusHandle.Name() == "transaction_status" && len(value.Data()) > 300 {
					{

						// use github.com/gagliardetto/radiance/ledger and decode from protobuf
						if false {
							took := time.Now()
							// use zstd to compress
							compressed := Compress(value.Data())
							timeCompressionPerColumnFamily += time.Since(took)
							sizePerColumnFamilyCompressed += uint64(len(compressed))
						}
						{
							// use zstd to decompress
							wrote, err := compressedMetaWriter.Write(value.Data())
							if err != nil {
								panic(err)
							}
							_ = wrote
						}
						if false {
							// try decoding
							var status confirmed_block.TransactionStatusMeta
							err := proto.Unmarshal(value.Data(), &status)
							if err != nil {
								panic(err)
							}
						}

						if false {
							klog.Infof(" - %q: %q", bin.FormatByteSlice(key.Data()), bin.FormatByteSlice(value.Data()))
							spew.Dump(key.Data(), value.Data())

							status, err := parseTxMeta(value.Data())
							if err != nil {
								panic(err)
							}
							spew.Config.DisableMethods = true
							spew.Config.DisablePointerMethods = true
							spew.Config.MaxDepth = 5

							spew.Dump(status)

							// marshal to json
							jsonBytes, err := json.MarshalIndent(status, "", "  ")
							if err != nil {
								panic(err)
							}
							klog.Infof("JSON: %s", string(jsonBytes))

							os.Exit(0)
						}
					}
					iter.Next()
				}
				klog.Infof("Total items in column family %q: %d", txStatusHandle.Name(), numItemsPerColumnFamily)
				klog.Infof("Total size in column family %q: %d (%s)", txStatusHandle.Name(), sizePerColumnFamilyRaw, humanize.Bytes(sizePerColumnFamilyRaw))
				klog.Infof("Total size in column family %q (compressed): %d (%s)", txStatusHandle.Name(), sizePerColumnFamilyCompressed, humanize.Bytes(sizePerColumnFamilyCompressed))
				klog.Infof("Total time spent compressing in column family %q: %s", txStatusHandle.Name(), timeCompressionPerColumnFamily)
				klog.Infof("Compression rate in column family %q: %s", txStatusHandle.Name(), hunamizeAndRate(sizePerColumnFamilyRaw, sizePerColumnFamilyCompressed))
				klog.Infof("Average raw size per item in column family %q: %f", txStatusHandle.Name(), float64(sizePerColumnFamilyRaw)/float64(numItemsPerColumnFamily))
				klog.Infof("Average compressed size per item in column family %q: %f", txStatusHandle.Name(), float64(sizePerColumnFamilyCompressed)/float64(numItemsPerColumnFamily))
				klog.Infof("Compression rate (if all went to same compressed blob) in column family %q: %s", txStatusHandle.Name(), hunamizeAndRate(sizePerColumnFamilyRaw, compressedMetaFile.Count()))
			}
			klog.Infof("Total size: %d (%s)", totalSize, humanize.Bytes(totalSize))
		}
		return
	}

	// total amount of slots
	slotLo, slotHi, ok := slotBounds(db)
	if !ok {
		klog.Exitf("Cannot find slot boundaries")
	}
	if slotLo > slotHi {
		panic("wtf: slotLo > slotHi")
	}
	total := slotHi - slotLo
	klog.Infof("Verifying %d slots", total)

	// per-worker amount of slots
	step := total / uint64(workers)
	if step == 0 {
		step = 1
	}
	cursor := slotLo
	klog.Infof("Slots per worker: %d", step)

	// stats trackers
	var numSuccess atomic.Uint64
	var numSkipped atomic.Uint64
	var numFailure atomic.Uint32
	var numBytes atomic.Uint64
	var numTxns atomic.Uint64

	// application lifetime
	rootCtx := c.Context()
	ctx, cancel := context.WithCancel(rootCtx)
	defer cancel()
	group, ctx := errgroup.WithContext(ctx)

	txRate := ewma.NewMovingAverage(7)
	lastStatsUpdate := time.Now()
	var lastNumTxns uint64
	updateEWMA := func() {
		now := time.Now()
		sinceLast := now.Sub(lastStatsUpdate)
		curNumTxns := numTxns.Load()
		increase := curNumTxns - lastNumTxns
		iRate := float64(increase) / sinceLast.Seconds()
		txRate.Add(iRate)
		lastNumTxns = curNumTxns
		lastStatsUpdate = now
	}
	stats := func() {
		klog.Infof("[stats] good=%d skipped=%d bad=%d tps=%.0f",
			numSuccess.Load(), numSkipped.Load(), numFailure.Load(), txRate.Value())
	}

	var barOutput io.Writer
	isAtty := isatty.IsTerminal(os.Stderr.Fd())
	if isAtty {
		barOutput = os.Stderr
	} else {
		barOutput = io.Discard
	}

	progress := mpb.NewWithContext(ctx, mpb.WithOutput(barOutput))
	bar := progress.New(int64(total), mpb.BarStyle(),
		mpb.PrependDecorators(
			decor.Spinner(nil),
			decor.CurrentNoUnit(" %d"),
			decor.TotalNoUnit(" / %d slots"),
			decor.NewPercentage(" (% d)"),
		),
		mpb.AppendDecorators(
			decor.Name("eta="),
			decor.AverageETA(decor.ET_STYLE_GO),
		))

	if isAtty {
		klog.LogToStderr(false)
		klog.SetOutput(progress)
	}

	statInterval := *flagStatIvl
	if statInterval > 0 {
		statTicker := time.NewTicker(statInterval)
		rateTicker := time.NewTicker(250 * time.Millisecond)
		go func() {
			defer statTicker.Stop()
			defer rateTicker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-statTicker.C:
					stats()
				case <-rateTicker.C:
					updateEWMA()
				}
			}
		}()
	}

	printFirstThenStop := false
	callback := func(slotMeta blockstore.SlotMeta, entries []blockstore.Entries) bool {
		keysToBeFound := make([][]byte, 0)
		for _, outer := range entries {
			for _, e := range outer.Entries {
				for _, tx := range e.Txns {
					if len(tx.Signatures) > 0 {
						{
							firstSignature := tx.Signatures[0]
							if printFirstThenStop {
								spew.Dump(slotMeta)
								fmt.Println(firstSignature.String())
							}
							key := formatTxMetadataKey(slotMeta.Slot, firstSignature)
							keysToBeFound = append(keysToBeFound, key)
							if printFirstThenStop {
								os.Exit(0)
							}
						}
					}
				}
			}
		}
		{
			got, err := db.DB.MultiGet(grocksdb.NewDefaultReadOptions(), keysToBeFound...)
			if err != nil {
				panic(err)
			}
			{
				for i, key := range keysToBeFound {
					if got[i] == nil {
						fmt.Println("not found", hex.EncodeToString(key))
					}
					status, err := parseTxMeta(got[i].Data())
					if err != nil {
						panic(err)
					}
					status.Reset()
				}
			}
			got.Destroy()
		}
		return true
	}

	for i := uint(0); i < workers; i++ {
		// Find segment assigned to worker
		wLo := cursor
		wHi := wLo + step
		if wHi > slotHi || i == workers-1 {
			wHi = slotHi
		}
		cursor = wHi
		if wLo >= wHi {
			break
		}

		klog.Infof("[worker %d]: range=[%d:%d]", i, wLo, wHi)
		w := &worker{
			id:          i,
			bar:         bar,
			stop:        wHi,
			numSuccess:  &numSuccess,
			numSkipped:  &numSkipped,
			numFailures: &numFailure,
			maxFailures: *flagMaxErrs,
			numBytes:    &numBytes,
			numTxns:     &numTxns,
		}
		w.init(db, wLo)
		group.Go(func() error {
			defer w.close()
			return w.run(ctx, callback)
		})
	}

	err = group.Wait()
	if isAtty {
		klog.Flush()
		klog.SetOutput(os.Stderr)
	}

	var exitCode int
	if err != nil {
		klog.Errorf("Aborting: %s", err)
		exitCode = 1
	} else if err = rootCtx.Err(); err == nil {
		klog.Info("Done!")
		exitCode = 0
	} else {
		klog.Infof("Aborted: %s", err)
		exitCode = 1
	}

	stats()
	timeTaken := time.Since(start)
	klog.Infof("Time taken: %s", timeTaken)
	klog.Infof("Bytes Read: %d (%.2f MB/s)", numBytes.Load(), float64(numBytes.Load())/timeTaken.Seconds()/1000000)
	klog.Infof("Transaction Count: %d (%.2f tps)", numTxns.Load(), float64(numTxns.Load())/timeTaken.Seconds())
	os.Exit(exitCode)
}

// slotBounds returns the lowest and highest available slots in the meta table.
func slotBounds(db *blockstore.DB) (low uint64, high uint64, ok bool) {
	iter := db.DB.NewIteratorCF(grocksdb.NewDefaultReadOptions(), db.CfMeta)
	defer iter.Close()

	iter.SeekToFirst()
	if ok = iter.Valid(); !ok {
		return
	}
	low, ok = blockstore.ParseSlotKey(iter.Key().Data())
	if !ok {
		return
	}

	iter.SeekToLast()
	if ok = iter.Valid(); !ok {
		return
	}
	high, ok = blockstore.ParseSlotKey(iter.Key().Data())
	high++
	return
}

var encoder, _ = zstd.NewWriter(nil,
	// zstd.WithEncoderLevel(zstd.SpeedBestCompression),
	zstd.WithEncoderLevel(zstd.SpeedBetterCompression),
)

// Compress a buffer.
// If you have a destination buffer, the allocation in the call can also be eliminated.
func Compress(src []byte) []byte {
	return encoder.EncodeAll(src, make([]byte, 0, len(src)))
}

func hunamizeAndRate(sizeOriginal uint64, sizeCompressed uint64) string {
	return fmt.Sprintf("%s (x%.2f)", humanize.Bytes((sizeCompressed)), calcCompressionRate(sizeOriginal, sizeCompressed))
}

func calcCompressionRate(sizeOriginal uint64, sizeCompressed uint64) float64 {
	return float64(sizeOriginal) / float64(sizeCompressed)
}

type WriterCounter struct {
	counter *uint64
}

func NewWriterCounter() *WriterCounter {
	return &WriterCounter{counter: new(uint64)}
}

func (w *WriterCounter) Write(p []byte) (int, error) {
	*w.counter += uint64(len(p))
	return len(p), nil
}

func (w *WriterCounter) Count() uint64 {
	if w.counter == nil {
		return 0
	}
	return *w.counter
}
