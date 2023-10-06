package blockstore

import (
	"context"
	"fmt"
	"math"
	"sort"

	"github.com/vbauerster/mpb/v8"
	"go.firedancer.io/radiance/pkg/slotedges"
	"k8s.io/klog/v2"
)

type TraversalSchedule struct {
	shredRevision                   int
	nextShredRevisionActivationSlot *uint64

	///
	totalSlotsToProcess uint64
	bar                 *mpb.Bar
	///

	schedule []DBtoSlots
}

type ScheduleIterator struct {
	schedule *TraversalSchedule
	limit    uint64
}

func (s TraversalSchedule) NewIterator(limit uint64) *ScheduleIterator {
	return &ScheduleIterator{
		schedule: &s,
		limit:    limit,
	}
}

// TraversalSchedule.Iterate iterates over the schedule.
func (i *ScheduleIterator) Iterate(
	ctx context.Context,
	f func(dbIdex int, h *WalkHandle, slot uint64, shredRevision int) error,
) error {
	numDone := uint64(0)
	return i.schedule.EachSlot(
		ctx,
		func(dbIdex int, h *WalkHandle, slot uint64, shredRevision int) error {
			if err := f(dbIdex, h, slot, shredRevision); err != nil {
				return err
			}
			numDone++
			if i.limit > 0 && numDone >= i.limit {
				return ErrStopIteration
			}
			return nil
		})
}

var ErrStopIteration = fmt.Errorf("stop iteration")

func (s TraversalSchedule) FirstSlot() (uint64, bool) {
	first := s.firstNonEmptyDB()
	if first == nil {
		return 0, false
	}
	return first.FirstSlot()
}

func (s TraversalSchedule) LastSlot() (uint64, bool) {
	last := s.lastNonEmptyDB()
	if last == nil {
		return 0, false
	}
	return last.LastSlot()
}

// Each iterates over each DB in the schedule.
func (s TraversalSchedule) Each(
	ctx context.Context,
	callback func(dbIndex int, db *WalkHandle, slots []uint64) error,
) error {
	for dbIndex, db := range s.schedule {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := callback(dbIndex, db.handle, db.slots); err != nil {
			if err == ErrStopIteration {
				return nil
			}
			return err
		}
	}
	return nil
}

// EachSlot iterates over each slot in the schedule.
func (s TraversalSchedule) EachSlot(
	ctx context.Context,
	f func(dbIdex int, h *WalkHandle, slot uint64, shredRevision int) error,
) error {
	if err := s.initStatsTracker(ctx); err != nil {
		return err
	}
	defer s.bar.Abort(true)
	shredRevision := s.shredRevision
	nextRevisionActivationSlot := s.nextShredRevisionActivationSlot

	activationSlot := cloneUint64Ptr(nextRevisionActivationSlot)
	return s.Each(
		ctx,
		func(dbIndex int, db *WalkHandle, slots []uint64) error {
			for _, slot := range slots {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				s.bar.Increment()
				// If we have a shred revision, we need to check if the slot is in the range of the next revision activation.
				// If so, we need to increment the shred revision.
				if activationSlot != nil && slot >= *activationSlot {
					shredRevision++
					activationSlot = nil
				}
				if err := f(dbIndex, db, slot, shredRevision); err != nil {
					return err
				}
			}
			return nil
		})
}

func (s TraversalSchedule) firstNonEmptyDB() *DBtoSlots {
	for _, db := range s.schedule {
		if len(db.slots) > 0 {
			return &db
		}
	}
	return nil
}

func (s TraversalSchedule) lastNonEmptyDB() *DBtoSlots {
	for i := len(s.schedule) - 1; i >= 0; i-- {
		db := s.schedule[i]
		if len(db.slots) > 0 {
			return &db
		}
	}
	return nil
}

func (s TraversalSchedule) NumSlots() int {
	var numSlots int
	for _, db := range s.schedule {
		numSlots += len(db.slots)
	}
	return numSlots
}

func (s TraversalSchedule) SatisfiesEpochEdges(epoch uint64) error {
	if len(s.schedule) == 0 {
		return fmt.Errorf("schedule is empty")
	}

	start, end := slotedges.CalcEpochLimits(epoch)

	{ // The first DB's first slot must be <= the start slot.
		firstDB := s.firstNonEmptyDB()
		if firstDB == nil {
			return fmt.Errorf("all DBs are empty")
		}
		firstSlot, ok := firstDB.FirstSlot()
		if !ok {
			return fmt.Errorf("first DB has no slots")
		}
		if firstSlot > start {
			// NOTE: in exceptional cases, the epoch has a few skipped slots at the beginning,
			// and the DB has no slots before that.
			return fmt.Errorf(
				"epoch %d first slot %d is not available in any DB (first available slot is %d)",
				epoch,
				start,
				firstSlot,
			)
		}
	}

	{ // The first DB's last slot must be >= the end slot.
		lastDB := s.lastNonEmptyDB()
		if lastDB == nil {
			return fmt.Errorf("all DBs are empty")
		}
		lastSlot, ok := lastDB.LastSlot()
		if !ok {
			return fmt.Errorf("last DB has no slots")
		}
		if lastSlot < end {
			return fmt.Errorf(
				"epoch %d last slot %d is not available in any DB (last available slot is %d)",
				epoch,
				end,
				lastSlot,
			)
		}
	}

	return nil
}

type DBtoSlots struct {
	handle *WalkHandle
	slots  []uint64
}

// DBtoSlots.Slots returns the slots in ascending order.
func (d DBtoSlots) Slots() []uint64 {
	Uint64Ascending(d.slots)
	return d.slots
}

func (d DBtoSlots) FirstSlot() (uint64, bool) {
	if len(d.slots) == 0 {
		return 0, false
	}
	return d.slots[0], true
}

func (d DBtoSlots) LastSlot() (uint64, bool) {
	if len(d.slots) == 0 {
		return 0, false
	}
	return d.slots[len(d.slots)-1], true
}

// DBtoSlots.DBHandle returns the DBHandle.
func (d DBtoSlots) DBHandle() *WalkHandle {
	return d.handle
}

func (s TraversalSchedule) Slots() []uint64 {
	var slots []uint64
	for _, db := range s.schedule {
		slots = append(slots, db.slots...)
	}
	Uint64Ascending(slots)
	if !uint64SlicesAreEqual(slots, unique(slots)) {
		klog.Error("slots contains duplicates")
	}
	return unique(slots)
}

func contains(slots []uint64, slot uint64) bool {
	i := SearchUint64(slots, slot)
	return i < len(slots) && slots[i] == slot
}

func SearchUint64(a []uint64, x uint64) int {
	return sort.Search(len(a), func(i int) bool { return a[i] >= x })
}

// Uint64Ascending sorts a slice of uint64 in increasing order.
func Uint64Ascending(x []uint64) { sort.Sort(Uint64Slice(x)) }

func Uint64Descending(x []uint64) {
	sort.Sort(sort.Reverse(Uint64Slice(x)))
}

type Uint64Slice []uint64

func (x Uint64Slice) Len() int           { return len(x) }
func (x Uint64Slice) Less(i, j int) bool { return x[i] < x[j] }
func (x Uint64Slice) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }

// Sort is a convenience method: x.Sort() calls Sort(x).
func (x Uint64Slice) Sort() { sort.Sort(x) }

func unique(slots []uint64) []uint64 {
	var out []uint64
	for _, slot := range slots {
		if !contains(out, slot) {
			out = append(out, slot)
		}
	}
	return out
}

func uint64OrNil(x *uint64) any {
	if x == nil {
		return nil
	}
	return *x
}

func NewSchedule(
	epoch uint64,
	requireFullEpoch bool,
	handles []*WalkHandle,
	shredRevision int,
	nextRevisionActivationSlot uint64,
) (*TraversalSchedule, error) {
	var activationSlot *uint64
	if nextRevisionActivationSlot != 0 {
		activationSlot = &nextRevisionActivationSlot
	}
	if err := sortWalkHandles(handles, shredRevision, activationSlot); err != nil {
		return nil, err
	}
	for ii := range handles {
		handles[ii].shredRevision = shredRevision
		handles[ii].nextShredRevisionActivationSlot = activationSlot
	}
	officialEpochStart, officialEpochStop := slotedges.CalcEpochLimits(epoch)
	haveStart, haveStop := slotEdges(handles)

	var startFrom, stopAt uint64
	if requireFullEpoch {
		startFrom, stopAt = officialEpochStart, officialEpochStop
	} else {
		startFrom, stopAt = haveStart, haveStop
	}

	if !slotedges.Uint64RangesHavePartialOverlapIncludingEdges(
		[2]uint64{officialEpochStart, officialEpochStop},
		[2]uint64{startFrom, stopAt},
	) {
		klog.Exitf(
			"no overlap between requested epoch [%d:%d] and available slots [%d:%d]",
			officialEpochStart, officialEpochStop,
			startFrom, stopAt,
		)
	}

	schedule := new(TraversalSchedule)
	err := schedule.init(epoch, startFrom, stopAt, requireFullEpoch, handles, shredRevision, activationSlot)
	if err != nil {
		return nil, err
	}
	schedule.totalSlotsToProcess = uint64(schedule.NumSlots())
	return schedule, nil
}

func (s *TraversalSchedule) Close() error {
	for _, db := range s.schedule {
		db.handle.DB.Close()
	}
	return nil
}

func (s *TraversalSchedule) getHandles() []*WalkHandle {
	var out []*WalkHandle
	for _, db := range s.schedule {
		out = append(out, db.handle)
	}
	return out
}

func (s *TraversalSchedule) RootExistsInAnyDB(slot uint64) (string, error) {
	for _, h := range s.getHandles() {
		key := encodeSlotAsKey(slot)
		opts := getReadOptions()
		defer putReadOptions(opts)
		got, err := h.DB.DB.GetCF(opts, h.DB.CfRoot, key)
		if err != nil {
			continue
		}
		defer got.Free()
		if got == nil || got.Size() == 0 {
			continue
		}
		if !got.Exists() {
			continue
		}
		return h.DB.DB.Name(), nil
	}
	return "", fmt.Errorf("slot %d not found in any DB", slot)
}

func (m *TraversalSchedule) GetSlotMetaFromAnyDB(slot uint64) (*SlotMeta, error) {
	for _, h := range m.getHandles() {
		got, err := h.DB.GetSlotMeta(slot)
		if err == nil && got != nil {
			return got, nil
		}
	}
	return nil, fmt.Errorf("meta for slot %d not found in any DB", slot)
}

func slotEdges(handles []*WalkHandle) (low, high uint64) {
	if len(handles) == 0 {
		return 0, 0
	}
	low = handles[0].Start
	high = handles[len(handles)-1].Stop
	return
}

func (m *TraversalSchedule) SlotEdges() (low, high uint64) {
	if len(m.schedule) == 0 {
		return 0, 0
	}
	low = m.firstNonEmptyDB().DBHandle().Start
	high = m.lastNonEmptyDB().handle.Stop
	return
}

func (schedule *TraversalSchedule) init(
	epoch uint64,
	start,
	stop uint64,
	requireFullEpoch bool,
	handles []*WalkHandle,
	shredRevision int,
	nextRevisionActivationSlot *uint64,
) error {
	schedule.shredRevision = shredRevision
	schedule.nextShredRevisionActivationSlot = cloneUint64Ptr(nextRevisionActivationSlot)

	reversedHandles := make([]*WalkHandle, len(handles))
	copy(reversedHandles, handles)
	reverse(reversedHandles)

	{
		for _, h := range reversedHandles {
			msg := fmt.Sprintf("db %q:", h.DB.DB.Name())
			{
				// NOTE: this is the lowest slot that is rooted, but it doesn't mean that it's the lowest slot that has meta and entries.
				slot, err := h.DB.MinRoot()
				if err != nil {
					msg += fmt.Sprintf(" MinRoot=error(%s)", err)
				} else {
					msg += fmt.Sprintf(" MinRoot=%d", slot)
				}
			}
			{
				slot, err := h.DB.MinMaybeRootedValidSlot()
				if err != nil {
					msg += fmt.Sprintf(" MinMaybeRootedValidSlot=error(%s)", err)
				} else {
					msg += fmt.Sprintf(" MinMaybeRootedValidSlot=%d", slot)
				}
			}
			{
				slot, err := h.DB.MaxRoot()
				if err != nil {
					msg += fmt.Sprintf(" MaxRoot=error(%s)", err)
				} else {
					msg += fmt.Sprintf(" MaxRoot=%d", slot)
				}
			}
			{
				slot, err := h.DB.MaxMaybeRootedValidSlot()
				if err != nil {
					msg += fmt.Sprintf(" MaxMaybeRootedValidSlot=error(%s)", err)
				} else {
					msg += fmt.Sprintf(" MaxMaybeRootedValidSlot=%d", slot)
				}
			}
			{
				shredSlot, shredIndex, _, err := h.DB.MinShred()
				if err != nil {
					msg += fmt.Sprintf(" MinShred=error(%s)", err)
				} else {
					msg += fmt.Sprintf(" MinShred=%d/%d", shredSlot, shredIndex)
				}
			}
			{
				shredSlot, shredIndex, _, err := h.DB.MaxShred()
				if err != nil {
					msg += fmt.Sprintf(" MaxShred=error(%s)", err)
				} else {
					msg += fmt.Sprintf(" MaxShred=%d/%d", shredSlot, shredIndex)
				}
			}
			klog.Info(msg)
		}
	}

	recoveries := make(map[uint64]struct{})
	defer func() {
		if len(recoveries) > 0 {
			klog.Infof("Recovered slot roots:")
			recoveredSorted := make([]uint64, 0, len(recoveries))
			for k := range recoveries {
				recoveredSorted = append(recoveredSorted, k)
			}
			sort.Slice(recoveredSorted, func(i, j int) bool {
				return recoveredSorted[i] < recoveredSorted[j]
			})
			for _, slot := range recoveredSorted {
				klog.Infof("  - %d", slot)
			}
		}
	}()

	activationSlot := cloneUint64Ptr(nextRevisionActivationSlot)
	var prevProcessedSlot *uint64
	// Let's start with the last DB, last slot.
	// We will work our way backwards from highest to lowest slot,
	// from most recent to oldest DB.
	wanted := stop // NOTE: this `stop` is a guess; if not found, we will get the next bigger slot
	for hi := range reversedHandles {
		handle := reversedHandles[hi]
		isLastDB := hi == len(reversedHandles)-1

		logErrorf := func(format string, args ...interface{}) {
			// format error and add context (db name)
			klog.Error(
				fmt.Errorf(format, args...),
				fmt.Sprintf(" current_db=%s", handle.DB.DB.Name()),
			)
		}
		logInfof := func(format string, args ...interface{}) {
			// format error and add context (db name)
			klog.Info(
				fmt.Sprintf(format, args...),
				fmt.Sprintf(" current_db=%s", handle.DB.DB.Name()),
			)
		}

		var lastOfPReviousDB *uint64
		if hi > 0 {
			prevSlots := schedule.schedule[hi-1].slots
			if len(prevSlots) > 0 {
				lastOfPReviousDB = &prevSlots[len(prevSlots)-1]
			}
		}
		klog.Infof(
			"Starting with db %s, wanted=%d, prevProcessed=%v; lastOfPreviousDB=%v;",
			handle.DB.DB.Name(),
			wanted,
			uint64OrNil(prevProcessedSlot),
			uint64OrNil(lastOfPReviousDB),
		)

		var slots []uint64
		opts := getReadOptions()
		defer putReadOptions(opts)
		// Open Next database
		iter := handle.DB.DB.NewIteratorCF(opts, handle.DB.CfRoot)
		defer iter.Close()

	slotLoop:
		for {
			key := MakeSlotKey(wanted)
			iter.Seek(key[:])
			var gotRoot uint64
			if iter.Valid() {
				got, ok := ParseSlotKey(iter.Key().Data())
				if !ok {
					return fmt.Errorf("found invalid slot key in DB %s: %x", handle.DB.DB.Name(), iter.Key().Data())
				}
				gotRoot = got
			}

			if !iter.Valid() || gotRoot != wanted {
				notMatch_none := !iter.Valid()                                // not found at all
				notMatch_butGotDifferent := iter.Valid() && gotRoot != wanted // found a different slot
				if notMatch_none {
					logErrorf(
						"seeked to slot %d but got invalid (not found, and there is no greater one either): will try to recover",
						wanted,
					)
				}
				if notMatch_butGotDifferent {
					logErrorf(
						"seeked to slot %d but got slot %d instead: will try to recover",
						wanted,
						gotRoot,
					)
				}
				{
					// The wanted root was not found in the CfRoot; check if we can recover it from the CfMeta and it has all the data.
					ok, err := canRecoverSlot(handle, wanted)
					if ok {
						logInfof("recovery successful: even though slot %d is not rooted, we were able to recover it because it has the meta and all data", wanted)
						if _, ok := recoveries[wanted]; ok {
							logInfof(
								"root slot already recovered: %d",
								wanted,
							)
						}
						recoveries[wanted] = struct{}{}
						gotRoot = wanted
					} else {
						logInfof("failed to recover slot %d: %s", wanted, err)
						if thisWasParentOfPrevious := prevProcessedSlot != nil; thisWasParentOfPrevious {
							// We really wanted that slot because it was the parent of the previous slot.
							// If we can't find it, we can't continue.
							if isLastDB {
								if !requireFullEpoch {
									break slotLoop
								}
								return fmt.Errorf(
									"db %q: failed to recover slot %d (parent of %d): %s",
									handle.DB.DB.Name(),
									wanted,
									*prevProcessedSlot,
									err,
								)
							} else {
								// We can't continue with this DB, but we can continue with the next DB.
								// We will try to recover the slot from the next DB.
								logInfof(
									"can't continue with this DB; will try to get slot %d from the next DB",
									wanted,
								)
								break slotLoop
							}
						}
						if wanted == stop {
							if notMatch_butGotDifferent {
								if gotRoot < wanted {
									// We wanted the last slot (the biggest slot in the epoch), but we got a smaller slot.
									// This means that we can't be sure the DB is complete.
									// We can't continue. You need to include another DB that contains the last slot of this epoch or greater.
									return fmt.Errorf(
										"db %q: failed to recover slot %d (last slot in epoch %d), and we can't continue because we can't be sure the DB is complete: %s",
										handle.DB.DB.Name(),
										wanted,
										epoch,
										err,
									)
								}
								if gotRoot > wanted {
									// Ok, we wanted the last slot (the biggest slot in the epoch), but we got a bigger slot.
									// This is OK, we can continue.
									goto weAreGoodWithWhatWeGot
								}
							}
							if notMatch_none {
								if isLastDB {
									// We wanted the last slot (the biggest slot in the epoch), but we got nothing at all.
									// This means that we can't be sure the DB is complete.
									// We can't continue. You need to include another DB that contains the last slot of this epoch or greater.
									return fmt.Errorf(
										"db %q: failed to recover slot %d (last slot in epoch %d), and we can't continue because we can't be sure the DB is complete: %s",
										handle.DB.DB.Name(),
										wanted,
										epoch,
										err,
									)
								} else {
									// We wanted the last slot (the biggest slot in the epoch), but we got nothing at all.
									// This means that we can't be sure the DB is complete.
									// We can't continue with this DB, but we can continue with the next DB.
									// We will try to recover the slot from the next DB.
									logInfof(
										"can't continue with this DB; will try to get slot %d from the next DB",
										wanted,
									)
									break slotLoop
								}
							}
						}
						panic("failed slot recovery; contact developer for this edge case")
					}
				}
			}
		weAreGoodWithWhatWeGot:

			if prevProcessedSlot != nil && gotRoot == *prevProcessedSlot {
				// slots = append(slots, wanted)
				{
					// go back one slot so we won't re-process this slot again.
					iter.Prev()
					var ok bool
					gotRoot, ok = ParseSlotKey(iter.Key().Data())
					if !ok {
						return fmt.Errorf("found invalid slot key in DB %s: %x", handle.DB.DB.Name(), iter.Key().Data())
					}
				}
			}

			if start != 0 && gotRoot < start-1 {
				logInfof(
					"reached slot %d which is lower than the low bound %d (OK)",
					gotRoot,
					start,
				)
			}

			// now get the meta for this slot
			meta, err := handle.DB.GetSlotMeta(gotRoot)
			if err != nil {
				return fmt.Errorf("db %q: failed to get meta for slot %d: %s", handle.DB.DB.Name(), gotRoot, err)
			}
			if meta == nil {
				return fmt.Errorf("db %q: meta for slot %d is nil", handle.DB.DB.Name(), gotRoot)
			}

			// If we have a shred revision, we need to check if the slot is in the range of the next revision activation.
			// If so, we need to increment the shred revision.
			if activationSlot != nil && gotRoot >= *activationSlot {
				shredRevision++
				activationSlot = nil
			}

			// fmt.Printf("slot=%d has %d entries; prev=%d\n", gotRoot, len(entries), meta.ParentSlot)
			if meta.ParentSlot == math.MaxUint64 {
				// Has all the data except for what's the parent slot.
				// We skip adding it here, and hope we will find it in the next DB.
				// We also assume that if meta.ParentSlot == math.MaxUint64, then this is the end of the DB.
				break slotLoop
			}
			slots = append(slots, gotRoot)
			if gotRoot == 0 {
				break slotLoop
			}
			// if gotRoot == meta.ParentSlot {
			// 	// TODO: is this correct?
			// 	break
			// }

			prevProcessedSlot = &gotRoot
			{
				// if prevProcessedSlot is already in the previous Epoch, we can stop.
				if prevProcessedSlot != nil && *prevProcessedSlot < start {
					break slotLoop
				}
			}
			wanted = meta.ParentSlot
		}

		Uint64Ascending(slots)

		if !uint64SlicesAreEqual(slots, unique(slots)) {
			panic(fmt.Errorf("slots from DB %s contains duplicates", handle.DB.DB.Name()))
		}

		schedule.schedule = append(schedule.schedule, DBtoSlots{
			handle: handle,
			slots:  unique(slots),
		})
		{
			// if prevProcessedSlot is already in the previous Epoch, we can stop.
			if prevProcessedSlot != nil && *prevProcessedSlot < start {
				break
			}
		}
	}
	// reverse the schedule so that it is in ascending order
	reverse(schedule.schedule)
	return nil
}

func canRecoverSlot(db *WalkHandle, slot uint64) (bool, error) {
	// can get meta, and can get entries
	meta, err := db.DB.GetSlotMeta(slot)
	if err != nil {
		return false, fmt.Errorf("db %q: failed to get meta for slot %d: %s", db.DB.DB.Name(), slot, err)
	}
	if meta == nil {
		return false, fmt.Errorf("db %q: meta for slot %d is nil", db.DB.DB.Name(), slot)
	}

	// Has all the data except for what's the parent slot.
	// We skip adding it here, and hope we will find it in the next DB.
	// We also assume that if meta.ParentSlot == math.MaxUint64, then this is the end of the DB.
	if meta.ParentSlot == math.MaxUint64 {
		return false, fmt.Errorf("db %q: meta for slot %d has invalid parent slot %d (=MaxUint64)", db.DB.DB.Name(), slot, meta.ParentSlot)
	}

	// If the slot was "recovered", let's check that we have all the data for it.
	// If we have missing data, we can't use it. Which means it might be the end of the DB.
	//
	// From pkg/blockstore/blockwalk_rocks.go:65-68:
	// The Solana validator periodically prunes old slots to keep database space bounded.
	// Therefore, the first (few) slots might have valid meta entries but missing data shreds.
	// To work around this, we simply start at the lowest meta and iterate until we find a complete entry.
	_, err = db.DB.GetEntries(meta, db.shredRevision)
	if err != nil {
		return false, fmt.Errorf("db %q: failed to get entries for slot %d: %s", db.DB.DB.Name(), slot, err)
	}
	return true, nil
}

func reverse[T any](x []T) {
	for i := len(x)/2 - 1; i >= 0; i-- {
		opp := len(x) - 1 - i
		x[i], x[opp] = x[opp], x[i]
	}
}

func greenBG(s string) string {
	return blackFG("\033[48;5;2m" + s + "\033[0m")
}

func blackFG(s string) string {
	return "\033[38;5;0m" + s + "\033[0m"
}

func uint64SlicesAreEqual(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i, x := range a {
		if b[i] != x {
			return false
		}
	}
	return true
}
