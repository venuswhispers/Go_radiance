// Package loader implements an ELF loader for the Sealevel virtual machine.
//
// Based on https://docs.rs/solana_rbpf/latest/solana_rbpf/elf_parser/index.html
package loader

import (
	"bufio"
	"bytes"
	"debug/elf"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/bits"
	"strings"
)

// TODO Fuzz
// TODO Differential fuzz against rbpf

const EF_SBF_V2 = 0x20

const DT_NUM = 35

// Bounds checks
const (
	// 64 MiB max program size.
	// Allows loader to use unchecked math when adding 32-bit offsets.
	maxFileLen = 1 << 26

	maxSectionNameLen = 16
	maxSymbolNameLen  = 1024
)

// loader is based on solana_rbpf::elf_parser
type loader struct {
	rd       io.ReaderAt
	fileSize uint64

	eh         elf.Header64
	phLoad     elf.Prog64
	phDynamic  *elf.Prog64
	shShstrtab elf.Section64
	shSymtab   *elf.Section64
	shStrtab   *elf.Section64
	shDynstr   *elf.Section64
	shDynamic  *elf.Section64
	dynamic    [DT_NUM]uint64
	relocsIter *tableIter[elf.Rel64]
	dynSymIter *tableIter[elf.Sym64]
}

func newLoader(buf []byte) (*loader, error) {
	if len(buf) > maxFileLen {
		return nil, fmt.Errorf("ELF file too large")
	}
	l := &loader{
		rd:       bytes.NewReader(buf),
		fileSize: uint64(len(buf)),
	}
	return l, nil
}

// parse checks ELF file for validity and loads metadata with minimal allocations.
func (l *loader) parse() error {
	if err := l.readHeader(); err != nil {
		return err
	}
	if err := l.validateHeader(); err != nil {
		return err
	}
	if err := l.loadProgramHeaderTable(); err != nil {
		return err
	}
	if err := l.readSectionHeaderTable(); err != nil {
		return err
	}
	if err := l.parseSections(); err != nil {
		return err
	}
	if err := l.parseDynamic(); err != nil {
		return err
	}
	return nil
}

const (
	ehLen    = 0x40 // sizeof(elf.Header64)
	phEntLen = 0x38 // sizeof(elf.Prog64)
	shEntLen = 0x40 // sizeof(elf.Section64)
	dynLen   = 0x10 // sizeof(elf.Dyn64)
	relLen   = 0x10 // sizeof(elf.Rel64)
	symLen   = 0x18 // sizeof(elf.Sym64)
)

func (l *loader) newPhTableIter() *tableIter[elf.Prog64] {
	eh := &l.eh
	return newTableIterator[elf.Prog64](l, eh.Phoff, uint32(eh.Phnum), phEntLen)
}

func (l *loader) newShTableIter() *tableIter[elf.Section64] {
	eh := &l.eh
	return newTableIterator[elf.Section64](l, eh.Shoff, uint32(eh.Shnum), shEntLen)
}

func (l *loader) readHeader() error {
	var hdrBuf [ehLen]byte
	if _, err := io.ReadFull(io.NewSectionReader(l.rd, 0, ehLen), hdrBuf[:]); err != nil {
		return err
	}
	return binary.Read(bytes.NewReader(hdrBuf[:]), binary.LittleEndian, &l.eh)
}

func (l *loader) validateHeader() error {
	eh := &l.eh
	ident := &eh.Ident

	if string(ident[:elf.EI_CLASS]) != elf.ELFMAG {
		return fmt.Errorf("not an ELF file")
	}

	if elf.Class(ident[elf.EI_CLASS]) != elf.ELFCLASS64 ||
		elf.Data(ident[elf.EI_DATA]) != elf.ELFDATA2LSB ||
		elf.Version(ident[elf.EI_VERSION]) != elf.EV_CURRENT ||
		elf.OSABI(ident[elf.EI_OSABI]) != elf.ELFOSABI_NONE ||
		elf.Machine(eh.Machine) != elf.EM_BPF ||
		elf.Type(eh.Type) != elf.ET_DYN {
		return fmt.Errorf("incompatible binary")
	}
	// note: EI_PAD and EI_ABIVERSION are ignored

	if eh.Version != uint32(elf.EV_CURRENT) ||
		eh.Ehsize != ehLen ||
		eh.Phentsize != phEntLen ||
		eh.Shentsize != shEntLen ||
		eh.Shstrndx >= eh.Shnum {
		return fmt.Errorf("invalid ELF file")
	}

	if eh.Phoff < ehLen {
		return fmt.Errorf("program header overlaps with file header")
	}
	if eh.Shoff < ehLen {
		return fmt.Errorf("section header overlaps with file header")
	}
	if isOverlap(eh.Phoff, uint64(eh.Phnum)*phEntLen, eh.Shoff, uint64(eh.Shnum)*shEntLen) {
		return fmt.Errorf("program and section header overlap")
	}

	return nil
}

// scan the program header table and remember the last PT_LOAD segment
func (l *loader) loadProgramHeaderTable() error {
	iter := l.newPhTableIter()
	for iter.Next() && iter.Err() == nil {
		ph := iter.Item()

		switch elf.ProgType(ph.Type) {
		case elf.PT_DYNAMIC:
			// remember first segment with PT_DYNAMIC in case we need it later
			if l.phDynamic == nil {
				l.phDynamic = new(elf.Prog64)
				*l.phDynamic = ph
			}
			continue
		case elf.PT_LOAD:
			break
		default:
			continue
		}

		// vaddr must be ascending
		if ph.Vaddr < l.phLoad.Vaddr {
			return fmt.Errorf("invalid program header")
		}

		segmentEnd, overflow := bits.Add64(ph.Off, ph.Filesz, 0)
		if segmentEnd > l.fileSize || overflow > 0 {
			return fmt.Errorf("segment out of bounds")
		}

		l.phLoad = ph
	}
	return iter.Err()
}

// reads and validates the section header table.
// remembers the section header table.
func (l *loader) readSectionHeaderTable() error {
	eh := &l.eh
	iter := l.newShTableIter()
	sectionDataOff := uint64(0)

	if !iter.Next() {
		return fmt.Errorf("missing section 0")
	}
	if elf.SectionType(iter.Item().Type) != elf.SHT_NULL {
		return fmt.Errorf("section 0 is not SHT_NULL")
	}

	for iter.Next() && iter.Err() == nil {
		i, sh := iter.Index(), iter.Item()
		switch elf.SectionType(sh.Type) {
		case elf.SHT_NOBITS:
			continue
		case elf.SHT_DYNAMIC:
			// remember first section with SHT_DYNAMIC in case we need it later
			if l.shDynamic == nil {
				l.shDynamic = new(elf.Section64)
				*l.shDynamic = sh
			}
		default:
			break
		}

		// Ensure section data is not overlapping with ELF headers
		shend, overflow := bits.Add64(sh.Off, sh.Size, 0)
		if overflow != 0 {
			return fmt.Errorf("integer overflow in section %d", i)
		}
		if sh.Off < ehLen {
			return fmt.Errorf("section %d overlaps with file header", i)
		}
		if isOverlap(eh.Phoff, uint64(eh.Phnum)*phEntLen, sh.Off, sh.Size) {
			return fmt.Errorf("section %d overlaps with program header", i)
		}
		if isOverlap(eh.Shoff, uint64(eh.Shnum)*shEntLen, sh.Off, sh.Size) {
			return fmt.Errorf("section %d overlaps with section header", i)
		}

		// More checks
		if eh.Shoff < sectionDataOff {
			return fmt.Errorf("sections not in order")
		}
		if shend > l.fileSize {
			return fmt.Errorf("section %d out of bounds", i)
		}

		// Remember section header string table.
		if eh.Shstrndx != uint16(elf.SHN_UNDEF) && uint32(eh.Shstrndx) == i {
			l.shShstrtab = sh
		}

		sectionDataOff = shend
	}
	// TODO validate offset and size (?)
	if elf.SectionType(l.shShstrtab.Type) != elf.SHT_STRTAB {
		return fmt.Errorf("invalid .shstrtab")
	}
	return iter.Err()
}

func (l *loader) getString(strtab *elf.Section64, stroff uint32, maxLen uint16) (string, error) {
	if elf.SectionType(strtab.Type) != elf.SHT_STRTAB {
		return "", fmt.Errorf("invalid strtab")
	}
	offset := strtab.Off + uint64(stroff)
	if offset > l.fileSize || offset+uint64(maxLen) > l.fileSize {
		return "", io.ErrUnexpectedEOF
	}
	rd := bufio.NewReader(io.NewSectionReader(l.rd, int64(offset), int64(maxLen)))
	var builder strings.Builder
	for {
		b, err := rd.ReadByte()
		if err != nil {
			return "", err
		}
		if b == 0 {
			break
		}
		builder.WriteByte(b)
	}
	return builder.String(), nil
}

// Iterate sections and remember special sections by name.
func (l *loader) parseSections() error {
	shShstrtab := &l.shShstrtab
	iter := l.newShTableIter()
	for iter.Next() && iter.Err() == nil {
		sh := iter.Item()
		sectionName, err := l.getString(shShstrtab, sh.Name, maxSectionNameLen)
		if err != nil {
			return fmt.Errorf("getString: %w", err)
		}

		// Remember special section or error if it already exists.
		setSection := func(shPtr **elf.Section64) error {
			if *shPtr != nil {
				return fmt.Errorf("duplicate section: %s", sectionName)
			}
			*shPtr = new(elf.Section64)
			**shPtr = sh
			return nil
		}
		switch sectionName {
		case ".symtab":
			err = setSection(&l.shSymtab)
		case ".strtab":
			err = setSection(&l.shStrtab)
		case ".dynstr":
			err = setSection(&l.shDynstr)
		}
		if err != nil {
			return err
		}
	}
	return iter.Err()
}

func (l *loader) newDynamicIter() (*tableIter[elf.Dyn64], error) {
	var off uint64
	var size uint64
	if ph := l.phDynamic; ph != nil {
		off, size = ph.Off, ph.Filesz
	} else if sh := l.shDynamic; sh != nil {
		off, size = sh.Off, sh.Size
	} else {
		return nil, nil
	}

	if size%dynLen != 0 {
		return nil, fmt.Errorf("odd .dynamic size")
	}
	if (off+size) > l.fileSize || (off+size) < off {
		return nil, io.ErrUnexpectedEOF
	}

	iter := newTableIterator[elf.Dyn64](l, off, uint32(off/dynLen), dynLen)
	return iter, nil
}

func (l *loader) parseDynamicTable() error {
	iter, err := l.newDynamicIter()
	if err != nil {
		return err
	}
	if iter == nil {
		// static file, nothing to do
		return nil
	}

	for iter.Next() && iter.Err() == nil {
		dyn := iter.Item()
		if dyn.Tag == int64(elf.DT_NULL) {
			break
		}
		if dyn.Tag >= int64(len(l.dynamic)) {
			continue
		}
		l.dynamic[dyn.Tag] = dyn.Val
	}
	return iter.Err()
}

// sectionAt finds the section that has a start address matching vaddr.
func (l *loader) sectionAt(vaddr uint64) (*elf.Section64, error) {
	iter := l.newShTableIter()
	for iter.Next() && iter.Err() == nil {
		sh := iter.Item()
		if sh.Addr == vaddr {
			return &sh, nil
		}
	}
	return nil, iter.Err()
}

// segmentByVaddr finds the segment which vaddr lies within.
func (l *loader) segmentByVaddr(vaddr uint64) (*elf.Prog64, error) {
	iter := l.newPhTableIter()
	for iter.Next() && iter.Err() == nil {
		ph := iter.Item()
		if ph.Vaddr+ph.Memsz < ph.Vaddr {
			return nil, fmt.Errorf("segment ends past math.MaxUint64")
		}
		if ph.Vaddr <= vaddr && vaddr < ph.Vaddr+ph.Memsz {
			return &ph, nil
		}
	}
	return nil, iter.Err()
}

func (l *loader) parseRelocs() error {
	vaddr := l.dynamic[elf.DT_REL]
	if vaddr == 0 {
		return nil
	}
	if l.dynamic[elf.DT_RELENT] != relLen {
		return fmt.Errorf("invalid DT_RELENT")
	}
	size := l.dynamic[elf.DT_RELSZ]
	if size == 0 || size%relLen != 0 || size > math.MaxUint32 {
		return fmt.Errorf("invalid DT_RELSZ")
	}
	ph, err := l.segmentByVaddr(vaddr)
	if err != nil {
		return err
	}
	offset := vaddr
	if ph != nil {
		var overflow uint64
		offset, overflow = bits.Sub64(offset, ph.Vaddr, 0)
		if overflow != 0 {
			return fmt.Errorf("offset underflow")
		}
		offset, overflow = bits.Add64(offset, ph.Vaddr, 0)
		if overflow != 0 {
			return fmt.Errorf("offset overflow")
		}
	} else {
		// Handle invalid dynamic sections where DT_REL is not in any program segment.
		sh, err := l.sectionAt(vaddr)
		if err != nil {
			return err
		}
		if sh == nil {
			return fmt.Errorf("cannot find physical address of relocation table")
		}
		offset = sh.Off
	}
	l.relocsIter, err = newTableIteratorChecked[elf.Rel64](l, offset, offset+size, relLen)
	return err
}

// getSymtab returns an iterator over the symbols in a symtab-like section.
//
// Performs necessary bounds checking.
func (l *loader) getSymtab(sh *elf.Section64) (*tableIter[elf.Sym64], error) {
	switch elf.SectionType(sh.Type) {
	case elf.SHT_SYMTAB, elf.SHT_DYNSYM:
		break
	default:
		return nil, fmt.Errorf("not a symtab section")
	}
	return newTableIteratorChecked[elf.Sym64](l, sh.Off, sh.Off+sh.Size, symLen)
}

func (l *loader) parseDynSymtab() error {
	vaddr := l.dynamic[elf.DT_SYMTAB]
	if vaddr == 0 {
		return nil
	}

	dynsym, err := l.sectionAt(vaddr)
	if err != nil {
		return err
	}
	if dynsym == nil {
		return fmt.Errorf("cannot find DT_SYMTAB section")
	}

	l.dynSymIter, err = l.getSymtab(dynsym)
	return err
}

func (l *loader) parseDynamic() error {
	if err := l.parseDynamicTable(); err != nil {
		return err
	}
	if err := l.parseRelocs(); err != nil {
		return err
	}
	if err := l.parseDynSymtab(); err != nil {
		return err
	}
	return nil
}

// tableIter is a memory-efficient iterator over densely packed tables of statically sized items.
// Such as the ELF program header and section header tables.
type tableIter[T any] struct {
	l        *loader
	off      uint64
	i        uint32 // one ahead
	count    uint32
	elemSize uint16
	elem     T
	err      error
}

// newTableIteratorChecked is like newTableIterator, but with all necessary bounds checks.
func newTableIteratorChecked[T any](l *loader, start uint64, end uint64, elemSize uint16) (*tableIter[T], error) {
	if end < start || end > l.fileSize {
		return nil, io.ErrUnexpectedEOF
	}
	size := end - start
	if size%uint64(elemSize) != 0 {
		return nil, fmt.Errorf("misaligned table")
	}
	if size > math.MaxInt32 {
		return nil, io.ErrUnexpectedEOF
	}
	iter := newTableIterator[T](l, start, uint32(size/uint64(elemSize)), elemSize)
	return iter, nil
}

// newTableIterator creates a new tableIter at `off` for `count` elements of `elemSize` len.
func newTableIterator[T any](l *loader, off uint64, count uint32, elemSize uint16) *tableIter[T] {
	return &tableIter[T]{
		l:        l,
		off:      off,
		count:    count,
		elemSize: elemSize,
	}
}

// Next reads one element.
//
// Returns true on success, false if table end has been reached or error occurred.
// The caller should abort iteration on error.
func (it *tableIter[T]) Next() (ok bool) {
	ok, it.err = it.getNext()
	if ok && it.err != nil {
		panic("unreachable")
	}
	return
}

// Index returns the current table index.
func (it *tableIter[T]) Index() uint32 {
	return it.i - 1
}

// Err returns the current error.
func (it *tableIter[T]) Err() error {
	return it.err
}

// Item returns the current element read.
//
// Next must be called before.
func (it *tableIter[T]) Item() T {
	return it.elem
}

func (it *tableIter[T]) getNext() (bool, error) {
	if it.i >= it.count {
		return false, nil
	}
	if it.off >= math.MaxInt64 || it.off+uint64(it.elemSize) > math.MaxInt64 {
		return false, io.ErrUnexpectedEOF
	}

	rd := io.NewSectionReader(it.l.rd, int64(it.off), int64(it.elemSize))
	if err := binary.Read(rd, binary.LittleEndian, &it.elem); err != nil {
		return false, err
	}

	it.off += uint64(it.elemSize)
	it.i++
	return true, nil
}

func isOverlap(startA uint64, sizeA uint64, startB uint64, sizeB uint64) bool {
	if startA > startB {
		startA, sizeA, startB, sizeB = startB, sizeB, startA, sizeA
	}
	endA, endB := startA+sizeA, startB+sizeB
	if endA < startA || endB < startB {
		panic("isOverlap: integer overflow")
	}
	return sizeA != 0 && sizeB != 0 && (startA == startB || endA > endB)
}
