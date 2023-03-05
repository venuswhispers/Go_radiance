package sbpf

import (
	"encoding/binary"

	"github.com/spaolacci/murmur3"
)

const (
	// EntrypointHash equals SymbolHash("entrypoint")
	EntrypointHash = uint32(0x71e3cf81)
)

// SymbolHash returns the murmur3 32-bit hash of a symbol name.
func SymbolHash(s string) uint32 {
	return murmur3.Sum32([]byte(s))
}

// PCHash returns the murmur3 32-bit hash of a program counter.
//
// Used by VM for non-syscall functions
func PCHash(addr uint64) uint32 {
	// TODO this is kinda pointless …
	var key [8]byte
	binary.LittleEndian.PutUint64(key[:], addr)
	return murmur3.Sum32(key[:])
}

// Syscall are callback handles from VM to Go. (work in progress)
type Syscall interface {
	Invoke(vm VM, r1, r2, r3, r4, r5 uint64, cuIn int) (r0 uint64, cuOut int, err error)
}

type SyscallRegistry map[uint32]Syscall

func NewSyscallRegistry() SyscallRegistry {
	return make(SyscallRegistry)
}

func (s SyscallRegistry) Register(name string, syscall Syscall) (hash uint32, ok bool) {
	hash = SymbolHash(name)
	if _, exist := s[hash]; exist {
		return 0, false // collision or duplicate
	}
	s[hash] = syscall
	ok = true
	return
}

// Convenience Methods

type SyscallFunc0 func(vm VM, cuIn int) (r0 uint64, cuOut int, err error)

func (f SyscallFunc0) Invoke(vm VM, _, _, _, _, _ uint64, cuIn int) (r0 uint64, cuOut int, err error) {
	return f(vm, cuIn)
}

type SyscallFunc1 func(vm VM, r1 uint64, cuIn int) (r0 uint64, cuOut int, err error)

func (f SyscallFunc1) Invoke(vm VM, r1, _, _, _, _ uint64, cuIn int) (r0 uint64, cuOut int, err error) {
	return f(vm, r1, cuIn)
}

type SyscallFunc2 func(vm VM, r1, r2 uint64, cuIn int) (r0 uint64, cuOut int, err error)

func (f SyscallFunc2) Invoke(vm VM, r1, r2, _, _, _ uint64, cuIn int) (r0 uint64, cuOut int, err error) {
	return f(vm, r1, r2, cuIn)
}

type SyscallFunc3 func(vm VM, r1, r2, r3 uint64, cuIn int) (r0 uint64, cuOut int, err error)

func (f SyscallFunc3) Invoke(vm VM, r1, r2, r3, _, _ uint64, cuIn int) (r0 uint64, cuOut int, err error) {
	return f(vm, r1, r2, r3, cuIn)
}

type SyscallFunc4 func(vm VM, r1, r2, r3, r4 uint64, cuIn int) (r0 uint64, cuOut int, err error)

func (f SyscallFunc4) Invoke(vm VM, r1, r2, r3, r4, _ uint64, cuIn int) (r0 uint64, cuOut int, err error) {
	return f(vm, r1, r2, r3, r4, cuIn)
}

type SyscallFunc5 func(vm VM, r1, r2, r3, r4, r5 uint64, cuIn int) (r0 uint64, cuOut int, err error)

func (f SyscallFunc5) Invoke(vm VM, r1, r2, r3, r4, r5 uint64, cuIn int) (r0 uint64, cuOut int, err error) {
	return f(vm, r1, r2, r3, r4, r5, cuIn)
}
