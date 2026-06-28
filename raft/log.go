package raft

import "fmt"

type LogEntry struct {
	Term    uint64
	Index   uint64
	Command []byte
}

type Log struct {
	entries []LogEntry
}

func NewLog() *Log {
	return &Log{
		entries: []LogEntry{{Term: 0, Index: 0}},
	}
}

func (l *Log) LastIndex() uint64 {
	return uint64(len(l.entries) - 1)
}

func (l *Log) LastTerm() uint64 {
	return l.entries[len(l.entries)-1].Term
}

func (l *Log) Entry(index uint64) (LogEntry, error) {
	if index >= uint64(len(l.entries)) {
		return LogEntry{}, fmt.Errorf("index %d out of range", index)
	}
	return l.entries[index], nil
}

func (l *Log) Append(prevIndex uint64, entries []LogEntry) {
	l.entries = l.entries[:prevIndex+1]
	l.entries = append(l.entries, entries...)
}

func (l *Log) Slice(lo, hi uint64) []LogEntry {
	if lo >= hi || lo >= uint64(len(l.entries)) {
		return nil
	}
	if hi > uint64(len(l.entries)) {
		hi = uint64(len(l.entries))
	}
	return l.entries[lo:hi]
}
