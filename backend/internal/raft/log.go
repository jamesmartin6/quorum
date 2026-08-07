package raft

// Log is the replicated command log. Entries are 1-indexed to match the
// Raft paper; index 0 is a sentinel "no entry" position used as the
// implicit prevLogIndex/prevLogTerm base before anything has been appended.
type Log struct {
	entries []LogEntry // entries[0] is index 1
}

// NewLog builds a Log from persisted entries (may be empty/nil).
func NewLog(entries []LogEntry) *Log {
	l := &Log{entries: make([]LogEntry, len(entries))}
	copy(l.entries, entries)
	return l
}

// Len returns the number of entries in the log (not counting the sentinel).
func (l *Log) Len() int {
	return len(l.entries)
}

// LastIndex returns the index of the last entry, or 0 if the log is empty.
func (l *Log) LastIndex() uint64 {
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Index
}

// LastTerm returns the term of the last entry, or 0 if the log is empty.
func (l *Log) LastTerm() uint64 {
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Term
}

// TermAt returns the term of the entry at the given index, and whether it
// exists. Index 0 always "exists" with term 0 (the sentinel base).
func (l *Log) TermAt(index uint64) (uint64, bool) {
	if index == 0 {
		return 0, true
	}
	if index < 1 || index > uint64(len(l.entries)) {
		return 0, false
	}
	return l.entries[index-1].Term, true
}

// Get returns the entry at the given 1-based index, and whether it exists.
func (l *Log) Get(index uint64) (LogEntry, bool) {
	if index < 1 || index > uint64(len(l.entries)) {
		return LogEntry{}, false
	}
	return l.entries[index-1], true
}

// Slice returns entries in [from, LastIndex], inclusive, 1-indexed. Returns
// nil if from is past the end of the log.
func (l *Log) Slice(from uint64) []LogEntry {
	if from < 1 {
		from = 1
	}
	if from > uint64(len(l.entries)) {
		return nil
	}
	out := make([]LogEntry, uint64(len(l.entries))-from+1)
	copy(out, l.entries[from-1:])
	return out
}

// Append adds entries to the end of the log, assigning sequential indices
// and the given term. Returns the index of the first appended entry.
func (l *Log) Append(term uint64, cmds ...LogEntry) uint64 {
	start := l.LastIndex() + 1
	idx := start
	for i := range cmds {
		cmds[i].Index = idx
		cmds[i].Term = term
		idx++
	}
	l.entries = append(l.entries, cmds...)
	return start
}

// TruncateFrom removes all entries from the given index (inclusive) to the
// end, used when a follower's log conflicts with the leader's.
func (l *Log) TruncateFrom(index uint64) {
	if index < 1 {
		l.entries = l.entries[:0]
		return
	}
	if index > uint64(len(l.entries)) {
		return
	}
	l.entries = l.entries[:index-1]
}

// All returns a copy of every entry, for persistence or debugging.
func (l *Log) All() []LogEntry {
	out := make([]LogEntry, len(l.entries))
	copy(out, l.entries)
	return out
}

// AppendRaw appends entries verbatim (their Index/Term are already set),
// used by a follower copying entries sent by the leader.
func (l *Log) AppendRaw(entries []LogEntry) {
	l.entries = append(l.entries, entries...)
}

// FirstIndexOfTerm returns the lowest index whose entry has the given
// term, or 0 if no entry has that term. Used by the follower to tell a
// leader where a conflicting term began, for fast nextIndex backoff.
func (l *Log) FirstIndexOfTerm(term uint64) uint64 {
	for _, e := range l.entries {
		if e.Term == term {
			return e.Index
		}
	}
	return 0
}

// LastIndexOfTerm returns the highest index whose entry has the given
// term, or 0 if no entry has that term. Used by the leader for fast
// nextIndex backoff after a conflicting AppendEntries reply.
func (l *Log) LastIndexOfTerm(term uint64) uint64 {
	last := uint64(0)
	for _, e := range l.entries {
		if e.Term == term {
			last = e.Index
		}
	}
	return last
}
