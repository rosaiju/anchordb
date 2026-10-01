// Package manifest encodes the CURRENT file, which names the authoritative
// checkpoint and the first WAL segment recovery must replay. Replacing CURRENT
// atomically (temp file, fsync, rename, directory sync) is how a new
// checkpoint is published. Format: docs/architecture.md §4.4.
package manifest

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/rosaiju/anchordb/internal/checkpoint"
	"github.com/rosaiju/anchordb/internal/record"
	"github.com/rosaiju/anchordb/internal/vfs"
)

// FileName is the manifest's name inside the database directory.
const FileName = "CURRENT"

const header = "ANCHORDB-CURRENT 1"

// Current is the decoded manifest. Checkpoint is "" when there is none.
type Current struct {
	Checkpoint     string
	CheckpointTxID uint64
	WALStart       uint64
}

// Encode renders c as text with a trailing CRC line.
func Encode(c Current) []byte {
	name := c.Checkpoint
	if name == "" {
		name = "none"
	}
	body := fmt.Sprintf("%s\ncheckpoint %s\ncheckpoint_txid %d\nwal_start %d\n",
		header, name, c.CheckpointTxID, c.WALStart)
	return []byte(fmt.Sprintf("%scrc32c %08x\n", body, record.Checksum([]byte(body))))
}

// Decode parses and validates a manifest. It is strict: the exact layout
// produced by Encode is the only accepted input.
func Decode(b []byte) (Current, error) {
	if len(b) > 4096 {
		return Current{}, fmt.Errorf("manifest too large (%d bytes)", len(b))
	}
	idx := bytes.LastIndex(b, []byte("crc32c "))
	if idx < 0 {
		return Current{}, fmt.Errorf("manifest has no checksum line")
	}
	body, crcLine := b[:idx], string(b[idx:])
	if len(crcLine) != len("crc32c 00000000\n") || crcLine[len(crcLine)-1] != '\n' {
		return Current{}, fmt.Errorf("malformed checksum line")
	}
	want, err := strconv.ParseUint(crcLine[7:15], 16, 32)
	if err != nil || crcLine[7:15] != strings.ToLower(crcLine[7:15]) {
		return Current{}, fmt.Errorf("malformed checksum %q", crcLine[7:15])
	}
	if uint32(want) != record.Checksum(body) {
		return Current{}, fmt.Errorf("manifest checksum mismatch")
	}
	lines := strings.Split(string(body), "\n")
	if len(lines) != 5 || lines[4] != "" || lines[0] != header {
		return Current{}, fmt.Errorf("malformed manifest")
	}
	field := func(line, key string) (string, error) {
		v, ok := strings.CutPrefix(line, key+" ")
		if !ok || v == "" {
			return "", fmt.Errorf("expected %q line", key)
		}
		return v, nil
	}
	var c Current
	name, err := field(lines[1], "checkpoint")
	if err != nil {
		return Current{}, err
	}
	txs, err := field(lines[2], "checkpoint_txid")
	if err != nil {
		return Current{}, err
	}
	ws, err := field(lines[3], "wal_start")
	if err != nil {
		return Current{}, err
	}
	if c.CheckpointTxID, err = strconv.ParseUint(txs, 10, 64); err != nil {
		return Current{}, fmt.Errorf("bad checkpoint_txid: %v", err)
	}
	if c.WALStart, err = strconv.ParseUint(ws, 10, 64); err != nil || c.WALStart == 0 {
		return Current{}, fmt.Errorf("bad wal_start %q", ws)
	}
	if name == "none" {
		if c.CheckpointTxID != 0 {
			return Current{}, fmt.Errorf("checkpoint none with non-zero txid")
		}
	} else {
		txid, ok := checkpoint.ParseName(name)
		if !ok || txid != c.CheckpointTxID {
			return Current{}, fmt.Errorf("checkpoint name %q does not match txid %d", name, c.CheckpointTxID)
		}
		c.Checkpoint = name
	}
	// Canonical form only: rejects leading zeros, extra whitespace, etc.
	if !bytes.Equal(Encode(c), b) {
		return Current{}, fmt.Errorf("manifest is not in canonical form")
	}
	return c, nil
}

// Stage names points inside Publish, for crash hooks.
type Stage int

const (
	StageTmpSynced Stage = iota // CURRENT.tmp written and fsynced, not yet renamed
	StageRenamed                // renamed over CURRENT, directory not yet synced
)

// Publish atomically replaces CURRENT with c. It reports whether the rename
// was attempted: after that point either manifest may be the durable one.
// hook may be nil.
func Publish(fsys vfs.FS, dir string, c Current, hook func(Stage)) (renameAttempted bool, err error) {
	final := filepath.Join(dir, FileName)
	tmp := final + ".tmp"
	f, err := fsys.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return false, err
	}
	if _, err = f.Write(Encode(c)); err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		fsys.Remove(tmp)
		return false, err
	}
	if hook != nil {
		hook(StageTmpSynced)
	}
	if err := fsys.Rename(tmp, final); err != nil {
		return true, err
	}
	if hook != nil {
		hook(StageRenamed)
	}
	return true, fsys.SyncDir(dir)
}
