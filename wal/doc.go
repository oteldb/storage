// Package wal is the write-ahead log: CRC-framed records appended to numbered segment
// files, replayed in order to reconstruct the in-memory index after a crash. A record
// frames a length, a body (type + payload) and a CRC32C of the body; on replay a
// truncated or torn final record ends the log cleanly (crash recovery), while a complete
// record with a bad CRC is surfaced as corruption. Append-style and group-commit-friendly.
package wal
