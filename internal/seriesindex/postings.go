package seriesindex

import (
	"bytes"
	"context"
	"encoding/binary"
	"math/bits"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/index/postings"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// Resolve intersects typed-label predicates, yielding sorted part-local references.
func (r *Index) Resolve(ctx context.Context, matchers []fetch.Matcher) (postings.Postings, error) {
	if len(matchers) == 0 {
		return &refRange{n: r.count, at: -1}, nil
	}
	its := make([]postings.Postings, 0, len(matchers))
	for _, m := range matchers {
		if m.Match == nil {
			return nil, errors.New("seriesindex: nil matcher")
		}
		p, err := r.selectMatcher(ctx, m)
		if err != nil {
			return nil, err
		}
		its = append(its, p)
	}
	return postings.Intersect(its...), nil
}

func (r *Index) selectMatcher(ctx context.Context, m fetch.Matcher) (postings.Postings, error) {
	absent := m.Match(signal.EmptyValue())
	key := appendEscaped([]byte{'n'}, m.Name)
	c := r.table.Cursor(ctx)
	if !c.Seek(key) {
		if c.Err() != nil {
			return nil, c.Err()
		}
		if absent {
			return &refRange{n: r.count, at: -1}, nil
		}
		return postings.Empty(), nil
	}
	if !bytes.Equal(c.Key(), key) {
		if absent {
			return &refRange{n: r.count, at: -1}, nil
		}
		return postings.Empty(), nil
	}
	if len(c.Value()) != 1 || c.Value()[0] > 1 {
		return nil, errors.Wrap(ErrCorrupt, "label kind flags")
	}
	nonString := c.Value()[0] != 0
	selected := make([]uint64, (uint64(r.count)+63)/64)
	if absent {
		for i := range selected {
			selected[i] = ^uint64(0)
		}
		if err := r.markPrefix(ctx, appendEscaped([]byte{'h'}, m.Name), selected, false); err != nil {
			return nil, err
		}
	}
	values := m.StringValues
	if m.Spec != nil {
		values = [][]byte{[]byte(m.Spec.Value)}
	}
	if !nonString && len(values) > 0 {
		for _, value := range values {
			v := signal.StringValue(value)
			if !m.Match(v) {
				continue
			}
			prefix := appendEscaped([]byte{'p'}, m.Name)
			prefix = appendEscaped(prefix, signal.AppendValue(nil, v))
			if err := r.markPrefix(ctx, prefix, selected, true); err != nil {
				return nil, err
			}
		}
	} else if err := r.markValues(ctx, m, selected); err != nil {
		return nil, err
	}
	return &refBitmap{words: selected, n: r.count, at: -1}, nil
}

func (r *Index) markPrefix(ctx context.Context, prefix []byte, selected []uint64, set bool) error {
	c := r.table.Cursor(ctx)
	for ok := c.Seek(prefix); ok && bytes.HasPrefix(c.Key(), prefix); ok = c.Next() {
		if len(c.Key()) != len(prefix)+4 {
			return errors.Wrap(ErrCorrupt, "postings key")
		}
		if err := r.mark(c.Value(), binary.BigEndian.Uint32(c.Key()[len(prefix):]), selected, set); err != nil {
			return err
		}
	}
	return c.Err()
}

func prefixEnd(prefix []byte) []byte {
	end := bytes.Clone(prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 255 {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

func (r *Index) markValues(ctx context.Context, m fetch.Matcher, selected []uint64) error {
	prefix := appendEscaped([]byte{'p'}, m.Name)
	c := r.table.Cursor(ctx)
	ok := c.Seek(prefix)
	var encoded, group []byte
	for ok && bytes.HasPrefix(c.Key(), prefix) {
		key := c.Key()
		var err error
		var consumed int
		encoded, consumed, err = decodeEscaped(encoded[:0], key[len(prefix):])
		if err != nil {
			return err
		}
		if len(key) != len(prefix)+consumed+4 {
			return errors.Wrap(ErrCorrupt, "typed postings key")
		}
		value, used, err := signal.DecodeValue(encoded)
		if err != nil || used != len(encoded) {
			return errors.Wrap(ErrCorrupt, "typed label")
		}
		group = append(group[:0], key[:len(key)-4]...)
		if !m.Match(value) {
			ok = c.Seek(prefixEnd(group))
			continue
		}
		for ok && bytes.HasPrefix(c.Key(), group) {
			if len(c.Key()) != len(group)+4 {
				return errors.Wrap(ErrCorrupt, "postings chunk key")
			}
			if err := r.mark(c.Value(), binary.BigEndian.Uint32(c.Key()[len(group):]), selected, true); err != nil {
				return err
			}
			ok = c.Next()
		}
	}
	return c.Err()
}

func (r *Index) mark(data []byte, last uint32, selected []uint64, set bool) error {
	n, size := binary.Uvarint(data)
	if size <= 0 || n == 0 || n > postingChunk {
		return errors.Wrap(ErrCorrupt, "postings count")
	}
	data = data[size:]
	ref := uint64(0)
	for i := range n {
		delta, k := binary.Uvarint(data)
		if k <= 0 || (i > 0 && delta == 0) || delta > uint64(r.count) || ref+delta >= uint64(r.count) {
			return errors.Wrap(ErrCorrupt, "postings reference")
		}
		ref += delta
		data = data[k:]
		mask := uint64(1) << (ref & 63)
		if set {
			selected[ref>>6] |= mask
		} else {
			selected[ref>>6] &^= mask
		}
	}
	if len(data) != 0 || ref != uint64(last) {
		return errors.Wrap(ErrCorrupt, "postings last reference")
	}
	return nil
}

type refRange struct {
	n  uint32
	at int64
}

func (p *refRange) Next() bool { p.at++; return p.at < int64(p.n) }
func (p *refRange) Seek(id signal.SeriesID) bool {
	if id.Hi != 0 || id.Lo >= uint64(p.n) {
		p.at = int64(p.n)
		return false
	}
	p.at = max(p.at, int64(id.Lo))
	return p.at < int64(p.n)
}
func (p *refRange) At() signal.SeriesID { return signal.SeriesID{Lo: uint64(p.at)} }
func (p *refRange) Err() error          { return nil }

type refBitmap struct {
	words []uint64
	n     uint32
	at    int64
}

func (p *refBitmap) Next() bool { return p.nextFrom(uint64(p.at + 1)) }
func (p *refBitmap) Seek(id signal.SeriesID) bool {
	if id.Hi != 0 || id.Lo >= uint64(p.n) {
		p.at = int64(p.n)
		return false
	}
	if p.at >= 0 && uint64(p.at) >= id.Lo {
		return p.at < int64(p.n)
	}
	return p.nextFrom(id.Lo)
}
func (p *refBitmap) At() signal.SeriesID { return signal.SeriesID{Lo: uint64(p.at)} }
func (p *refBitmap) Err() error          { return nil }

// IDs scans canonical IDs without decoding identity payloads.
func (r *Index) IDs(ctx context.Context) postings.Postings {
	return &idPostings{cursor: r.table.Cursor(ctx)}
}

type idPostings struct {
	cursor  *Cursor
	started bool
	done    bool
	id      signal.SeriesID
}

func (p *idPostings) Next() bool {
	if !p.started {
		p.started = true
		return p.current(p.cursor.Seek([]byte{'i'}))
	}
	return p.current(p.cursor.Next())
}
func (p *idPostings) Seek(id signal.SeriesID) bool {
	if p.done {
		return false
	}
	if p.started && p.id.Compare(id) >= 0 {
		return p.cursor.Err() == nil
	}
	p.started = true
	return p.current(p.cursor.Seek(idKey(id)))
}
func (p *idPostings) At() signal.SeriesID { return p.id }
func (p *idPostings) Err() error          { return p.cursor.Err() }

// Names visits label names using borrowed scratch valid until the next callback.
func (r *Index) Names(ctx context.Context, visit func([]byte) error) error {
	c := r.table.Cursor(ctx)
	var name []byte
	for ok := c.Seek([]byte{'n'}); ok && len(c.Key()) > 0 && c.Key()[0] == 'n'; ok = c.Next() {
		var used int
		var err error
		name, used, err = decodeEscaped(name[:0], c.Key()[1:])
		if err != nil {
			return err
		}
		if used+1 != len(c.Key()) {
			return errors.Wrap(ErrCorrupt, "label name")
		}
		if err := visit(name); err != nil {
			return err
		}
	}
	return c.Err()
}

// Values visits a label's distinct typed values using borrowed scratch.
func (r *Index) Values(ctx context.Context, name []byte, visit func(signal.Value) error) error {
	prefix := appendEscaped([]byte{'p'}, name)
	c := r.table.Cursor(ctx)
	var data, group []byte
	for ok := c.Seek(prefix); ok && bytes.HasPrefix(c.Key(), prefix); ok = c.Seek(prefixEnd(group)) {
		var used int
		var err error
		data, used, err = decodeEscaped(data[:0], c.Key()[len(prefix):])
		if err != nil {
			return err
		}
		if used+len(prefix)+4 != len(c.Key()) {
			return errors.Wrap(ErrCorrupt, "label value key")
		}
		group = append(group[:0], c.Key()[:len(c.Key())-4]...)
		v, n, err := signal.DecodeValue(data)
		if err != nil || n != len(data) {
			return errors.Wrap(ErrCorrupt, "label value")
		}
		if err := visit(v); err != nil {
			return err
		}
	}
	return c.Err()
}

func (p *refBitmap) nextFrom(target uint64) bool {
	for target < uint64(p.n) {
		word := p.words[target>>6] & (^uint64(0) << (target & 63))
		if word != 0 {
			p.at = int64(target&^63) + int64(bits.TrailingZeros64(word))
			if p.at < int64(p.n) {
				return true
			}
			break
		}
		target = (target &^ 63) + 64
	}
	p.at = int64(p.n)
	return false
}

func (p *idPostings) current(ok bool) bool {
	if !ok || len(p.cursor.Key()) == 0 || p.cursor.Key()[0] != 'i' {
		p.done = true
		return false
	}
	if len(p.cursor.Key()) != 17 || len(p.cursor.Value()) != 4 {
		p.cursor.err = errors.Wrap(ErrCorrupt, "canonical ID entry")
		return false
	}
	p.id = signal.SeriesID{Hi: binary.BigEndian.Uint64(p.cursor.Key()[1:]), Lo: binary.BigEndian.Uint64(p.cursor.Key()[9:])}
	return true
}
