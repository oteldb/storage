package cluster

import (
	"bytes"
	"encoding/binary"
	"slices"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/query/fetch"
)

func appendStringMatchers(dst []byte, matchers []fetch.StringSetMatcher) []byte {
	if len(matchers) == 0 {
		return dst
	}
	dst = binary.AppendUvarint(dst, uint64(len(matchers)))
	for _, m := range matchers {
		dst = appendString(dst, m.Name)
		dst = binary.AppendUvarint(dst, uint64(len(m.Values)))
		for _, v := range m.Values {
			dst = binary.AppendUvarint(dst, uint64(len(v)))
			dst = append(dst, v...)
		}
	}
	return dst
}

func walkStringMatchers(data []byte, visit func(string, [][]byte)) error {
	if len(data) == 0 {
		return nil
	}
	n, size := binary.Uvarint(data)
	if size <= 0 || n > uint64(len(data)-size)/2 {
		return errors.New("cluster: malformed string matcher count")
	}
	data = data[size:]
	for range n {
		name, rest, err := takeString(data)
		if err != nil {
			return errors.Wrap(err, "string matcher name")
		}
		data = rest
		count, size := binary.Uvarint(data)
		if size <= 0 || count > uint64(len(data)-size) {
			return errors.New("cluster: malformed string matcher value count")
		}
		data = data[size:]
		var values [][]byte
		if visit != nil {
			values = make([][]byte, 0, count)
		}
		for range count {
			length, size := binary.Uvarint(data)
			if size <= 0 || length > uint64(len(data)-size) {
				return errors.New("cluster: malformed string matcher value")
			}
			data = data[size:]
			if visit != nil {
				values = append(values, bytes.Clone(data[:length]))
			}
			data = data[length:]
		}
		if visit != nil {
			slices.SortFunc(values, bytes.Compare)
			visit(name, slices.CompactFunc(values, bytes.Equal))
		}
	}
	return nil
}

func takeStringMatchers(data []byte) ([]fetch.StringSetMatcher, error) {
	if err := walkStringMatchers(data, nil); err != nil {
		return nil, err
	}
	var out []fetch.StringSetMatcher
	err := walkStringMatchers(data, func(name string, values [][]byte) {
		out = append(out, fetch.StringSetMatcher{Name: name, Values: values})
	})
	return out, err
}

func hintMatchers(tail []byte, eq []fetch.EqualMatcher) ([]fetch.Matcher, error) {
	sets, err := takeStringMatchers(tail)
	if err != nil {
		return nil, err
	}
	matchers := matchersFromEq(eq)
	for _, set := range sets {
		matchers = append(matchers, set.Matcher())
	}
	return matchers, nil
}
