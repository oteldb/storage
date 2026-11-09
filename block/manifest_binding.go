package block

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"hash/crc32"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/backend"
)

// BindManifest gives an unpublished part a persistent identity and returns its manifest digest.
// It must run before the enclosing engine publishes the part in its catalog.
func BindManifest(ctx context.Context, b backend.Backend, prefix string) ([32]byte, error) {
	raw, err := backend.ReadView(ctx, b, manifestKey(prefix))
	if err != nil {
		return [32]byte{}, err
	}
	m, err := DecodeManifest(raw)
	if err != nil {
		return [32]byte{}, err
	}
	if m.BindingNonce != ([16]byte{}) {
		return sha256.Sum256(raw), nil
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return [32]byte{}, errors.Wrap(err, "part identity nonce")
	}
	out := make([]byte, 0, len(raw)+20)
	out = append(out, raw[:len(raw)-4]...)
	out = binary.BigEndian.AppendUint32(out, bindingNonceMagic)
	out = append(out, nonce[:]...)
	out = binary.BigEndian.AppendUint32(out, crc32.Checksum(out, castagnoli))
	if err := backend.WriteDeferred(ctx, b, manifestKey(prefix), out); err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(out), nil
}
