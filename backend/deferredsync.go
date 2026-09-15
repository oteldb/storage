package backend

import "context"

// DeferredSyncer is an optional [Backend] capability: write or delete the objects of one prefix
// without making each durable on its own, then make the prefix durable once with SyncPrefix.
//
// A part is unreachable until the bucket-index commit names it, so its objects only need to be on
// the disk before that commit, not one by one. On a filesystem that is the difference between a
// directory fsync per object and a few per part.
//
// Until SyncPrefix returns, a power cut may take any subset of the deferred names, including
// a manifest without its columns. A deferred delete may come back. Callers must not publish anything
// naming the prefix before SyncPrefix, and must tolerate resurrected objects the way they tolerate
// orphans.
//
// Use the package helpers rather than asserting directly: each falls back to the synchronous
// operation (and SyncPrefix to nothing), so a wrapper that does not forward the capability only
// loses the saving, never durability. A wrapper that forwards one method must forward all four.
type DeferredSyncer interface {
	// WriteDeferred is [Backend.Write] without the durability of the name.
	WriteDeferred(ctx context.Context, key string, data []byte) error
	// CreateObjectDeferred is [CreateObject] whose commit defers durability of the name.
	CreateObjectDeferred(ctx context.Context, key string) (ObjectWriter, error)
	// DeleteDeferred is [Backend.Delete] without the durability of the removal.
	DeleteDeferred(ctx context.Context, key string) error
	// SyncPrefix makes every name under prefix, and prefix itself, durable.
	SyncPrefix(ctx context.Context, prefix string) error
}

// WriteDeferred writes through b's [DeferredSyncer], or [Backend.Write] without one.
func WriteDeferred(ctx context.Context, b Backend, key string, data []byte) error {
	if d, ok := b.(DeferredSyncer); ok {
		return d.WriteDeferred(ctx, key, data)
	}

	return b.Write(ctx, key, data)
}

// CreateObjectDeferred opens a writer through b's [DeferredSyncer], or [CreateObject] without one.
func CreateObjectDeferred(ctx context.Context, b Backend, key string) (ObjectWriter, error) {
	if d, ok := b.(DeferredSyncer); ok {
		return d.CreateObjectDeferred(ctx, key)
	}

	return CreateObject(ctx, b, key)
}

// DeleteDeferred deletes through b's [DeferredSyncer], or [Backend.Delete] without one.
func DeleteDeferred(ctx context.Context, b Backend, key string) error {
	if d, ok := b.(DeferredSyncer); ok {
		return d.DeleteDeferred(ctx, key)
	}

	return b.Delete(ctx, key)
}

// SyncPrefix makes the deferred operations under prefix durable through b's [DeferredSyncer]. It
// does nothing without one: every operation was already durable.
func SyncPrefix(ctx context.Context, b Backend, prefix string) error {
	if d, ok := b.(DeferredSyncer); ok {
		return d.SyncPrefix(ctx, prefix)
	}

	return nil
}
