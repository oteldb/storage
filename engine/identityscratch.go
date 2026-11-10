package engine

import (
	"github.com/oteldb/storage/internal/memsize"
	"github.com/oteldb/storage/signal"
)

const (
	mergeIdentityAttributeBytes = 16 << 10
	mergeIdentityScratchBytes   = 3 * mergeIdentityAttributeBytes
)

func releaseMergeIdentityScratch(sources []*diskMergeSource) {
	for _, source := range sources {
		identity := &source.identity
		r := releaseMergeAttributes(identity.Resource.Attributes)
		s := releaseMergeAttributes(identity.Scope.Attributes)
		a := releaseMergeAttributes(identity.Attributes)
		*identity = signal.Series{Resource: signal.Resource{Attributes: r}, Scope: signal.Scope{Attributes: s}, Attributes: a}
	}
}

func releaseMergeAttributes(attrs signal.Attributes) signal.Attributes {
	clear(attrs)
	if memsize.Slice(attrs) > mergeIdentityAttributeBytes {
		return nil
	}
	return attrs[:0]
}
