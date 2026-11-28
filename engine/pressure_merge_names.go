package engine

import (
	"context"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/block"
	"github.com/oteldb/storage/internal/partid"
)

var errMergeNamesExhausted = errors.New("engine: merge part namespace exhausted")

// mergePartNames retains no generation memory after its owner publishes.
type mergePartNames struct {
	names          *partid.Sequence
	floor, ceiling string
}

func (e *Engine) prepareMergeSnapshotLocked(ctx context.Context) error {
	if err := e.establishTenureLocked(ctx); err != nil {
		return err
	}
	if e.twoPressureGenerations() {
		var err error
		e.mergeNames, err = e.reserveMergeNamesLocked()
		return err
	}
	return nil
}

func (e *Engine) reserveMergeNamesLocked() (*mergePartNames, error) {
	var names *partid.Sequence
	var generationFloor string
	if len(e.pressureGenerations) > 0 {
		g := e.pressureGenerations[0]
		names, generationFloor = g.mergeNames, g.prefixFloor
		if names == nil {
			return nil, errPressureMergeOrder
		}
	} else {
		names = partid.NewSequence()
	}
	// Each attempt must follow earlier merges that used this same predecessor namespace.
	floor, ok := names.Next()
	if !ok {
		return nil, errMergeNamesExhausted
	}
	ceiling, ok := mergeNamespaceCeiling(floor)
	if !ok {
		return nil, errMergeNamesExhausted
	}
	prefix := e.cfg.Prefix + "/"
	ticket := &mergePartNames{names: names, floor: prefix + floor.String(), ceiling: prefix + ceiling.String()}
	if generationFloor != "" && ticket.ceiling > generationFloor {
		return nil, errPressureMergeOrder
	}
	return ticket, nil
}

func (e *Engine) pressureMergeOutputs(selected, outputs []*part) ([]*part, error) {
	names := e.mergeNames
	if names == nil {
		return outputs, nil
	}
	for _, p := range selected {
		if p.prefix >= names.floor {
			return nil, errPressureMergeOrder
		}
	}
	if len(outputs) == 0 {
		// Every possible output precedes the ceiling; later frozen owners cannot lose to it.
		return []*part{{prefix: names.ceiling, minTime: maxInt64, maxTime: minInt64}}, nil
	}
	return outputs, nil
}

func newMergePartStreamWriter(
	ctx context.Context, e *Engine, comp compressProfile, precisionBits uint8, withSF, withStats bool,
	rollup *block.Rollup, indexOnly bool, extra ...block.PartOption,
) (*partStreamWriter, error) {
	if e.mergeNames == nil && !indexOnly {
		return newPartStreamWriter(ctx, e, comp, precisionBits, withSF, withStats, rollup, extra...)
	}
	var prefix string
	if e.mergeNames != nil {
		var err error
		prefix, err = e.newMergePartPrefix()
		if err != nil {
			return nil, err
		}
	}
	return newPartStreamWriterMode(ctx, e, prefix, comp, precisionBits, withSF, withStats, rollup, indexOnly, extra...)
}

func mergeNamespaceCeiling(id partid.ID) (partid.ID, bool) {
	clear(id[6:])
	for i := 5; i >= 0; i-- {
		id[i]++
		if id[i] != 0 {
			return id, true
		}
	}
	return partid.ID{}, false
}

func (e *Engine) newMergePartPrefix() (string, error) {
	ticket := e.mergeNames
	if ticket == nil {
		return e.newPartPrefix(), nil
	}
	id, ok := ticket.names.Next()
	if !ok {
		return "", errMergeNamesExhausted
	}
	prefix := e.cfg.Prefix + "/" + id.String()
	if prefix <= ticket.floor || prefix >= ticket.ceiling {
		return "", errMergeNamesExhausted
	}
	return prefix, nil
}
