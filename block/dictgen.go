package block

import "github.com/oteldb/storage/encoding/chunk"

// DictGen names one decoded dictionary table, so a [Binding] can tell the table it was bound to from
// any other. A token is scoped to its owner — a [Decoder], or a table built by the caller — so two
// owners never produce equal tokens. The zero value is invalid.
//
// A token also says whether its table is still intact. A decoder's shared-dictionary token lives as
// long as the decoder. A self-granule table aliases the decoder's frame buffer, so its token dies at
// the decoder's next decode, and a [Binding] refuses it from then on.
type DictGen struct {
	owner *dictOwner
	epoch uint64
}

// dictOwner issues tokens. Bumping epoch retires every token it has issued; framed marks an owner
// whose tables alias a reused buffer, which a binding may not keep by reference.
type dictOwner struct {
	epoch  uint64
	framed bool
	owned  bool
}

// NewDictGen mints a token for a table the caller builds itself and keeps intact while bound.
func NewDictGen() DictGen { return DictGen{owner: &dictOwner{owned: true}} }

// IsZero reports whether g is the zero token.
func (g DictGen) IsZero() bool { return g.owner == nil }

func (g DictGen) live() bool { return g.owner != nil && g.owner.epoch == g.epoch }

// DecodedGranule is one decoded bytes granule together with what it indexes and whether it is still
// intact. The three are inseparable: a [Binding] takes the granule whole, so a column cannot be paired
// with another decode's validity. The zero value is not live.
type DecodedGranule struct {
	dc    *chunk.DictColumn
	table DictGen
	lease lease
}

// lease is a granule's validity: the owner epoch it was issued at, and the column it was issued for.
type lease struct {
	g  DictGen
	dc *chunk.DictColumn
}

// OwnedGranule wraps a column the caller built itself over a table named by [NewDictGen], which stays
// valid while that table does. A token from a [Decoder] yields a granule that is never live: a
// decoder's columns come only from [Decoder.DecodeBytesBlock].
func OwnedGranule(dc *chunk.DictColumn, table DictGen) DecodedGranule {
	g := DecodedGranule{dc: dc, table: table}
	if table.owner != nil && table.owner.owned {
		g.lease = lease{g: table, dc: dc}
	}

	return g
}

// Column returns the decoded column. It aliases the decoder's frame, so it is valid only while the
// granule is live.
func (g DecodedGranule) Column() *chunk.DictColumn { return g.dc }

// Table returns the token naming the entry table the column's ids index.
func (g DecodedGranule) Table() DictGen { return g.table }

func (g DecodedGranule) live() bool {
	return g.dc != nil && g.lease.dc == g.dc && g.lease.g.live() && g.table.live()
}

func (o *dictOwner) token() DictGen { return DictGen{owner: o, epoch: o.epoch} }

// retire invalidates every token o has issued, before the table they name is overwritten.
func (o *dictOwner) retire() { o.epoch++ }
