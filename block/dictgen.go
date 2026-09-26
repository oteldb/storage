package block

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
}

// NewDictGen mints a token for a table the caller builds itself and keeps intact while bound.
func NewDictGen() DictGen { return DictGen{owner: new(dictOwner)} }

// IsZero reports whether g is the zero token.
func (g DictGen) IsZero() bool { return g.owner == nil }

func (g DictGen) live() bool { return g.owner != nil && g.owner.epoch == g.epoch }

// Lease says whether a decoded granule — its ids, and a self granule's table — is still intact. A
// [Decoder] retires every lease at its next decode, which may overwrite the frame the granule
// aliases. The zero Lease is for a column the caller holds itself and never expires.
type Lease struct{ g DictGen }

func (l Lease) live() bool { return l.g.owner == nil || l.g.live() }

func (o *dictOwner) token() DictGen { return DictGen{owner: o, epoch: o.epoch} }

// retire invalidates every token o has issued, before the table they name is overwritten.
func (o *dictOwner) retire() { o.epoch++ }
