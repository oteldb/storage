package partid

import "github.com/go-faster/errors"

// EncodedLen is the length of an [ID] in its textual form.
const EncodedLen = 26

// alphabet is Crockford base32: no I, L, O or U, so an id cannot be misread as another one.
const alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// ErrInvalid is returned by [Parse] for anything that is not a canonical id.
var ErrInvalid = errors.New("invalid part id")

// decodeTable maps a byte to its 5-bit value, or 0xFF when it is not in the alphabet.
var decodeTable = func() (t [256]byte) {
	for i := range t {
		t[i] = 0xFF
	}

	for i := range len(alphabet) {
		t[alphabet[i]] = byte(i)
	}

	return t
}()

// 26 base32 digits hold 130 bits, so the encoding is offset by the 2 zero bits that pad an ID to
// that width. Both directions index bits through this offset.
const padBits = EncodedLen*5 - Len*8

// String returns the canonical 26-character textual form.
func (id ID) String() string {
	var buf [EncodedLen]byte

	id.encode(&buf)

	return string(buf[:])
}

// AppendText implements [encoding.TextAppender].
func (id ID) AppendText(dst []byte) ([]byte, error) {
	var buf [EncodedLen]byte

	id.encode(&buf)

	return append(dst, buf[:]...), nil
}

func (id ID) encode(buf *[EncodedLen]byte) {
	for i := range buf {
		var v byte

		for b := range 5 {
			pos := i*5 + b - padBits

			var bit byte
			if pos >= 0 {
				bit = id[pos>>3] >> (7 - uint(pos&7)) & 1
			}

			v = v<<1 | bit
		}

		buf[i] = alphabet[v]
	}
}

// Parse decodes the canonical textual form. It is strict: only uppercase alphabet characters are
// accepted, and the leading digit must not overflow 128 bits, so encoding a parsed id reproduces
// the input byte for byte.
func Parse(s string) (ID, error) {
	var id ID

	if len(s) != EncodedLen {
		return id, errors.Wrapf(ErrInvalid, "length %d", len(s))
	}

	for i := range EncodedLen {
		v := decodeTable[s[i]]
		if v == 0xFF {
			return ID{}, errors.Wrapf(ErrInvalid, "character %q at %d", s[i], i)
		}

		for b := range 5 {
			pos := i*5 + b - padBits
			bit := v >> (4 - uint(b)) & 1

			if pos < 0 {
				if bit != 0 {
					return ID{}, errors.Wrap(ErrInvalid, "overflows 128 bits")
				}

				continue
			}

			id[pos>>3] |= bit << (7 - uint(pos&7))
		}
	}

	return id, nil
}

// Valid reports whether s is a canonical part id, which is what tells a part directory apart from
// an engine-level object on a backend listing.
func Valid(s string) bool {
	_, err := Parse(s)

	return err == nil
}
