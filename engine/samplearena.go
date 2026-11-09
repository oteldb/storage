package engine

const (
	sampleNumericPageSize  = 1024
	sampleHeaderPageSize   = 256
	sampleFreeNumericPages = 512
	sampleFreeHeaderPages  = 128
)

type sampleNumericPage struct {
	next   *sampleNumericPage
	used   int
	ts     [sampleNumericPageSize]int64
	values [sampleNumericPageSize]float64
}

type sampleHeaderPage struct {
	next *sampleHeaderPage
	used int
	bufs [sampleHeaderPageSize]sampleBuf
}

var (
	freeSampleNumerics = make(chan *sampleNumericPage, sampleFreeNumericPages)
	freeSampleHeaders  = make(chan *sampleHeaderPage, sampleFreeHeaderPages)
)

// sampleArena owns one head generation's small buffers. A published flush releases it only after
// removing its frozen buffers under the state lock; read plans already own their window copies.
type sampleArena struct {
	numeric, numericTail *sampleNumericPage
	headers, headerTail  *sampleHeaderPage
}

func (a *sampleArena) buffer() *sampleBuf {
	p := a.headers
	if p == nil || p.used == len(p.bufs) {
		select {
		case p = <-freeSampleHeaders:
		default:
			p = &sampleHeaderPage{}
		}
		p.next = a.headers
		a.headers = p
		if a.headerTail == nil {
			a.headerTail = p
		}
	}
	b := &p.bufs[p.used]
	p.used++
	return b
}

func (a *sampleArena) columns(n int) ([]int64, []float64) {
	if n > sampleNumericPageSize {
		return make([]int64, 0, n), make([]float64, 0, n)
	}
	p := a.numeric
	if p == nil || n > len(p.ts)-p.used {
		select {
		case p = <-freeSampleNumerics:
		default:
			p = &sampleNumericPage{}
		}
		p.next = a.numeric
		a.numeric = p
		if a.numericTail == nil {
			a.numericTail = p
		}
	}
	lo, hi := p.used, p.used+n
	p.used = hi
	return p.ts[lo:lo:hi], p.values[lo:lo:hi]
}

func (a *sampleArena) initColumns(b *sampleBuf, n int) {
	if cap(b.ts) == 0 && cap(b.values) == 0 {
		b.ts, b.values = a.columns(n)
	}
}

func (a *sampleArena) adopt(other *sampleArena) {
	if other.numeric != nil {
		other.numericTail.next = a.numeric
		a.numeric = other.numeric
		if a.numericTail == nil {
			a.numericTail = other.numericTail
		}
	}
	if other.headers != nil {
		other.headerTail.next = a.headers
		a.headers = other.headers
		if a.headerTail == nil {
			a.headerTail = other.headerTail
		}
	}
	*other = sampleArena{}
}

// abandon leaves surviving buffers to the GC after rollback has discarded obsolete headers.
func (a *sampleArena) abandon() {
	for p := a.numeric; p != nil; {
		next := p.next
		p.next = nil
		p = next
	}
	for p := a.headers; p != nil; {
		next := p.next
		p.next = nil
		p = next
	}
	*a = sampleArena{}
}

func (a *sampleArena) release() {
	for p := a.numeric; p != nil; {
		next := p.next
		p.next, p.used = nil, 0
		select {
		case freeSampleNumerics <- p:
		default:
		}
		p = next
	}
	for p := a.headers; p != nil; {
		next := p.next
		p.next = nil
		if len(freeSampleHeaders) < cap(freeSampleHeaders) {
			clear(p.bufs[:p.used])
			p.used = 0
			select {
			case freeSampleHeaders <- p:
			default:
			}
		}
		p = next
	}
	*a = sampleArena{}
}
