// Command gensimd generates the AVX2 assembly kernels in internal/simd via avo. The kernels
// always ship with a pure-Go fallback (internal/simd/*_generic.go) and a runtime CPU-feature
// dispatch, so the library builds and runs on every architecture; the generated .s is committed.
//
// Run via `go generate ./internal/simd/...` (the //go:generate directive there invokes this).
package main

import (
	//nolint:revive // avo's build DSL is designed to be dot-imported.
	. "github.com/mmcloughlin/avo/build"
	//nolint:revive // avo's operand DSL is designed to be dot-imported.
	. "github.com/mmcloughlin/avo/operand"
	"github.com/mmcloughlin/avo/reg"
)

func main() {
	Package("github.com/oteldb/storage/internal/simd")
	ConstraintExpr("amd64")

	genMinMaxInt64()
	genMinMaxFloat64()
	genEqualFixed16()

	Generate()
}

// genMinMaxInt64 emits minMaxInt64AVX2(s []int64) (min, max int64): the min and max of s,
// computed four lanes at a time with VPCMPGTQ + VPBLENDVB, with a scalar fold of the vector
// accumulators and the tail. The caller guarantees len(s) >= 1.
func genMinMaxInt64() {
	TEXT("minMaxInt64AVX2", NOSPLIT, "func(s []int64) (rmin, rmax int64)")
	Doc("minMaxInt64AVX2 returns the minimum and maximum of s using AVX2. len(s) must be >= 1.")

	ptr := Load(Param("s").Base(), GP64())
	n := Load(Param("s").Len(), GP64())

	mn := GP64() // scalar running min
	mx := GP64() // scalar running max
	MOVQ(Mem{Base: ptr}, mn)
	MOVQ(mn, mx)

	i := GP64()
	XORQ(i, i)

	// Vector accumulators, each lane initialized to s[0].
	vmin := YMM()
	vmax := YMM()
	VPBROADCASTQ(Mem{Base: ptr}, vmin)
	VMOVDQU(vmin, vmax)

	// Vector loop: process 4 int64 per iteration while i+4 <= n.
	limit := GP64()
	MOVQ(n, limit)
	SUBQ(Imm(3), limit) // i < n-3  ⇔  i+4 <= n

	v := YMM()
	mask := YMM()

	Label("vecloop")
	CMPQ(i, limit)
	JGE(LabelRef("vecdone"))

	VMOVDQU(Mem{Base: ptr, Index: i, Scale: 8}, v)

	// vmin = min(vmin, v): mask = vmin > v (per lane), then pick v where mask, else vmin.
	VPCMPGTQ(v, vmin, mask) // mask lane = vmin > v ? all-ones : 0
	VPBLENDVB(mask, v, vmin, vmin)

	// vmax = max(vmax, v): mask = v > vmax, then pick v where mask, else vmax.
	VPCMPGTQ(vmax, v, mask) // mask lane = v > vmax ? all-ones : 0
	VPBLENDVB(mask, v, vmax, vmax)

	ADDQ(Imm(4), i)
	JMP(LabelRef("vecloop"))

	Label("vecdone")

	// Fold the 4 vector lanes into the scalar min/max via a stack spill.
	foldLane := func(base Mem, running reg.GPVirtual, isMin bool) {
		for lane := range 4 {
			lv := GP64()
			MOVQ(base.Offset(lane*8), lv)
			CMPQ(lv, running)

			if isMin {
				cmov := GP64()
				MOVQ(running, cmov)
				CMOVQLT(lv, cmov) // cmov = (lv < running) ? lv : running
				MOVQ(cmov, running)
			} else {
				cmov := GP64()
				MOVQ(running, cmov)
				CMOVQGT(lv, cmov) // cmov = (lv > running) ? lv : running
				MOVQ(cmov, running)
			}
		}
	}

	// Spill vmin and vmax to separate buffers and fold.
	bufMin := AllocLocal(32)
	bufMax := AllocLocal(32)
	VMOVDQU(vmin, bufMin)
	VMOVDQU(vmax, bufMax)
	foldLane(bufMin, mn, true)
	foldLane(bufMax, mx, false)

	// Scalar tail: fold any remaining elements (i..n).
	Label("tail")
	CMPQ(i, n)
	JGE(LabelRef("done"))

	tv := GP64()
	MOVQ(Mem{Base: ptr, Index: i, Scale: 8}, tv)

	cmn := GP64()
	MOVQ(mn, cmn)
	CMPQ(tv, mn)
	CMOVQLT(tv, cmn)
	MOVQ(cmn, mn)

	cmx := GP64()
	MOVQ(mx, cmx)
	CMPQ(tv, mx)
	CMOVQGT(tv, cmx)
	MOVQ(cmx, mx)

	ADDQ(Imm(1), i)
	JMP(LabelRef("tail"))

	Label("done")
	Store(mn, ReturnIndex(0))
	Store(mx, ReturnIndex(1))
	RET()
}

// genMinMaxFloat64 emits minMaxFloat64AVX2(s []float64) (rmin, rmax float64): the min and max of s
// ignoring NaN, four lanes at a time. NaN lanes are replaced with +Inf before VMINPD and -Inf
// before VMAXPD (so they never affect the result), the accumulators start at (+Inf, -Inf), and the
// scalar tail skips NaN — so an all-NaN (or empty) slice returns (+Inf, -Inf), the sentinel the
// caller treats as "no real values". min/max are order-independent, so the result is bit-identical
// to the pure-Go reference.
func genMinMaxFloat64() {
	TEXT("minMaxFloat64AVX2", NOSPLIT, "func(s []float64) (rmin, rmax float64)")
	Doc("minMaxFloat64AVX2 returns the min and max of s ignoring NaN, using AVX2; all-NaN ⇒ (+Inf, -Inf).")

	ptr := Load(Param("s").Base(), GP64())
	n := Load(Param("s").Len(), GP64())

	// +Inf / -Inf bit patterns, broadcast to YMM.
	posBits := GP64()
	MOVQ(U64(0x7FF0000000000000), posBits)
	negBits := GP64()
	MOVQ(U64(0xFFF0000000000000), negBits)

	xPos := XMM()
	MOVQ(posBits, xPos)
	xNeg := XMM()
	MOVQ(negBits, xNeg)

	vPos := YMM()
	VPBROADCASTQ(xPos, vPos)
	vNeg := YMM()
	VPBROADCASTQ(xNeg, vNeg)

	vmin := YMM()
	VMOVDQU(vPos, vmin) // running min = +Inf
	vmax := YMM()
	VMOVDQU(vNeg, vmax) // running max = -Inf

	i := GP64()
	XORQ(i, i)

	limit := GP64()
	MOVQ(n, limit)
	SUBQ(Imm(3), limit) // i < n-3  ⇔  i+4 <= n (signed compare handles n<4)

	v := YMM()
	nanmask := YMM()
	blended := YMM()

	Label("vecloop")
	CMPQ(i, limit)
	JGE(LabelRef("vecdone"))

	VMOVUPD(Mem{Base: ptr, Index: i, Scale: 8}, v)

	// nanmask lane = all-ones where v is NaN (VCMPPD predicate 3 = unordered).
	VCMPPD(Imm(3), v, v, nanmask)

	// vmin = min(vmin, NaN?+Inf:v)
	VBLENDVPD(nanmask, vPos, v, blended)
	VMINPD(blended, vmin, vmin)

	// vmax = max(vmax, NaN?-Inf:v)
	VBLENDVPD(nanmask, vNeg, v, blended)
	VMAXPD(blended, vmax, vmax)

	ADDQ(Imm(4), i)
	JMP(LabelRef("vecloop"))

	Label("vecdone")

	// Spill the 4 lanes and fold with scalar SSE (lanes are NaN-free after the blend).
	bufMin := AllocLocal(32)
	bufMax := AllocLocal(32)
	VMOVDQU(vmin, bufMin)
	VMOVDQU(vmax, bufMax)
	VZEROUPPER() // avoid the AVX↔legacy-SSE transition penalty before the scalar fold/tail

	rmin := XMM()
	MOVSD(bufMin.Offset(0), rmin)
	rmax := XMM()
	MOVSD(bufMax.Offset(0), rmax)

	for lane := 1; lane < 4; lane++ {
		lo := XMM()
		MOVSD(bufMin.Offset(lane*8), lo)
		MINSD(lo, rmin)

		hi := XMM()
		MOVSD(bufMax.Offset(lane*8), hi)
		MAXSD(hi, rmax)
	}

	// Scalar tail with NaN skip.
	Label("tail")
	CMPQ(i, n)
	JGE(LabelRef("done"))

	t := XMM()
	MOVSD(Mem{Base: ptr, Index: i, Scale: 8}, t)
	UCOMISD(t, t) // sets PF when t is NaN (unordered)
	JP(LabelRef("tailnext"))
	MINSD(t, rmin)
	MAXSD(t, rmax)

	Label("tailnext")
	ADDQ(Imm(1), i)
	JMP(LabelRef("tail"))

	Label("done")
	Store(rmin, ReturnIndex(0))
	Store(rmax, ReturnIndex(1))
	RET()
}

// genEqualFixed16 emits equalFixed16AVX2(blob, needle, dst []byte): dst[i] = 1 where the i-th
// 16-byte row of blob equals needle, else 0. Rows are compared 2 at a time (one 32-byte YMM
// register holds needle broadcast into both 128-bit lanes, VPCMPEQB against 2 loaded rows,
// VPMOVMSKB's two 16-bit halves each test for all-ones), but — unlike a naive single-vector loop —
// the main loop runs 4 of these 2-row compares per iteration (8 rows / 128 bytes), each against
// its own fresh set of registers. The four compares have no data dependency on each other (each
// only reads blobPtr/blobOff/needleVec/i/dstPtr and writes its own registers/dst bytes), so even
// though avo's register allocator may reuse the same physical registers across the four call
// sites, the CPU's out-of-order engine still runs them concurrently via register renaming — the
// 4-wide unroll exists to give it 4 independent load→compare→store chains to overlap across
// multiple load/ALU ports, instead of forcing one chain's latency to serialize behind the next.
// The caller guarantees len(dst) is even and len(blob) == len(dst)*16 (an odd tail row is handled
// by the Go wrapper via the generic reference).
func genEqualFixed16() {
	TEXT("equalFixed16AVX2", NOSPLIT, "func(blob []byte, needle []byte, dst []byte)")
	Doc("equalFixed16AVX2 sets dst[i]=1 where blob's i-th 16-byte row equals needle, 0 otherwise. len(dst) must be even.")

	// Load the three slice base pointers, plus dst's length — the row count to compare (blob and
	// needle carry no useful length here: blob's is rows*16, needle's is always exactly 16).
	blobPtr := Load(Param("blob").Base(), GP64())
	needlePtr := Load(Param("needle").Base(), GP64())
	dstPtr := Load(Param("dst").Base(), GP64())
	n := Load(Param("dst").Len(), GP64())

	// needleVec = [needle(16B) | needle(16B)]: one 16-byte load, broadcast into both 128-bit lanes
	// of a YMM register, so a single 32-byte vector compare checks two rows against it at once.
	// Loaded once and reused (read-only) by every pair below.
	needleVec := YMM()
	VBROADCASTI128(Mem{Base: needlePtr}, needleVec)

	i := GP64() // row index (dst[i]); advances by however many rows the current loop stage compares
	XORQ(i, i)  // i = 0

	blobOff := GP64()      // byte offset into blob; kept as i*16 incrementally
	XORQ(blobOff, blobOff) // blobOff = 0

	// cmpPair emits one 32-byte (2-row) compare at byte offset blobDisp from blobOff, writing
	// dst[i+rowDisp] and dst[i+rowDisp+1]. blobDisp/rowDisp are Go-time constants (0, 32, 64, 96 /
	// 0, 2, 4, 6 for the unrolled main loop; both 0 for the 2-row tail loop below) baked into the
	// memory operands' displacement — Mem.Offset — so no extra address-computing instruction (and
	// so no flags-clobber hazard: an earlier version computed dst[i+1]'s address via a runtime
	// ADDQ sitting between the CMPL and the SETEQ that reads its flags, which clobbered them for
	// odd rows; addressing via a constant displacement instead sidesteps the hazard entirely).
	// Every call allocates fresh v/cmp/mask/low registers, so independent calls carry no false
	// dependency (see the func doc above).
	cmpPair := func(blobDisp, rowDisp int) {
		v := YMM()     // this pair's 2-row (32-byte) load from blob
		cmp := YMM()   // per-byte equality: cmp's byte k = 0xFF if v's byte k == needleVec's byte k
		mask := GP32() // VPMOVMSKB(cmp): bit k = the sign (top) bit of cmp's byte k — 1 bit/byte

		VMOVDQU(Mem{Base: blobPtr, Index: blobOff, Scale: 1}.Offset(blobDisp), v)
		VPCMPEQB(needleVec, v, cmp)
		VPMOVMSKB(cmp, mask)

		// Row 0 of the pair matches iff all 16 of its bits (mask's low 16 bits) are set — every
		// byte compared equal. Copy into low before masking (rather than masking mask itself) so
		// mask is still intact for row 1's check below.
		low := GP32()
		MOVL(mask, low)
		ANDL(U32(0xFFFF), low)                                       // low &= 0x0000FFFF: keep only row 0's 16 bits
		CMPL(low, U32(0xFFFF))                                       // sets ZF iff low == 0xFFFF (all 16 bytes matched)
		SETEQ(Mem{Base: dstPtr, Index: i, Scale: 1}.Offset(rowDisp)) // dst[i+rowDisp] = ZF ? 1 : 0

		// Row 1's 16 bits are mask's top half (bits 16-31); shift down to bits 0-15 so the same
		// 0xFFFF comparison serves both rows. mask is dead after this (last use), so shifting it
		// in place — instead of copying to a fresh register, as row 0 did — costs nothing extra.
		SHRL(Imm(16), mask)
		CMPL(mask, U32(0xFFFF))                                          // ZF iff row 1's 16 bytes all matched
		SETEQ(Mem{Base: dstPtr, Index: i, Scale: 1}.Offset(rowDisp + 1)) // dst[i+rowDisp+1] = ZF ? 1 : 0
	}

	// Main loop: 4 independent pairs (8 rows / 128 bytes) per iteration.
	limit8 := GP64()
	MOVQ(n, limit8)
	SUBQ(Imm(7), limit8) // i < n-7  ⇔  i+8 <= n

	Label("loop8")
	CMPQ(i, limit8)
	JGE(LabelRef("tail2setup"))

	cmpPair(0, 0)
	cmpPair(32, 2)
	cmpPair(64, 4)
	cmpPair(96, 6)

	ADDQ(Imm(8), i)
	ADDQ(Imm(128), blobOff)
	JMP(LabelRef("loop8"))

	// Tail: fewer than 8 rows remain. n is even and 8 is a multiple of 2, so the remainder (0, 2,
	// 4, or 6 rows) is always even too — finish it 2 rows (one pair) at a time.
	Label("tail2setup")
	limit2 := GP64()
	MOVQ(n, limit2)
	SUBQ(Imm(1), limit2) // i < n-1  ⇔  i+2 <= n

	Label("loop2")
	CMPQ(i, limit2)
	JGE(LabelRef("done"))

	cmpPair(0, 0)

	ADDQ(Imm(2), i)
	ADDQ(Imm(32), blobOff)
	JMP(LabelRef("loop2"))

	Label("done")
	VZEROUPPER() // clear the YMM upper halves before returning to Go (avoids an AVX/SSE transition penalty)
	RET()
}
