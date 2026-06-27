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
