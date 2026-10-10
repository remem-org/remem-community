//go:build arm64 && !purego

#include "textflag.h"

// dotNEON sums n products of the floats at a and b.
//
// Sixteen floats an iteration into four 4-lane accumulators: sixteen partial
// sums, so the four multiply-adds of one iteration are independent of each
// other and only the next iteration waits on them. Four is the number the
// scalar loop in dot_generic.go already used, and for the same reason; what
// NEON adds is that each of the four is now four lanes wide, and that the
// multiply and the add are one instruction with one rounding.
//
// At 384 dimensions — the production width, and 24 iterations with no remainder
// — this is the whole kernel. The tail exists because 384 is not every width a
// model emits, and it is a scalar loop on purpose: a width that is not a
// multiple of sixteen pays at most fifteen scalar products, which is cheaper to
// read than it would ever be to vectorise.
//
// The sum's order is therefore neither the scalar loop's nor a sequential one.
// It is a tree, which is more accurate rather than less, and
// dot_internal_test.go bounds the difference.
//
// func dotNEON(a, b *float32, n int) float32
TEXT ·dotNEON(SB), NOSPLIT, $0-28
	MOVD	a+0(FP), R0
	MOVD	b+8(FP), R1
	MOVD	n+16(FP), R2

	VEOR	V0.B16, V0.B16, V0.B16
	VEOR	V1.B16, V1.B16, V1.B16
	VEOR	V2.B16, V2.B16, V2.B16
	VEOR	V3.B16, V3.B16, V3.B16

	LSR	$4, R2, R3      // whole blocks of sixteen
	CBZ	R3, reduce

blocks:
	VLD1.P	64(R0), [V4.S4, V5.S4, V6.S4, V7.S4]
	VLD1.P	64(R1), [V8.S4, V9.S4, V10.S4, V11.S4]
	VFMLA	V8.S4, V4.S4, V0.S4
	VFMLA	V9.S4, V5.S4, V1.S4
	VFMLA	V10.S4, V6.S4, V2.S4
	VFMLA	V11.S4, V7.S4, V3.S4
	SUB	$1, R3, R3
	CBNZ	R3, blocks

reduce:
	// Four accumulators to one, then four lanes to one, pairwise throughout.
	// F0 and V0 are the same register, so lane zero is already the scalar the
	// tail adds to and the function returns.
	VFADD	V1.S4, V0.S4, V0.S4
	VFADD	V3.S4, V2.S4, V2.S4
	VFADD	V2.S4, V0.S4, V0.S4
	VFADDP	V0.S4, V0.S4, V0.S4
	VFADDP	V0.S4, V0.S4, V0.S4

	AND	$15, R2, R4     // the remainder the blocks left
	CBZ	R4, done

tail:
	FMOVS.P	4(R0), F4
	FMOVS.P	4(R1), F5
	FMADDS	F5, F0, F4, F0
	SUB	$1, R4, R4
	CBNZ	R4, tail

done:
	FMOVS	F0, ret+24(FP)
	RET
