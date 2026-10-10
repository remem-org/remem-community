//go:build arm64 && purego

package hnsw

// arm64 under `purego`: the portable four-accumulator dot product, with the
// compiler fusing multiply-add. These are the values recorded before plan item
// 15, and item 15 left them bit-identical on this kernel.
var recordedDigests = walkDigests{
	l2:     "ee50f764d879162a6aaf9773c3ca193931dccaafe0d1fbb3e3bf2d370390cec3",
	cosine: "d6105f606ce614be58149fe518c6486d45503fe9c1eaae4be256cab412ca0cfc",
	covers: "arm64 purego, scalar dot product, fused multiply-add",
}
