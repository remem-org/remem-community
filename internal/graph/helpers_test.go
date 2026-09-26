package graph_test

import "github.com/remem-org/remem-go/internal/codec/pb"

func vectorBody() *pb.Vector {
	return &pb.Vector{ModelId: "all-MiniLM-L6-v2", Dim: 2, Values: []float32{0.6, 0.8}}
}
