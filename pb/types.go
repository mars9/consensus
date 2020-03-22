package pb

//go:generate protoc --go_out=. raft.proto

import "github.com/golang/protobuf/proto"

type Message interface {
	proto.Message
	
	GetFrom() uint64
	GetTo() uint64
	GetTerm() uint64
}
