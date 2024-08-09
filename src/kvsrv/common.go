package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OptionId int64
	Type     RequestType
}

type PutAppendReply struct {
	Value string
	// Status RequestStatus
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Value string
	// Status RequestStatus
}
