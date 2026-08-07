package emit

const (
	HeaderReason       = "x-kafkatb-reason"
	HeaderError        = "x-kafkatb-error"
	HeaderDetail       = "x-kafkatb-detail"
	HeaderSrcTopic     = "x-kafkatb-src-topic"
	HeaderSrcPartition = "x-kafkatb-src-partition"
	HeaderSrcOffset    = "x-kafkatb-src-offset"
	HeaderSrcTimestamp = "x-kafkatb-src-timestamp"
	HeaderAttemptTS    = "x-kafkatb-attempt-ts"
)

type Reason string

const (
	ReasonPoison Reason = "poison"
	ReasonReject Reason = "reject"
)
