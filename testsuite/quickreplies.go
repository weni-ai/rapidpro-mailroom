package testsuite

import "github.com/nyaruka/goflow/flows"

// QuickReplies builds MsgContent quick replies for tests (goflow v0.233+).
func QuickReplies(texts ...string) []flows.QuickReply {
	out := make([]flows.QuickReply, len(texts))
	for i, t := range texts {
		out[i] = flows.QuickReply{Text: t}
	}
	return out
}
