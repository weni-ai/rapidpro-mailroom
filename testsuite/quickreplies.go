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

// QuickReplyStrings returns DB/courier string form of quick replies (matches models.quickRepliesToDB).
func QuickReplyStrings(qrs []flows.QuickReply) []string {
	if len(qrs) == 0 {
		return []string{}
	}
	out := make([]string, len(qrs))
	for i, qr := range qrs {
		text, err := qr.MarshalText()
		if err != nil {
			out[i] = qr.Text
			continue
		}
		out[i] = string(text)
	}
	return out
}
