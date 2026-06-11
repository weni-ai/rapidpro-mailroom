package twilioflex_test

import (
	"testing"
	"time"

	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
)

func TestEventCallback(t *testing.T) {
	ctx, rt := testsuite.Runtime()
	testsuite.Reset(testsuite.ResetData | testsuite.ResetStorage)

	defer testsuite.Reset(testsuite.ResetData | testsuite.ResetStorage)

	// India: the Twilioflex ticketer is not part of the upstream test
	// database fixture (mailroom_test.dump) yet, so insert it on the fly.
	// The config must satisfy twilioflex.NewService validation.
	rt.DB.MustExec(
		`INSERT INTO tickets_ticketer (id, is_active, created_on, modified_on, uuid, ticketer_type, name, config, created_by_id, modified_by_id, org_id, is_system)
		 VALUES ($1, TRUE, NOW(), NOW(), $2, 'twilioflex', 'Twilio Flex',
		 '{"auth_token": "sesame", "account_sid": "AC81d44315e19372138bdaffcc13cf3b94", "chat_service_sid": "IS38067ec392f1486bb6e4de4610f26fb3", "workspace_sid": "WS954611f5aeb04ce6a8b389b41f0146d2", "flex_flow_sid": "FO123456abcdefg789ijklm"}',
		 2, 2, 1, FALSE)
		 ON CONFLICT (id) DO NOTHING`,
		testdata.Twilioflex.ID, testdata.Twilioflex.UUID,
	)

	ticket := testdata.InsertOpenTicket(
		rt,
		testdata.Org1,
		testdata.Cathy,
		testdata.Twilioflex,
		testdata.DefaultTopic,
		"Have you seen my cookies?",
		"CH6442c09c93ba4d13966fa42e9b78f620",
		time.Time{},
		testdata.Viewer,
	)

	testsuite.RunWebTests(t, ctx, rt, "testdata/event_callback.json", map[string]string{"cathy_ticket_uuid": string(ticket.UUID)})
}
