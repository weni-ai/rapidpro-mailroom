package twilioflex_test

import (
	"testing"
	"time"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/nyaruka/mailroom/web"
)

func TestEventCallback(t *testing.T) {
	ctx, rt, db, _ := testsuite.Get()
	testsuite.Reset(testsuite.ResetData | testsuite.ResetStorage)

	defer testsuite.Reset(testsuite.ResetData | testsuite.ResetStorage)

	// India: the Twilioflex ticketer is not part of the upstream test
	// database fixture (mailroom_test.dump) yet, so insert it on the fly.
	db.MustExec(
		`INSERT INTO tickets_ticketer (id, is_active, created_on, modified_on, uuid, ticketer_type, name, config, created_by_id, modified_by_id, org_id, is_system)
		 VALUES ($1, TRUE, NOW(), NOW(), $2, 'twilioflex', 'Twilio Flex', '{}', 2, 2, 1, FALSE)
		 ON CONFLICT (id) DO NOTHING`,
		testdata.Twilioflex.ID, testdata.Twilioflex.UUID,
	)

	ticket := testdata.InsertOpenTicket(
		db,
		testdata.Org1,
		testdata.Cathy,
		testdata.Twilioflex,
		testdata.DefaultTopic,
		"Have you seen my cookies?",
		"CH6442c09c93ba4d13966fa42e9b78f620",
		time.Time{},
		testdata.Viewer,
	)

	web.RunWebTests(t, ctx, rt, "testdata/event_callback.json", map[string]string{"cathy_ticket_uuid": string(ticket.UUID)})
}
