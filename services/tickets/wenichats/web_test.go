package wenichats_test

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

	// India: the Wenichats ticketer is not part of the upstream test
	// database fixture (mailroom_test.dump) yet, so insert it on the fly.
	// The config must satisfy wenichats.NewService validation.
	rt.DB.MustExec(
		`INSERT INTO tickets_ticketer (id, is_active, created_on, modified_on, uuid, ticketer_type, name, config, created_by_id, modified_by_id, org_id, is_system)
		 VALUES ($1, TRUE, NOW(), NOW(), $2, 'wenichats', 'Wenichats',
		 '{"project_auth": "sesame", "sector_uuid": "1a4bae05-993c-4f3b-91b5-80f4e09951f2"}',
		 2, 2, 1, FALSE)
		 ON CONFLICT (id) DO NOTHING`,
		testdata.Wenichats.ID, testdata.Wenichats.UUID,
	)

	ticket := testdata.InsertOpenTicket(
		rt,
		testdata.Org1,
		testdata.Cathy,
		testdata.Wenichats,
		testdata.DefaultTopic,
		"Have you seen my cookies?",
		"e0fa6b4b-92c2-4906-98dc-e1a9f6b141d2",
		time.Now(),
		nil,
	)

	testsuite.RunWebTests(t, ctx, rt, "testdata/event_callback.json", map[string]string{"cathy_ticket_uuid": string(ticket.UUID)})
}
