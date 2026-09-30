package ctasks_test

import (
	"testing"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/gocommon/i18n"
	_ "github.com/nyaruka/mailroom/core/handlers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks"
	"github.com/nyaruka/mailroom/core/tasks/handler"
	"github.com/nyaruka/mailroom/core/tasks/handler/ctasks"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestContactChanged(t *testing.T) {
	ctx, rt := testsuite.Runtime()
	rc := rt.RP.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetData)

	tcs := []struct {
		label       string
		contact     *testdata.Contact
		newURN      *ctasks.NewURNSpec
		expectedURN []string
	}{
		{
			label:   "append new URN",
			contact: testdata.Bob,
			newURN: &ctasks.NewURNSpec{
				Value:  "telegram:98765",
				Action: "append",
			},
			expectedURN: []string{"tel:+16055742222", "telegram:98765"},
		},
		{
			label:   "append WhatsApp BSUID",
			contact: testdata.George,
			newURN: &ctasks.NewURNSpec{
				Value:  "whatsapp:US.ABC123",
				Action: "append",
			},
			expectedURN: []string{"tel:+16055743333", "whatsapp:US.ABC123"},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.label, func(t *testing.T) {
			task := &ctasks.ContactChangedTask{
				ChannelID: testdata.TwilioChannel.ID,
				NewURN:    tc.newURN,
			}

			err := handler.QueueTask(rc, testdata.Org1.ID, tc.contact.ID, task)
			require.NoError(t, err)

			queued, err := tasks.HandlerQueue.Pop(rc)
			require.NoError(t, err)

			err = tasks.Perform(ctx, rt, queued)
			require.NoError(t, err)

			var identities []string
			err = rt.DB.Select(&identities, `SELECT identity FROM contacts_contacturn WHERE contact_id = $1 ORDER BY priority DESC, id`, tc.contact.ID)
			require.NoError(t, err)
			assert.Equal(t, tc.expectedURN, identities)
		})
	}
}

func TestReassignShellOnContactChanged(t *testing.T) {
	ctx, rt := testsuite.Runtime()
	rc := rt.RP.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetData)

	shell := testdata.InsertContact(rt, testdata.Org1, "8b2b8b4c-8e6e-4c96-9e9c-bf6b56a04e37", "Shell", i18n.Language("eng"), models.ContactStatusActive)
	testdata.InsertContactURN(rt, testdata.Org1, shell, "whatsapp:US.A1B2C3", 1000, nil)

	task := &ctasks.ContactChangedTask{
		ChannelID: testdata.TwilioChannel.ID,
		NewURN: &ctasks.NewURNSpec{
			Value:  "whatsapp:US.A1B2C3",
			Action: "append",
		},
	}

	err := handler.QueueTask(rc, testdata.Org1.ID, testdata.Cathy.ID, task)
	require.NoError(t, err)

	queued, err := tasks.HandlerQueue.Pop(rc)
	require.NoError(t, err)
	require.NoError(t, tasks.Perform(ctx, rt, queued))

	assertdb.Query(t, rt.DB, `SELECT contact_id FROM contacts_contacturn WHERE identity = $1`, "whatsapp:US.A1B2C3").Returns(int64(testdata.Cathy.ID))
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM contacts_contacturn WHERE contact_id = $1`, shell.ID).Returns(0)
}
