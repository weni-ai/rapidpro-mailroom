package ctasks_test

import (
	"testing"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/mailroom/v26/core/models"
	"github.com/nyaruka/mailroom/v26/core/tasks"
	"github.com/nyaruka/mailroom/v26/core/tasks/ctasks"
	"github.com/nyaruka/mailroom/v26/testsuite"
	"github.com/nyaruka/mailroom/v26/testsuite/testdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestContactChanged(t *testing.T) {
	ctx, rt := testsuite.Runtime(t)
	vc := rt.VK.Get()
	defer vc.Close()

	defer testsuite.Reset(t, rt, testsuite.ResetData|testsuite.ResetDynamo|testsuite.ResetElastic)

	type urnRow struct {
		Identity  string            `db:"identity"`
		ChannelID *models.ChannelID `db:"channel_id"`
	}

	tcs := []struct {
		label       string
		preHook     func()
		contact     *testdb.Contact
		channel     *testdb.Channel
		newURN      *ctasks.NewURNSpec
		expectedURN []urnRow
	}{
		{
			label:   "append new URN saves channel affinity",
			contact: testdb.Bob,
			channel: testdb.TwilioChannel,
			newURN: &ctasks.NewURNSpec{
				Value:  "telegram:98765",
				Action: "append",
			},
			expectedURN: []urnRow{
				{Identity: "tel:+16055742222", ChannelID: nil},
				{Identity: "telegram:98765", ChannelID: &testdb.TwilioChannel.ID},
			},
		},
		{
			label: "append duplicate URN",
			preHook: func() {
				rt.DB.MustExec(`DELETE FROM contacts_contacturn WHERE contact_id = $1 AND scheme = 'telegram'`, testdb.Bob.ID)
				testdb.InsertContactURN(t, rt, testdb.Org1, testdb.Bob, "telegram:98765", 999, nil)
			},
			contact: testdb.Bob,
			channel: testdb.TwilioChannel,
			newURN: &ctasks.NewURNSpec{
				Value:  "telegram:98765",
				Action: "append",
			},
			// telegram URN already existed without a channel, no modification event emitted
			expectedURN: []urnRow{
				{Identity: "tel:+16055742222", ChannelID: nil},
				{Identity: "telegram:98765", ChannelID: nil},
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.label, func(t *testing.T) {
			models.FlushCache()

			if tc.preHook != nil {
				tc.preHook()
			}

			task := &ctasks.ContactChanged{
				ChannelID: tc.channel.ID,
				NewURN:    tc.newURN,
			}

			err := tasks.QueueContact(ctx, rt, testdb.Org1.ID, tc.contact.ID, task)
			require.NoError(t, err)

			queued, err := rt.Queues.Realtime.Pop(ctx, vc)
			require.NoError(t, err)
			require.NotNil(t, queued)

			err = tasks.Perform(ctx, rt, queued)
			require.NoError(t, err)

			var urnRows []urnRow
			err = rt.DB.Select(&urnRows, `SELECT identity, channel_id FROM contacts_contacturn WHERE contact_id = $1 ORDER BY priority DESC`, tc.contact.ID)
			require.NoError(t, err)
			assert.Equal(t, tc.expectedURN, urnRows)
		})
	}
}

func TestReassignShellOnContactChanged(t *testing.T) {
	ctx, rt := testsuite.Runtime(t)
	vc := rt.VK.Get()
	defer vc.Close()

	defer testsuite.Reset(t, rt, testsuite.ResetData|testsuite.ResetDynamo|testsuite.ResetElastic)

	shell := testdb.InsertContact(t, rt, testdb.Org1, "8b2b8b4c-8e6e-4c96-9e9c-bf6b56a04e37", "Shell", "eng", models.ContactStatusActive)
	testdb.InsertContactURN(t, rt, testdb.Org1, shell, "bsuid:US.A1B2C3", 1000, nil)

	task := &ctasks.ContactChanged{
		ChannelID: testdb.TwilioChannel.ID,
		NewURN: &ctasks.NewURNSpec{
			Value:  "bsuid:US.A1B2C3",
			Action: "append",
		},
	}

	err := tasks.QueueContact(ctx, rt, testdb.Org1.ID, testdb.Ann.ID, task)
	require.NoError(t, err)

	queued, err := rt.Queues.Realtime.Pop(ctx, vc)
	require.NoError(t, err)
	require.NotNil(t, queued)
	require.NoError(t, tasks.Perform(ctx, rt, queued))

	assertdb.Query(t, rt.DB, `SELECT contact_id FROM contacts_contacturn WHERE identity = $1`, "bsuid:US.A1B2C3").Returns(int64(testdb.Ann.ID))
	assertdb.Query(t, rt.DB, `SELECT count(*) FROM contacts_contacturn WHERE contact_id = $1`, shell.ID).Returns(0)
}
