package ivr_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/mailroom/core/ivr"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/queue"
	"github.com/nyaruka/mailroom/core/tasks"
	ivrtasks "github.com/nyaruka/mailroom/core/tasks/ivr"
	"github.com/nyaruka/mailroom/core/tasks/starts"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetries(t *testing.T) {
	ctx, rt := testsuite.Runtime()
	rc := rt.RP.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetAll)

	// register our mock client
	ivr.RegisterServiceType(models.ChannelType("ZZ"), NewMockProvider)

	// update our twilio channel to be of type 'ZZ' and set max_concurrent_events to 1
	rt.DB.MustExec(`UPDATE channels_channel SET channel_type = 'ZZ', config = '{"max_concurrent_events": 1}' WHERE id = $1`, testdata.TwilioChannel.ID)

	// create a flow start for cathy
	start := models.NewFlowStart(testdata.Org1.ID, models.StartTypeTrigger, models.FlowTypeVoice, testdata.IVRFlow.ID).
		WithContactIDs([]models.ContactID{testdata.Cathy.ID})

	err := tasks.Queue(rc, queue.BatchQueue, testdata.Org1.ID, &starts.StartFlowTask{FlowStart: start}, queue.DefaultPriority)
	require.NoError(t, err)

	service.callError = nil
	service.callID = ivr.CallID("call1")

	testsuite.FlushTasks(t, rt)

	assertdb.Query(t, rt.DB, `SELECT COUNT(*) FROM ivr_call WHERE contact_id = $1 AND status = $2 AND external_id = $3`,
		testdata.Cathy.ID, models.CallStatusWired, "call1").Returns(1)

	// change our call to be errored instead of wired
	rt.DB.MustExec(`UPDATE ivr_call SET status = 'E', next_attempt = NOW() WHERE external_id = 'call1';`)

	// fire our retries
	err = ivrtasks.RetryCalls(ctx, rt)
	assert.NoError(t, err)

	// should now be in wired state
	assertdb.Query(t, rt.DB, `SELECT COUNT(*) FROM ivr_call WHERE contact_id = $1 AND status = $2 AND external_id = $3`,
		testdata.Cathy.ID, models.CallStatusWired, "call1").Returns(1)

	// back to retry and make the channel inactive
	rt.DB.MustExec(`UPDATE ivr_call SET status = 'E', next_attempt = NOW() WHERE external_id = 'call1';`)
	rt.DB.MustExec(`UPDATE channels_channel SET is_active = FALSE WHERE id = $1`, testdata.TwilioChannel.ID)

	models.FlushCache()
	err = ivrtasks.RetryCalls(ctx, rt)
	assert.NoError(t, err)

	// this time should be failed
	assertdb.Query(t, rt.DB, `SELECT COUNT(*) FROM ivr_call WHERE contact_id = $1 AND status = $2 AND external_id = $3`,
		testdata.Cathy.ID, models.CallStatusFailed, "call1").Returns(1)
}

func TestRetryCallsInWorkerPool(t *testing.T) {
	ctx, rt := testsuite.Runtime()
	rc := rt.RP.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetAll)

	ivr.RegisterServiceType(models.ChannelType("ZZ"), NewMockProvider)

	rt.DB.MustExec(`UPDATE channels_channel SET channel_type = 'ZZ', config = '{"max_concurrent_events": 1}' WHERE id = $1`, testdata.TwilioChannel.ID)

	start := models.NewFlowStart(testdata.Org1.ID, models.StartTypeTrigger, models.FlowTypeVoice, testdata.IVRFlow.ID).
		WithContactIDs([]models.ContactID{testdata.Cathy.ID})

	err := tasks.Queue(rc, queue.BatchQueue, testdata.Org1.ID, &starts.StartFlowTask{FlowStart: start}, queue.DefaultPriority)
	require.NoError(t, err)

	service.callError = nil
	service.callID = ivr.CallID("call1")

	testsuite.FlushTasks(t, rt)

	assertdb.Query(t, rt.DB, `SELECT COUNT(*) FROM ivr_call WHERE contact_id = $1 AND status = $2 AND external_id = $3`,
		testdata.Cathy.ID, models.CallStatusWired, "call1").Returns(1)

	rt.DB.MustExec(`UPDATE ivr_call SET status = 'E', next_attempt = NOW() WHERE external_id = 'call1';`)

	err = ivrtasks.RetryCallsInWorkerPool(ctx, rt)
	assert.NoError(t, err)

	assertdb.Query(t, rt.DB, `SELECT COUNT(*) FROM ivr_call WHERE contact_id = $1 AND status = $2 AND external_id = $3`,
		testdata.Cathy.ID, models.CallStatusWired, "call1").Returns(1)

	rt.DB.MustExec(`UPDATE ivr_call SET status = 'E', next_attempt = NOW() WHERE external_id = 'call1';`)
	rt.DB.MustExec(`UPDATE channels_channel SET is_active = FALSE WHERE id = $1`, testdata.TwilioChannel.ID)

	models.FlushCache()
	err = ivrtasks.RetryCallsInWorkerPool(ctx, rt)
	assert.NoError(t, err)

	assertdb.Query(t, rt.DB, `SELECT COUNT(*) FROM ivr_call WHERE contact_id = $1 AND status = $2 AND external_id = $3`,
		testdata.Cathy.ID, models.CallStatusFailed, "call1").Returns(1)
}

func TestClearConnections(t *testing.T) {
	ctx, rt := testsuite.Runtime()
	rc := rt.RP.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetAll)

	ivr.RegisterServiceType(models.ChannelType("ZZ"), NewMockProvider)

	rt.DB.MustExec(`UPDATE channels_channel SET channel_type = 'ZZ', config = '{"max_concurrent_events": 1}' WHERE id = $1`, testdata.TwilioChannel.ID)

	start := models.NewFlowStart(testdata.Org1.ID, models.StartTypeTrigger, models.FlowTypeVoice, testdata.IVRFlow.ID).
		WithContactIDs([]models.ContactID{testdata.Cathy.ID})

	err := tasks.Queue(rc, queue.BatchQueue, testdata.Org1.ID, &starts.StartFlowTask{FlowStart: start}, queue.DefaultPriority)
	require.NoError(t, err)

	service.callError = nil
	service.callID = ivr.CallID("call1")

	testsuite.FlushTasks(t, rt)

	assertdb.Query(t, rt.DB,
		`SELECT COUNT(*) FROM ivr_call WHERE contact_id = $1 AND status = $2 AND external_id = $3`,
		testdata.Cathy.ID, models.CallStatusWired, "call1",
	).Returns(1)

	rt.DB.MustExec(`UPDATE ivr_call SET modified_on = NOW() - INTERVAL '2 DAY' WHERE contact_id = $1 AND status = $2 AND external_id = $3`,
		testdata.Cathy.ID, models.CallStatusWired, "call1",
	)

	err = ivrtasks.ClearStuckChannelConnections(ctx, rt)
	assert.NoError(t, err)

	assertdb.Query(t, rt.DB,
		`SELECT COUNT(*) FROM ivr_call WHERE contact_id = $1 AND status = $2 AND external_id = $3`,
		testdata.Cathy.ID, models.CallStatusFailed, "call1",
	).Returns(1)
}

func TestUpdateMaxChannelsConnection(t *testing.T) {
	ctx, rt := testsuite.Runtime()
	rc := rt.RP.Get()
	defer rc.Close()

	defer testsuite.Reset(testsuite.ResetAll)

	ivr.RegisterServiceType(models.ChannelType("ZZ"), NewMockProvider)

	rt.DB.MustExec(`UPDATE channels_channel SET channel_type = 'ZZ', config = '{"max_concurrent_events": 1}' WHERE id = $1`, testdata.TwilioChannel.ID)

	err := ivrtasks.ChangeMaxConnectionsConfig(ctx, rt, "ZZ", 0)
	assert.NoError(t, err)
	var confStr string
	err = rt.DB.QueryRowx("SELECT config FROM channels_channel WHERE id = $1", testdata.TwilioChannel.ID).Scan(&confStr)
	assert.NoError(t, err)
	conf := make(map[string]interface{})
	err = json.Unmarshal([]byte(confStr), &conf)
	assert.NoError(t, err)
	assert.Equal(t, 0, int(conf["max_concurrent_events"].(float64)))

	start := models.NewFlowStart(testdata.Org1.ID, models.StartTypeTrigger, models.FlowTypeVoice, testdata.IVRFlow.ID).
		WithContactIDs([]models.ContactID{testdata.Cathy.ID})

	err = tasks.Queue(rc, queue.BatchQueue, testdata.Org1.ID, &starts.StartFlowTask{FlowStart: start}, queue.DefaultPriority)
	require.NoError(t, err)

	service.callError = nil
	service.callID = ivr.CallID("call1")

	testsuite.FlushTasks(t, rt)

	assertdb.Query(t, rt.DB, `SELECT COUNT(*) FROM ivr_call WHERE contact_id = $1 AND status = $2`,
		testdata.Cathy.ID, models.CallStatusQueued).Returns(1)

	err = ivrtasks.ChangeMaxConnectionsConfig(ctx, rt, "ZZ", 500)
	assert.NoError(t, err)
	err = rt.DB.QueryRowx("SELECT config FROM channels_channel WHERE id = $1", testdata.TwilioChannel.ID).Scan(&confStr)
	assert.NoError(t, err)
	conf2 := make(map[string]interface{})
	err = json.Unmarshal([]byte(confStr), &conf2)
	assert.NoError(t, err)
	assert.Equal(t, 500, int(conf2["max_concurrent_events"].(float64)))

	rt.DB.MustExec(`UPDATE ivr_call SET next_attempt = NOW() - INTERVAL '1 MINUTE' WHERE contact_id = $1;`, testdata.Cathy.ID)
	assert.NoError(t, err)

	rt.DB.MustExec("SELECT pg_sleep(5)")

	err = ivrtasks.RetryCalls(ctx, rt)
	assert.NoError(t, err)

	assertdb.Query(t, rt.DB, `SELECT COUNT(*) FROM ivr_call WHERE contact_id = $1 AND status = $2`,
		testdata.Cathy.ID, models.CallStatusWired).Returns(1)
}

func TestSetupLocation(t *testing.T) {
	err := ivrtasks.SetupLocationTimezone(".invalid.")
	assert.Error(t, err)
	assert.ErrorContains(t, err, "unknown time zone .invalid.")

	location := ivrtasks.GetLocationTimezone()
	assert.Nil(t, location)
	timezone := "Asia/Kolkata"
	ivrtasks.SetupLocationTimezone(timezone)
	location = ivrtasks.GetLocationTimezone()
	assert.NotNil(t, location)
	expectedLocation, err := time.LoadLocation("Asia/Kolkata")
	assert.NoError(t, err)
	assert.Equal(t, expectedLocation, location)
}
