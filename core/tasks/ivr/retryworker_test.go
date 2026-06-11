package ivr_test

import (
	"sync"
	"testing"

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

func TestHandleWork(t *testing.T) {
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

	conns, err := models.LoadCallsToRetry(ctx, rt.DB, 1)
	assert.NoError(t, err)
	assert.Equal(t, len(conns), 1)

	err = ivrtasks.RetryCall(0, rt, conns[0])
	assert.NoError(t, err)

	assertdb.Query(t, rt.DB, `SELECT COUNT(*) FROM ivr_call WHERE contact_id = $1 AND status = $2 AND external_id = $3`,
		testdata.Cathy.ID, models.CallStatusWired, "call1").Returns(1)

	rt.DB.MustExec(`UPDATE ivr_call SET status = 'E', next_attempt = NOW() WHERE external_id = 'call1';`)
	rt.DB.MustExec(`UPDATE channels_channel SET is_active = FALSE WHERE id = $1`, testdata.TwilioChannel.ID)

	models.FlushCache()

	conns, err = models.LoadCallsToRetry(ctx, rt.DB, 1)
	assert.NoError(t, err)
	assert.Equal(t, len(conns), 1)

	jobs := []ivrtasks.Job{{Id: 1, Conn: conns[0]}}

	var (
		wg         sync.WaitGroup
		jobChannel = make(chan ivrtasks.Job)
	)
	wg.Add(1)

	go ivrtasks.HandleWork(0, rt, &wg, jobChannel)

	for _, job := range jobs {
		jobChannel <- job
	}

	close(jobChannel)
	wg.Wait()

	assertdb.Query(t, rt.DB, `SELECT COUNT(*) FROM ivr_call WHERE contact_id = $1 AND status = $2 AND external_id = $3`,
		testdata.Cathy.ID, models.CallStatusFailed, "call1").Returns(1)
}
