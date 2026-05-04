package ivr

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/nyaruka/mailroom/core/ivr"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Job represents a single retry-call job dispatched to a worker. The Conn
// field was previously *models.ChannelConnection but upstream renamed
// ChannelConnection to Call in v8.0.0.
type Job struct {
	Id   int
	Conn *models.Call
}

type JobResult struct {
	Output string
}

func HandleWork(id int, rt *runtime.Runtime, wg *sync.WaitGroup, jobChannel <-chan Job) {
	defer wg.Done()
	lastExecutionTime := time.Now()
	minimumTimeBetweenEachExecution := time.Duration(math.Ceil(1e9 / float64(rt.Config.IVRRetryMaximumExecutionsPerSecond)))

	for job := range jobChannel {
		timeUntilNextExecution := -(time.Since(lastExecutionTime) - minimumTimeBetweenEachExecution)
		if timeUntilNextExecution > 0 {
			logrus.Infof("Worker #%d backing off for %s\n", id, timeUntilNextExecution)
			time.Sleep(timeUntilNextExecution)
		} else {
			logrus.Infof("Worker #%d not backing off \n", id)
		}
		lastExecutionTime = time.Now()
		err := RetryCall(id, rt, job.Conn)
		if err != nil {
			logrus.Error(err)
		}
	}
}

func RetryCall(workerId int, rt *runtime.Runtime, conn *models.Call) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*1)
	defer cancel()
	oa, err := models.GetOrgAssets(ctx, rt, conn.OrgID())
	if err != nil {
		return errors.Wrapf(err, "error loading org with id %v", conn.OrgID())
	}

	channel := oa.ChannelByID(conn.ChannelID())
	if channel == nil {
		err = models.BulkUpdateCallStatuses(ctx, rt.DB, []models.CallID{conn.ID()}, models.CallStatusFailed)
		if err != nil {
			return errors.Wrapf(err, "error marking call as failed due to missing channel with id %v", conn.ChannelID())
		}
		return err
	}

	urn, err := models.URNForID(ctx, rt.DB, oa, conn.ContactURNID())
	if err != nil {
		return errors.Wrapf(err, "unable to load contact urn for urn_id %v", conn.ContactURNID())
	}

	if _, err := ivr.RequestStartForCall(ctx, rt, channel, urn, conn); err != nil {
		return err
	}

	return nil
}
