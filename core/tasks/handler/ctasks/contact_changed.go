package ctasks

import (
	"context"
	"fmt"

	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/tasks/handler"
	"github.com/nyaruka/mailroom/runtime"
)

const TypeContactChanged = "contact_changed"

func init() {
	handler.RegisterContactTask(TypeContactChanged, func() handler.Task { return &ContactChangedTask{} })
}

type ContactChangedTask struct {
	ChannelID models.ChannelID `json:"channel_id"`
	NewURN    *NewURNSpec      `json:"new_urn,omitempty"`
}

func (t *ContactChangedTask) Type() string {
	return TypeContactChanged
}

func (t *ContactChangedTask) UseReadOnly() bool {
	return false
}

func (t *ContactChangedTask) Perform(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, contact *models.Contact) error {
	flowContact, err := contact.FlowContact(oa)
	if err != nil {
		return fmt.Errorf("error creating flow contact: %w", err)
	}

	if t.NewURN != nil {
		if err := t.NewURN.Apply(ctx, rt, oa, contact, flowContact, oa.ChannelByID(t.ChannelID)); err != nil {
			return fmt.Errorf("error applying new URN: %w", err)
		}
	}

	return nil
}
