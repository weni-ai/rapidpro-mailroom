package ctasks

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/goflow/flows/modifiers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/runner"
	"github.com/nyaruka/mailroom/runtime"
)

// NewURNSpec describes a new URN to add to a contact
type NewURNSpec struct {
	Value  urns.URN `json:"value" validate:"required"`
	Action string   `json:"action" validate:"required,eq=append"`
}

// Apply appends the new URN to the contact. A WhatsApp BSUID owned by a shell contact (no other URNs)
// is reassigned to this contact first so the append can proceed.
func (s *NewURNSpec) Apply(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, scene *runner.Scene, channel *models.Channel) error {
	if urns.IsWhatsAppBSUID(s.Value) {
		ownerID, reassigned, err := models.ReassignShellContactURN(ctx, rt.DB, oa, scene.ContactID(), s.Value)
		if err != nil {
			return fmt.Errorf("error reassigning URN from shell contact: %w", err)
		}
		if reassigned {
			slog.Info("reassigned BSUID URN from shell contact", "urn", s.Value, "contact", scene.ContactUUID(), "shell_id", ownerID)
		} else if ownerID != models.NilContactID {
			slog.Info("BSUID URN not appended because it belongs to a contact with other URNs", "urn", s.Value, "contact", scene.ContactUUID(), "owner_id", ownerID)
		}
	}

	if _, err := scene.ApplyModifier(ctx, rt, oa, modifiers.NewURNs([]urns.URN{s.Value}, modifiers.URNsAppend), models.NilUserID); err != nil {
		return fmt.Errorf("error applying URNs modifier: %w", err)
	}
	return nil
}
