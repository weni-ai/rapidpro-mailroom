package models_test

import (
	"net/url"
	"testing"

	"github.com/nyaruka/gocommon/urns"
	"github.com/nyaruka/mailroom/v26/core/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWhatsAppBSUIDHelpers(t *testing.T) {
	bsuid := urns.URN("whatsapp:US.IRLBSUID001")
	phone := urns.URN("whatsapp:5511987654321")
	parent := urns.URN("whatsapp:US.ENT.11815799212886844830")
	legacy := urns.URN("bsuid:US.A1B2C3")

	assert.True(t, models.IsWhatsAppBSUID(bsuid))
	assert.True(t, models.IsWhatsAppBSUID(legacy))
	assert.False(t, models.IsWhatsAppBSUID(phone))
	assert.False(t, models.IsWhatsAppBSUID(parent))

	assert.NoError(t, models.ValidateURN(models.NormalizeURN(bsuid)))
	assert.NoError(t, models.ValidateURN(phone))
	assert.Error(t, models.ValidateURN(parent))

	// Ireland contract: gocommon 1.83 would reject this without the scheme patch
	assert.NoError(t, bsuid.Validate())
	assert.NoError(t, models.ValidateURN(bsuid))

	assert.Equal(t, urns.URN("whatsapp:US.abcDEF123"), models.NormalizeURN("whatsapp:us.abcDEF123"))

	u, err := models.URNFromParts(urns.WhatsApp.Prefix, "US.IRLBSUID001", url.Values{"channel": {"abc"}}, "")
	require.NoError(t, err)
	assert.Equal(t, "whatsapp", u.Scheme())
	assert.Equal(t, "US.IRLBSUID001", u.Path())
}
