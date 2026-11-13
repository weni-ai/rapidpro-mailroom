package models

import (
	"net/url"
	"regexp"
	"strings"

	"github.com/nyaruka/gocommon/urns"
)

// Ireland stores WhatsApp BSUIDs on the whatsapp: scheme (phone OR CC.alphanumeric), matching
// RapidPro main / gocommon v1.75.7. Upstream gocommon v1.83.0 (mailroom 26.2) only allows digits
// on whatsapp: and moved BSUID to the bsuid: scheme. These helpers restore the Ireland contract
// without pinning gocommon back.
var whatsAppBSUIDPathRegex = regexp.MustCompile(`^[A-Z]{2}\.[a-zA-Z0-9]{1,128}$`)

func init() {
	// Mutating the registered scheme makes urns.Validate / NewFromParts / goflow Encode
	// accept Ireland whatsapp:US.… paths. WhatsApp is a pointer shared with schemeByPrefix.
	origValidate := urns.WhatsApp.Validate
	urns.WhatsApp.Validate = func(path string) bool {
		if origValidate != nil && origValidate(path) {
			return true
		}
		return isWhatsAppBSUIDPath(path)
	}
	urns.WhatsApp.Normalize = func(path string) string {
		return normalizeWhatsAppBSUIDPath(path)
	}
}

func normalizeWhatsAppBSUIDPath(path string) string {
	path = strings.TrimSpace(path)
	if len(path) > 2 && path[2] == '.' {
		return strings.ToUpper(path[:2]) + path[2:]
	}
	return path
}

func isWhatsAppBSUIDPath(path string) bool {
	return whatsAppBSUIDPathRegex.MatchString(normalizeWhatsAppBSUIDPath(path))
}

// IsWhatsAppBSUID reports whether u is a WhatsApp business-scoped user id, either on whatsapp:
// (Ireland) or the upstream bsuid: scheme.
func IsWhatsAppBSUID(u urns.URN) bool {
	switch u.Scheme() {
	case urns.WhatsApp.Prefix, urns.BSUID.Prefix:
		return isWhatsAppBSUIDPath(u.Path())
	default:
		return false
	}
}

// NormalizeURN is gocommon Normalize plus uppercasing the CC on a whatsapp: BSUID path.
func NormalizeURN(u urns.URN) urns.URN {
	return u.Normalize()
}

// ValidateURN is gocommon Validate, but whatsapp: BSUID paths are accepted (Ireland).
func ValidateURN(u urns.URN) error {
	return u.Validate()
}

// URNFromParts is urns.NewFromParts, with a fallback for whatsapp: BSUID paths that gocommon 1.83 rejects
// if the scheme patch has not run.
func URNFromParts(scheme, path string, query url.Values, display string) (urns.URN, error) {
	u, err := urns.NewFromParts(scheme, path, query, display)
	if err == nil {
		return u, nil
	}

	path = normalizeWhatsAppBSUIDPath(path)
	if scheme != urns.WhatsApp.Prefix || !isWhatsAppBSUIDPath(path) {
		return urns.NilURN, err
	}

	raw := scheme + ":" + path
	if enc := query.Encode(); enc != "" {
		raw += "?" + enc
	}
	if display != "" {
		raw += "#" + display
	}
	parsed, perr := urns.Parse(raw)
	if perr != nil {
		return urns.NilURN, err
	}
	return parsed, nil
}
