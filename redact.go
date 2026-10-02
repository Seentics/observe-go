package observe

import "strings"

// defaultRedact names the fields most likely to carry a secret. Matching is by
// field name, case-insensitive, at any depth.
var defaultRedact = []string{
	"password", "passwd", "secret", "token", "access_token", "refresh_token",
	"authorization", "cookie", "set-cookie", "api_key", "apikey", "x-api-key",
	"private_key", "client_secret", "session",
}

const redacted = "[Redacted]"

// redactFields returns a copy of fields with every value under a redacted
// name replaced, descending into nested maps and slices. The caller's map is
// never modified.
func redactFields(fields map[string]any, names map[string]struct{}) map[string]any {
	if len(names) == 0 {
		return fields
	}
	out := make(map[string]any, len(fields))
	for k, v := range fields {
		if _, hit := names[strings.ToLower(k)]; hit {
			out[k] = redacted
			continue
		}
		out[k] = redactValue(v, names)
	}
	return out
}

func redactValue(v any, names map[string]struct{}) any {
	switch t := v.(type) {
	case map[string]any:
		return redactFields(t, names)
	case Fields:
		return redactFields(t, names)
	case []any:
		out := make([]any, len(t))
		for i, item := range t {
			out[i] = redactValue(item, names)
		}
		return out
	default:
		return v
	}
}

func nameSet(names []string) map[string]struct{} {
	set := make(map[string]struct{}, len(names))
	for _, n := range names {
		set[strings.ToLower(n)] = struct{}{}
	}
	return set
}
