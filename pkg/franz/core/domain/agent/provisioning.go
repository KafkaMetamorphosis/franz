package agent

import (
	"strings"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
)

// provisioningKeyPrefix is the reserved-label namespace a provisioning-label
// spec key must sit under (003.1, ADR-006).
const provisioningKeyPrefix = "franz."

// ProvisioningLabelSpec describes one franz.provisioning/* label an agent's
// recipe reads. It is advisory (003.9, ADR-API-008): Franz stores and serves it
// for the console to render resource forms, and validates only its own
// well-formedness — never another resource's labels against it.
type ProvisioningLabelSpec struct {
	Key           string
	Description   string
	AllowedValues []string
	DefaultValue  string
	Required      bool
}

// ValidateProvisioningLabels checks a spec list for well-formedness: every key
// is non-empty and franz.-prefixed, keys are unique, and a spec that sets both
// DefaultValue and AllowedValues has the default among the allowed values. An
// empty list is valid.
func ValidateProvisioningLabels(specs []ProvisioningLabelSpec) error {
	seen := make(map[string]bool, len(specs))
	for _, s := range specs {
		if s.Key == "" {
			return errs.InvalidField("provisioning_labels", "spec has an empty key")
		}
		if !strings.HasPrefix(s.Key, provisioningKeyPrefix) {
			return errs.InvalidField("provisioning_labels", "key "+s.Key+" must be under the franz. reserved namespace")
		}
		if seen[s.Key] {
			return errs.InvalidField("provisioning_labels", "duplicate key "+s.Key)
		}
		seen[s.Key] = true
		if s.DefaultValue != "" && len(s.AllowedValues) > 0 && !contains(s.AllowedValues, s.DefaultValue) {
			return errs.InvalidField("provisioning_labels", "default value "+s.DefaultValue+" for key "+s.Key+" is not in allowed_values")
		}
	}
	return nil
}

func contains(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}
