package governance

import (
	"time"

	"github.com/google/uuid"
)

// ActionRecord is the audit row of one automated change (003.8 `PolicyAction`):
// "every automated change is a PolicyAction; there is no silent mutation". The
// series is append-only and pruned nightly at 30 days, like every other Franz
// time series (003.12 / 003.14).
//
// PolicyName is denormalised alongside PolicyID and there is no foreign key to
// `policy`, so deleting a policy does not erase the record of what it did.
type ActionRecord struct {
	ID      uuid.UUID // surrogate key; assigned by the repository on Append
	RealmID uuid.UUID

	PolicyID   uuid.UUID
	PolicyName string

	OccurredAt time.Time
	// ResourceFRN is the resource the action changed, rendered prefix-less.
	ResourceFRN string
	// IndicatorValue is the sample that made the policy trigger, verbatim.
	IndicatorValue string
	Action         Action
	// Result is the outcome in operator-facing prose: the new field value, a
	// "capped at …" note, "no change", or the reason the action failed.
	Result string

	ReceivedAt time.Time
}

// NewActionRecord builds one audit row. occurredAt is Franz's clock — an action
// is something Franz did, so there is no external timestamp to trust.
func NewActionRecord(
	realmID uuid.UUID, policyID uuid.UUID, policyName string,
	resourceFRN, indicatorValue string, action Action, result string,
	occurredAt time.Time,
) *ActionRecord {
	return &ActionRecord{
		RealmID:        realmID,
		PolicyID:       policyID,
		PolicyName:     policyName,
		OccurredAt:     occurredAt.UTC(),
		ResourceFRN:    resourceFRN,
		IndicatorValue: indicatorValue,
		Action:         action,
		Result:         result,
		ReceivedAt:     occurredAt.UTC(),
	}
}
