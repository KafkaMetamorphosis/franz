package indicator

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/frn"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/naming"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
)

// Health is the derived freshness of an indicator (003.14). It is never stored:
// Health(now) computes it from last_sample_at and staleness_threshold on every
// read.
type Health string

const (
	HealthHealthy Health = "HEALTHY"
	HealthStale   Health = "STALE"
)

// maxSourceAgents bounds the declarative producer list so a single row cannot
// grow without limit. 003.14 fixes no number; this is defensive.
const maxSourceAgents = 64

// Indicator is the pre-registered signal a Telemetry Agent publishes samples for
// and a governance Policy watches (003.14 "Indicators are pre-registered").
// Registration is an admin action; there is no auto-creation.
//
// CurrentValue / CurrentResourceFRN / LastSampleAt are the ingest-maintained
// projection of the newest sample. Deliverable 14 owns the columns and derives
// Health from them; deliverable 15's ingest path writes them.
type Indicator struct {
	ID      uuid.UUID // surrogate key; assigned by the repository on Create
	FRN     frn.FRN
	RealmID uuid.UUID

	Name string
	Unit Unit
	// AppliesTo is immutable once registered (003.14): a policy's
	// matcher.entity must agree with it, and changing it would silently
	// invalidate every policy and every stored sample.
	AppliesTo Entity
	// StalenessThreshold is the parsed form of StalenessSpec.
	StalenessThreshold time.Duration
	// StalenessSpec is the operator-authored text ("90d", "5m") kept verbatim so
	// a read returns what was written (003.8 renders it on Indicator).
	StalenessSpec string
	SourceAgents  []string

	// CurrentValue is the newest sample's value, CurrentResourceFRN the resource
	// it described, LastSampleAt its sample_at. Nil / empty until the first
	// sample lands.
	CurrentValue       string
	CurrentResourceFRN string
	LastSampleAt       *time.Time

	CreatedAt time.Time
	UpdatedAt time.Time
}

// NewIndicator registers an indicator, validating the name, unit, entity and
// staleness threshold. realm supplies the FRN realm segment and the realm_id.
func NewIndicator(
	r realm.Realm,
	name string, unit Unit, appliesTo Entity, stalenessSpec string, sourceAgents []string,
) (*Indicator, error) {
	id, err := frn.New(r.Slug, frn.TypeIndicator, name)
	if err != nil {
		return nil, err
	}
	i := &Indicator{FRN: id, RealmID: r.ID, Name: name, AppliesTo: appliesTo}
	if !appliesTo.Valid() {
		return nil, errs.InvalidField("applies_to",
			"must be one of ASYNC_CHANNEL, KAFKA_TOPIC, KAFKA_CLUSTER")
	}
	if err := i.SetUnit(unit); err != nil {
		return nil, err
	}
	if err := i.SetStalenessThreshold(stalenessSpec); err != nil {
		return nil, err
	}
	if err := i.SetSourceAgents(sourceAgents); err != nil {
		return nil, err
	}
	return i, nil
}

// SetUnit replaces the unit. It is maskable on UpdateIndicator — a unit change
// re-interprets stored values and every policy limit bound to this indicator,
// which is the operator's call to make.
func (i *Indicator) SetUnit(u Unit) error {
	if err := u.Validate(); err != nil {
		return err
	}
	i.Unit = u
	return nil
}

// SetStalenessThreshold parses and stores the threshold. Maskable on
// UpdateIndicator.
func (i *Indicator) SetStalenessThreshold(spec string) error {
	d, err := ParseDurationSpec(spec)
	if err != nil {
		return err
	}
	if d <= 0 {
		return errs.InvalidField("staleness_threshold", "must be positive")
	}
	i.StalenessThreshold = d
	i.StalenessSpec = strings.TrimSpace(spec)
	return nil
}

// SetSourceAgents replaces the declarative producer list. Maskable on
// UpdateIndicator. 003.14 OQ5 (auto-append on first sample) stays open — the
// list is purely declarative here.
func (i *Indicator) SetSourceAgents(agents []string) error {
	if len(agents) > maxSourceAgents {
		return errs.InvalidField("source_agents",
			fmt.Sprintf("at most %d entries", maxSourceAgents))
	}
	out := make([]string, 0, len(agents))
	for _, a := range agents {
		a = strings.TrimSpace(a)
		if a == "" {
			return errs.InvalidField("source_agents", "must not contain an empty name")
		}
		if err := naming.Validate(a); err != nil {
			return errs.InvalidField("source_agents", "invalid agent name "+a)
		}
		out = append(out, a)
	}
	i.SourceAgents = out
	return nil
}

// Health derives freshness at instant now (003.14). An indicator that has never
// been sampled is STALE, not HEALTHY: there is no value for a policy to compare,
// and treating "no data" as healthy would let a policy fire off a stale reading
// the moment one arrived late.
func (i *Indicator) Health(now time.Time) Health {
	if i.LastSampleAt == nil || i.LastSampleAt.IsZero() {
		return HealthStale
	}
	if now.Sub(*i.LastSampleAt) > i.StalenessThreshold {
		return HealthStale
	}
	return HealthHealthy
}

// EnsureSampleEntity rejects a sample whose resource_entity disagrees with
// applies_to (003.14).
func (i *Indicator) EnsureSampleEntity(e Entity) error {
	if e != i.AppliesTo {
		return errs.InvalidField("resource_entity",
			fmt.Sprintf("indicator %q applies to %s, not %s", i.Name, i.AppliesTo, e))
	}
	return nil
}

// dayWeekRe matches the day / week suffixes Go's time.ParseDuration does not
// understand. 003.8 writes thresholds like "90d".
var dayWeekRe = regexp.MustCompile(`^([0-9]+(?:\.[0-9]+)?)([dw])$`)

// ParseDurationSpec parses a threshold / duration string. It accepts Go's
// duration grammar ("5m", "1h30m", "250ms") plus the day and week suffixes
// ("90d", "2w") the specs use, which time.ParseDuration rejects.
func ParseDurationSpec(spec string) (time.Duration, error) {
	s := strings.TrimSpace(spec)
	if s == "" {
		return 0, errs.InvalidField("staleness_threshold", "must not be empty")
	}
	if m := dayWeekRe.FindStringSubmatch(s); m != nil {
		n, err := strconv.ParseFloat(m[1], 64)
		if err != nil {
			return 0, errs.InvalidField("staleness_threshold", "malformed duration "+spec)
		}
		unit := 24 * time.Hour
		if m[2] == "w" {
			unit = 7 * 24 * time.Hour
		}
		return time.Duration(n * float64(unit)), nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, errs.InvalidField("staleness_threshold",
			"must be a duration such as 90d, 12h or 5m (got "+spec+")")
	}
	return d, nil
}

// ValidateName is exposed so a handler can reject a bad indicator name before
// touching the store.
func ValidateName(name string) error { return naming.Validate(name) }
