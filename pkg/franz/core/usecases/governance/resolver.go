// Package governance is the governance application service (003.8): the
// Indicator registry, Policy CRUD and dry run, the event-driven evaluation pass,
// and the whitelisted actions it applies. It orchestrates the domain and the out
// ports; it holds no SQL and no transport types. The caller's realm is read from
// context.
package governance

import (
	"context"
	"strings"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/channel"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/cluster"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/frn"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/indicator"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/topic"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
)

// resource is one governed resource, resolved from the `resource_frn` a sample
// named. Labels is what a Matcher's selector is evaluated against; exactly one
// of the three entity pointers is non-nil.
type resource struct {
	Entity indicator.Entity
	FRN    frn.FRN
	Name   string
	Labels map[string]string

	Channel *channel.AsyncChannel
	Cluster *cluster.Cluster
	Topic   *topic.KafkaTopic
}

// resolver turns a `resource_frn` into the entity behind it. It is deliberately
// read-only: evaluation decides what to change from the resolved resource, and
// the action applier makes the change through the owning service.
type resolver struct {
	channels out.AsyncChannelRepository
	clusters out.ClusterRepository
	topics   out.TopicRepository
}

// resolveFRN loads the resource a sample's `resource_frn` names.
//
// A sample may address a cluster sub-resource — 005 ADR §2.1 publishes
// "<cluster-frn>/broker/3" — which is not itself a Franz resource. The
// sub-resource path is trimmed and the governed parent is resolved, so a policy
// watching a per-broker indicator still acts on the cluster that owns the broker.
func (r resolver) resolveFRN(ctx context.Context, realmID uuid.UUID, raw string) (*resource, error) {
	base, _, _ := strings.Cut(strings.TrimSpace(raw), "/")
	if base == "" {
		return nil, errs.InvalidField("resource_frn", "must not be empty")
	}
	parsed, err := frn.Parse(base)
	if err != nil {
		return nil, errs.InvalidField("resource_frn", "malformed FRN "+raw)
	}

	switch parsed.Type() {
	case frn.TypeAsyncChannel:
		c, err := r.channels.Get(ctx, realmID, parsed.Name())
		if err != nil {
			return nil, err
		}
		return &resource{
			Entity: indicator.EntityAsyncChannel, FRN: c.FRN, Name: c.Name,
			Labels: c.Labels, Channel: c,
		}, nil

	case frn.TypeKafkaCluster:
		c, err := r.clusters.Get(ctx, realmID, parsed.Name())
		if err != nil {
			return nil, err
		}
		return &resource{
			Entity: indicator.EntityKafkaCluster, FRN: c.FRN, Name: c.Name,
			Labels: c.Labels, Cluster: c,
		}, nil

	case frn.TypeKafkaTopic:
		t, err := r.topics.Get(ctx, realmID, parsed.Name())
		if err != nil {
			return nil, err
		}
		// A Kafka Topic carries no label map — neither kafka.proto's KafkaTopic
		// nor the kafka_topic table has one — so only an empty selector can match
		// one. See the owed 003.8 note in whitelist.go's deferredAction.
		return &resource{
			Entity: indicator.EntityKafkaTopic, FRN: t.FRN, Name: t.Name,
			Labels: map[string]string{}, Topic: t,
		}, nil

	default:
		return nil, errs.InvalidField("resource_frn",
			string(parsed.Type())+" is not a governable entity")
	}
}
