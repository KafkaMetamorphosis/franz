package governance_test

import (
	"context"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/accesspolicy"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/channel"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/cluster"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	gov "github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/governance"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/indicator"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/topic"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/in"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
)

// The fakes below are in-memory stand-ins for the out ports and the three entity
// services. They record what the governance service and evaluator asked for, so
// a test can assert on the *absence* of a write as easily as on its presence —
// which is what "a dry run mutates nothing" requires.

func ctxWithRealm() context.Context {
	return realm.NewContext(context.Background(),
		realm.Realm{ID: realm.DefaultID, Slug: realm.DefaultSlug})
}

// --- IndicatorRepository -------------------------------------------------

type fakeIndicatorRepo struct {
	rows    map[string]*indicator.Indicator
	deleted []string
}

func newIndicatorRepo(indicators ...*indicator.Indicator) *fakeIndicatorRepo {
	r := &fakeIndicatorRepo{rows: map[string]*indicator.Indicator{}}
	for _, i := range indicators {
		r.rows[i.Name] = i
	}
	return r
}

var _ out.IndicatorRepository = (*fakeIndicatorRepo)(nil)

func (f *fakeIndicatorRepo) Create(_ context.Context, i *indicator.Indicator) error {
	if _, ok := f.rows[i.Name]; ok {
		return errs.Existsf("indicator %q already exists", i.Name)
	}
	i.ID = uuid.New()
	f.rows[i.Name] = i
	return nil
}

func (f *fakeIndicatorRepo) Get(
	_ context.Context, _ uuid.UUID, name string,
) (*indicator.Indicator, error) {
	i, ok := f.rows[name]
	if !ok {
		return nil, errs.NotFoundf("indicator %q not found", name)
	}
	return i, nil
}

func (f *fakeIndicatorRepo) List(_ context.Context, q out.IndicatorQuery) (out.IndicatorPage, error) {
	names := make([]string, 0, len(f.rows))
	for name := range f.rows {
		if name > q.AfterName {
			names = append(names, name)
		}
	}
	sort.Strings(names)

	var page out.IndicatorPage
	for _, name := range names {
		page.Indicators = append(page.Indicators, f.rows[name])
	}
	if q.Limit > 0 && len(page.Indicators) > q.Limit {
		page.Indicators = page.Indicators[:q.Limit]
		page.LastName = page.Indicators[q.Limit-1].Name
	}
	return page, nil
}

func (f *fakeIndicatorRepo) Mutate(
	_ context.Context, _ uuid.UUID, name string, mutate func(*indicator.Indicator) error,
) (*indicator.Indicator, error) {
	i, ok := f.rows[name]
	if !ok {
		return nil, errs.NotFoundf("indicator %q not found", name)
	}
	// The real repository never writes applies_to back (003.14). Snapshotting it
	// here reproduces that, so a mutate that changes it is invisible to the caller
	// exactly as it would be against Postgres.
	appliesTo := i.AppliesTo
	if err := mutate(i); err != nil {
		return nil, err
	}
	i.AppliesTo = appliesTo
	return i, nil
}

func (f *fakeIndicatorRepo) Delete(_ context.Context, _ uuid.UUID, name string) error {
	if _, ok := f.rows[name]; !ok {
		return errs.NotFoundf("indicator %q not found", name)
	}
	delete(f.rows, name)
	f.deleted = append(f.deleted, name)
	return nil
}

func (f *fakeIndicatorRepo) RecordSample(
	_ context.Context, _ uuid.UUID, name, resourceFRN, value string, sampleAt time.Time,
) (bool, error) {
	i, ok := f.rows[name]
	if !ok {
		return false, errs.NotFoundf("indicator %q not found", name)
	}
	if i.LastSampleAt != nil && !sampleAt.After(*i.LastSampleAt) {
		return false, nil
	}
	at := sampleAt.UTC()
	i.CurrentValue, i.CurrentResourceFRN, i.LastSampleAt = value, resourceFRN, &at
	return true, nil
}

// --- PolicyRepository ----------------------------------------------------

type fakePolicyRepo struct {
	rows  map[string]*gov.Policy
	fired []string
}

func newPolicyRepo(policies ...*gov.Policy) *fakePolicyRepo {
	r := &fakePolicyRepo{rows: map[string]*gov.Policy{}}
	for _, p := range policies {
		if p.ID == uuid.Nil {
			p.ID = uuid.New()
		}
		r.rows[p.Name] = p
	}
	return r
}

var _ out.PolicyRepository = (*fakePolicyRepo)(nil)

func (f *fakePolicyRepo) Create(_ context.Context, p *gov.Policy) error {
	if _, ok := f.rows[p.Name]; ok {
		return errs.Existsf("policy %q already exists", p.Name)
	}
	p.ID = uuid.New()
	f.rows[p.Name] = p
	return nil
}

func (f *fakePolicyRepo) Get(_ context.Context, _ uuid.UUID, name string) (*gov.Policy, error) {
	p, ok := f.rows[name]
	if !ok {
		return nil, errs.NotFoundf("policy %q not found", name)
	}
	return p, nil
}

func (f *fakePolicyRepo) List(_ context.Context, q out.PolicyQuery) (out.PolicyPage, error) {
	names := make([]string, 0, len(f.rows))
	for name := range f.rows {
		if name > q.AfterName {
			names = append(names, name)
		}
	}
	sort.Strings(names)

	var page out.PolicyPage
	for _, name := range names {
		page.Policies = append(page.Policies, f.rows[name])
	}
	if q.Limit > 0 && len(page.Policies) > q.Limit {
		page.Policies = page.Policies[:q.Limit]
		page.LastName = page.Policies[q.Limit-1].Name
	}
	return page, nil
}

func (f *fakePolicyRepo) Mutate(
	_ context.Context, _ uuid.UUID, name string, mutate func(*gov.Policy) error,
) (*gov.Policy, error) {
	p, ok := f.rows[name]
	if !ok {
		return nil, errs.NotFoundf("policy %q not found", name)
	}
	if err := mutate(p); err != nil {
		return nil, err
	}
	return p, nil
}

func (f *fakePolicyRepo) Delete(_ context.Context, _ uuid.UUID, name string) error {
	if _, ok := f.rows[name]; !ok {
		return errs.NotFoundf("policy %q not found", name)
	}
	delete(f.rows, name)
	return nil
}

func (f *fakePolicyRepo) ListEnabledByIndicator(
	_ context.Context, _ uuid.UUID, indicatorName string,
) ([]*gov.Policy, error) {
	names := make([]string, 0, len(f.rows))
	for name, p := range f.rows {
		if p.Enabled && p.Indicator == indicatorName {
			names = append(names, name)
		}
	}
	sort.Strings(names)

	policies := make([]*gov.Policy, 0, len(names))
	for _, name := range names {
		policies = append(policies, f.rows[name])
	}
	return policies, nil
}

func (f *fakePolicyRepo) CountByIndicator(
	_ context.Context, _ uuid.UUID, indicatorName string,
) (int, error) {
	n := 0
	for _, p := range f.rows {
		if p.Indicator == indicatorName {
			n++
		}
	}
	return n, nil
}

func (f *fakePolicyRepo) MarkFired(_ context.Context, _ uuid.UUID, name string, at time.Time) error {
	p, ok := f.rows[name]
	if !ok {
		return errs.NotFoundf("policy %q not found", name)
	}
	p.MarkFired(at)
	f.fired = append(f.fired, name)
	return nil
}

// --- PolicyActionRepository ----------------------------------------------

type fakeActionRepo struct {
	records []*gov.ActionRecord
}

var _ out.PolicyActionRepository = (*fakeActionRepo)(nil)

func (f *fakeActionRepo) Append(_ context.Context, records []*gov.ActionRecord) error {
	f.records = append(f.records, records...)
	return nil
}

func (f *fakeActionRepo) List(
	_ context.Context, q out.PolicyActionQuery,
) (out.PolicyActionPage, error) {
	var page out.PolicyActionPage
	for _, rec := range f.records {
		if rec.PolicyName == q.PolicyName {
			page.Actions = append(page.Actions, rec)
		}
	}
	return page, nil
}

func (f *fakeActionRepo) PruneOlderThan(_ context.Context, cutoff time.Time) (int64, error) {
	kept := f.records[:0]
	var removed int64
	for _, rec := range f.records {
		if rec.OccurredAt.Before(cutoff) {
			removed++
			continue
		}
		kept = append(kept, rec)
	}
	f.records = kept
	return removed, nil
}

// --- IndicatorSampleRepository -------------------------------------------

type fakeSampleRepo struct {
	samples []*indicator.Sample
}

var _ out.IndicatorSampleRepository = (*fakeSampleRepo)(nil)

func (f *fakeSampleRepo) Append(_ context.Context, samples []*indicator.Sample) (int, error) {
	f.samples = append(f.samples, samples...)
	return len(samples), nil
}

func (f *fakeSampleRepo) PruneOlderThan(context.Context, time.Time) (int64, error) { return 0, nil }

func (f *fakeSampleRepo) List(_ context.Context, q out.SampleQuery) (out.SamplePage, error) {
	var page out.SamplePage
	for _, s := range f.samples {
		if s.Indicator != q.Indicator {
			continue
		}
		if q.ResourceFRN != "" && s.ResourceFRN != q.ResourceFRN {
			continue
		}
		page.Samples = append(page.Samples, s)
	}
	return page, nil
}

func (f *fakeSampleRepo) LatestPerResource(
	_ context.Context, _ uuid.UUID, name string, _ int,
) ([]*indicator.Sample, error) {
	latest := map[string]*indicator.Sample{}
	for _, s := range f.samples {
		if s.Indicator != name {
			continue
		}
		if prev, ok := latest[s.ResourceFRN]; !ok || s.SampleAt.After(prev.SampleAt) {
			latest[s.ResourceFRN] = s
		}
	}
	frns := make([]string, 0, len(latest))
	for frn := range latest {
		frns = append(frns, frn)
	}
	sort.Strings(frns)

	out := make([]*indicator.Sample, 0, len(frns))
	for _, frn := range frns {
		out = append(out, latest[frn])
	}
	return out, nil
}

// --- entity repositories (read side) -------------------------------------

type fakeChannelRepo struct {
	rows map[string]*channel.AsyncChannel
}

var _ out.AsyncChannelRepository = (*fakeChannelRepo)(nil)

func (f *fakeChannelRepo) Create(context.Context, *channel.AsyncChannel) error { return nil }

func (f *fakeChannelRepo) Get(
	_ context.Context, _ uuid.UUID, name string,
) (*channel.AsyncChannel, error) {
	c, ok := f.rows[name]
	if !ok {
		return nil, errs.NotFoundf("async channel %q not found", name)
	}
	return c, nil
}

func (f *fakeChannelRepo) List(context.Context, out.ChannelQuery) (out.ChannelPage, error) {
	return out.ChannelPage{}, nil
}

func (f *fakeChannelRepo) ListActive(context.Context, uuid.UUID) ([]*channel.AsyncChannel, error) {
	return nil, nil
}

func (f *fakeChannelRepo) ListUnderplaced(context.Context) ([]*channel.AsyncChannel, error) {
	return nil, nil
}

func (f *fakeChannelRepo) Mutate(
	_ context.Context, _ uuid.UUID, name string, mutate func(*channel.AsyncChannel) error,
) (*channel.AsyncChannel, error) {
	c, ok := f.rows[name]
	if !ok {
		return nil, errs.NotFoundf("async channel %q not found", name)
	}
	if err := mutate(c); err != nil {
		return nil, err
	}
	return c, nil
}

func (f *fakeChannelRepo) MutateWithShards(
	_ context.Context, _ uuid.UUID, name string,
	mutate func(*channel.AsyncChannel, []*topic.KafkaTopic) error,
) (*channel.AsyncChannel, error) {
	c, ok := f.rows[name]
	if !ok {
		return nil, errs.NotFoundf("async channel %q not found", name)
	}
	if err := mutate(c, nil); err != nil {
		return nil, err
	}
	return c, nil
}

type fakeClusterRepo struct {
	rows map[string]*cluster.Cluster
}

var _ out.ClusterRepository = (*fakeClusterRepo)(nil)

func (f *fakeClusterRepo) Create(context.Context, *cluster.Cluster) error { return nil }

func (f *fakeClusterRepo) Get(_ context.Context, _ uuid.UUID, name string) (*cluster.Cluster, error) {
	c, ok := f.rows[name]
	if !ok {
		return nil, errs.NotFoundf("kafka cluster %q not found", name)
	}
	return c, nil
}

func (f *fakeClusterRepo) List(context.Context, out.ClusterQuery) (out.ClusterPage, error) {
	return out.ClusterPage{}, nil
}

func (f *fakeClusterRepo) Mutate(
	_ context.Context, _ uuid.UUID, name string, mutate func(*cluster.Cluster) error,
) (*cluster.Cluster, error) {
	c, ok := f.rows[name]
	if !ok {
		return nil, errs.NotFoundf("kafka cluster %q not found", name)
	}
	if err := mutate(c); err != nil {
		return nil, err
	}
	return c, nil
}

func (f *fakeClusterRepo) ListAll(context.Context, uuid.UUID) ([]*cluster.Cluster, error) {
	return nil, nil
}

func (f *fakeClusterRepo) ListByProviderAgent(
	context.Context, uuid.UUID, string,
) ([]*cluster.Cluster, error) {
	return nil, nil
}

type fakeTopicRepo struct {
	rows map[string]*topic.KafkaTopic
}

var _ out.TopicRepository = (*fakeTopicRepo)(nil)

func (f *fakeTopicRepo) Create(context.Context, *topic.KafkaTopic) error { return nil }

func (f *fakeTopicRepo) Get(_ context.Context, _ uuid.UUID, name string) (*topic.KafkaTopic, error) {
	t, ok := f.rows[name]
	if !ok {
		return nil, errs.NotFoundf("kafka topic %q not found", name)
	}
	return t, nil
}

func (f *fakeTopicRepo) List(context.Context, out.TopicQuery) (out.TopicPage, error) {
	return out.TopicPage{}, nil
}

func (f *fakeTopicRepo) MutateChannelShards(
	context.Context, uuid.UUID, uuid.UUID, func([]*topic.KafkaTopic) error,
) ([]*topic.KafkaTopic, error) {
	return nil, nil
}

func (f *fakeTopicRepo) PlaceChannelShards(
	context.Context, uuid.UUID, uuid.UUID, func([]*topic.KafkaTopic) (out.ShardPlan, error),
) ([]*topic.KafkaTopic, error) {
	return nil, nil
}

func (f *fakeTopicRepo) ResolveChannelID(context.Context, uuid.UUID, string) (uuid.UUID, error) {
	return uuid.Nil, nil
}

func (f *fakeTopicRepo) CountLiveTopics(context.Context, uuid.UUID) (int, error) { return 0, nil }

func (f *fakeTopicRepo) ListByClusters(
	context.Context, uuid.UUID, []uuid.UUID,
) ([]*topic.KafkaTopic, error) {
	return nil, nil
}

func (f *fakeTopicRepo) MutateByFRN(
	_ context.Context, _ uuid.UUID, frnPath string, mutate func(*topic.KafkaTopic) error,
) (*topic.KafkaTopic, error) {
	for _, t := range f.rows {
		if t.FRN.Path() != frnPath {
			continue
		}
		if err := mutate(t); err != nil {
			return nil, err
		}
		return t, nil
	}
	return nil, errs.NotFoundf("kafka topic %q not found", frnPath)
}

// --- entity services (write side) ----------------------------------------

// serviceCall is one write the applier asked an entity service to make. Tests
// assert on the recorded sequence.
type serviceCall struct {
	Entity string
	Op     string
	Name   string
	Labels map[string]string
	Extra  string
}

type fakeChannelSvc struct {
	repo  *fakeChannelRepo
	calls *[]serviceCall
}

var _ in.AsyncChannelService = (*fakeChannelSvc)(nil)

func (f fakeChannelSvc) Create(context.Context, in.CreateChannelInput) (*channel.AsyncChannel, error) {
	return nil, nil
}

func (f fakeChannelSvc) Get(ctx context.Context, name string) (*channel.AsyncChannel, error) {
	return f.repo.Get(ctx, uuid.Nil, name)
}

func (f fakeChannelSvc) List(context.Context, in.ListChannelsInput) (in.ChannelPage, error) {
	return in.ChannelPage{}, nil
}

func (f fakeChannelSvc) Update(
	_ context.Context, input in.UpdateChannelInput,
) (*channel.AsyncChannel, error) {
	call := serviceCall{Entity: "channel", Op: "update", Name: input.Name}
	if input.Labels != nil {
		call.Labels = *input.Labels
		if c, ok := f.repo.rows[input.Name]; ok {
			c.Labels = *input.Labels
		}
	}
	*f.calls = append(*f.calls, call)
	return f.repo.rows[input.Name], nil
}

func (f fakeChannelSvc) SetAccessPolicy(
	context.Context, string, accesspolicy.Policy,
) (*channel.AsyncChannel, error) {
	return nil, nil
}

func (f fakeChannelSvc) Delete(_ context.Context, name string) error {
	*f.calls = append(*f.calls, serviceCall{Entity: "channel", Op: "delete", Name: name})
	return nil
}

func (f fakeChannelSvc) Pause(_ context.Context, name string) (*channel.AsyncChannel, error) {
	*f.calls = append(*f.calls, serviceCall{Entity: "channel", Op: "pause", Name: name})
	return f.repo.rows[name], nil
}

func (f fakeChannelSvc) Resume(_ context.Context, name string) (*channel.AsyncChannel, error) {
	*f.calls = append(*f.calls, serviceCall{Entity: "channel", Op: "resume", Name: name})
	return f.repo.rows[name], nil
}

type fakeClusterSvc struct {
	repo  *fakeClusterRepo
	calls *[]serviceCall
}

var _ in.KafkaClusterService = (*fakeClusterSvc)(nil)

func (f fakeClusterSvc) Create(context.Context, in.CreateClusterInput) (*cluster.Cluster, error) {
	return nil, nil
}

func (f fakeClusterSvc) Get(ctx context.Context, name string) (*cluster.Cluster, error) {
	return f.repo.Get(ctx, uuid.Nil, name)
}

func (f fakeClusterSvc) List(context.Context, in.ListClustersInput) (in.ClusterPage, error) {
	return in.ClusterPage{}, nil
}

func (f fakeClusterSvc) Update(
	_ context.Context, input in.UpdateClusterInput,
) (*cluster.Cluster, error) {
	call := serviceCall{Entity: "cluster", Op: "update", Name: input.Name}
	c := f.repo.rows[input.Name]
	switch {
	case input.Labels != nil:
		call.Labels = *input.Labels
		if c != nil {
			c.Labels = *input.Labels
		}
	case input.Configuration != nil:
		call.Labels = *input.Configuration
		if c != nil {
			c.Configuration = *input.Configuration
		}
	case input.Brokers != nil:
		call.Extra = "brokers"
		if c != nil {
			c.Brokers = *input.Brokers
		}
	case input.DiskSize != nil:
		call.Extra = "disk_size=" + *input.DiskSize
		if c != nil {
			c.DiskSize = *input.DiskSize
		}
	}
	*f.calls = append(*f.calls, call)
	return c, nil
}

func (f fakeClusterSvc) Delete(_ context.Context, name string) error {
	*f.calls = append(*f.calls, serviceCall{Entity: "cluster", Op: "delete", Name: name})
	return nil
}

func (f fakeClusterSvc) Pause(_ context.Context, name string) (*cluster.Cluster, error) {
	*f.calls = append(*f.calls, serviceCall{Entity: "cluster", Op: "pause", Name: name})
	return f.repo.rows[name], nil
}

func (f fakeClusterSvc) Resume(_ context.Context, name string) (*cluster.Cluster, error) {
	*f.calls = append(*f.calls, serviceCall{Entity: "cluster", Op: "resume", Name: name})
	return f.repo.rows[name], nil
}

type fakeTopicSvc struct {
	repo  *fakeTopicRepo
	calls *[]serviceCall
}

var _ in.KafkaTopicService = (*fakeTopicSvc)(nil)

func (f fakeTopicSvc) Get(ctx context.Context, name string) (*topic.KafkaTopic, error) {
	return f.repo.Get(ctx, uuid.Nil, name)
}

func (f fakeTopicSvc) List(context.Context, in.ListTopicsInput) (in.TopicPage, error) {
	return in.TopicPage{}, nil
}

func (f fakeTopicSvc) SetConsumption(
	_ context.Context, name string, c topic.Consumption,
) (*topic.KafkaTopic, error) {
	*f.calls = append(*f.calls, serviceCall{
		Entity: "topic", Op: "set-consumption", Name: name, Extra: string(c),
	})
	t := f.repo.rows[name]
	if t != nil {
		t.Consumption = c
	}
	return t, nil
}
