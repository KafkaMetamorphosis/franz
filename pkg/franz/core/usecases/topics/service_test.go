package topics

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/topic"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
)

// fakeTopicRepo is an in-memory TopicRepository over one channel's shards.
type fakeTopicRepo struct {
	channelID uuid.UUID
	shards    []*topic.KafkaTopic
	getErr    error
}

func (f *fakeTopicRepo) Create(context.Context, *topic.KafkaTopic) error { return nil }
func (f *fakeTopicRepo) Get(_ context.Context, _ uuid.UUID, name string) (*topic.KafkaTopic, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	for _, s := range f.shards {
		if s.Name == name {
			return s, nil
		}
	}
	return nil, errs.NotFoundf("kafka topic not found")
}
func (f *fakeTopicRepo) List(context.Context, out.TopicQuery) (out.TopicPage, error) {
	return out.TopicPage{}, nil
}
func (f *fakeTopicRepo) MutateChannelShards(
	_ context.Context, _, _ uuid.UUID, mutate func([]*topic.KafkaTopic) error,
) ([]*topic.KafkaTopic, error) {
	if err := mutate(f.shards); err != nil {
		return nil, err
	}
	return f.shards, nil
}
func (f *fakeTopicRepo) ResolveChannelID(context.Context, uuid.UUID, string) (uuid.UUID, error) {
	return f.channelID, nil
}
func (f *fakeTopicRepo) CountLiveTopics(context.Context, uuid.UUID) (int, error) { return 0, nil }

func (f *fakeTopicRepo) PlaceChannelShards(context.Context, uuid.UUID, uuid.UUID,
	func([]*topic.KafkaTopic) (out.ShardPlan, error),
) ([]*topic.KafkaTopic, error) {
	return nil, nil
}

func (f *fakeTopicRepo) ListByClusters(context.Context, uuid.UUID, []uuid.UUID) ([]*topic.KafkaTopic, error) {
	panic("unused")
}

func (f *fakeTopicRepo) MutateByFRN(context.Context, uuid.UUID, string, func(*topic.KafkaTopic) error) (*topic.KafkaTopic, error) {
	panic("unused")
}

func shard(name string, c topic.Consumption) *topic.KafkaTopic {
	return &topic.KafkaTopic{Name: name, State: topic.StatePending, Consumption: c,
		TrafficShare: topic.TrafficShare{Unit: topic.TrafficShareUnit}}
}

func ctxWithRealm() context.Context {
	return realm.NewContext(context.Background(), realm.Realm{ID: uuid.New(), Slug: "default"})
}

func TestSetConsumptionRebalance(t *testing.T) {
	repo := &fakeTopicRepo{shards: []*topic.KafkaTopic{
		shard("c-0", topic.ConsumptionEnabled),
		shard("c-1", topic.ConsumptionEnabled),
		shard("c-2", topic.ConsumptionEnabled),
	}}
	svc := NewService(repo, nil, nil)

	if _, err := svc.SetConsumption(ctxWithRealm(), "c-1", topic.ConsumptionDisabled); err != nil {
		t.Fatal(err)
	}
	got := map[string]float64{}
	for _, s := range repo.shards {
		got[s.Name] = s.TrafficShare.Value
	}
	if got["c-1"] != 0 {
		t.Errorf("drained shard share = %v", got["c-1"])
	}
	if got["c-0"] != 50 || got["c-2"] != 50 {
		t.Errorf("remaining shares = %v", got)
	}
}

func TestSetConsumptionAllDisabled(t *testing.T) {
	repo := &fakeTopicRepo{shards: []*topic.KafkaTopic{
		shard("c-0", topic.ConsumptionEnabled),
		shard("c-1", topic.ConsumptionEnabled),
	}}
	svc := NewService(repo, nil, nil)
	_, _ = svc.SetConsumption(ctxWithRealm(), "c-0", topic.ConsumptionDisabled)
	_, _ = svc.SetConsumption(ctxWithRealm(), "c-1", topic.ConsumptionDisabled)
	for _, s := range repo.shards {
		if s.TrafficShare.Value != 0 {
			t.Errorf("%s share = %v, want 0", s.Name, s.TrafficShare.Value)
		}
	}
}

func TestSetConsumptionRejectsUnknownValue(t *testing.T) {
	repo := &fakeTopicRepo{shards: []*topic.KafkaTopic{shard("c-0", topic.ConsumptionEnabled)}}
	svc := NewService(repo, nil, nil)
	if _, err := svc.SetConsumption(ctxWithRealm(), "c-0", "WAT"); errs.KindOf(err) != errs.InvalidArgument {
		t.Fatalf("→ %v", err)
	}
}

func TestSetConsumptionOnDeleted(t *testing.T) {
	sh := shard("c-0", topic.ConsumptionEnabled)
	sh.State = topic.StateDeleted
	repo := &fakeTopicRepo{shards: []*topic.KafkaTopic{sh}}
	svc := NewService(repo, nil, nil)
	if _, err := svc.SetConsumption(ctxWithRealm(), "c-0", topic.ConsumptionDisabled); errs.KindOf(err) != errs.FailedPrecondition {
		t.Fatalf("→ %v", err)
	}
}
