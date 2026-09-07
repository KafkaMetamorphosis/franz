package placement_test

import (
	"reflect"
	"testing"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/cluster"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/placement"
)

// newCluster is a bare ACTIVE registration carrying only the labels a placement
// test cares about.
func newCluster(name string, labels map[string]string) *cluster.Cluster {
	return &cluster.Cluster{Name: name, Labels: labels, State: cluster.StateActive}
}

// names renders a selection outcome as the async-channel-shard-index → cluster
// name mapping the assertions compare.
func names(plan placement.Plan, shards int) []string {
	out := make([]string, shards)
	for index := range shards {
		if c := plan.ByShardIndex[index]; c != nil {
			out[index] = c.Name
		}
	}
	return out
}

func TestSelect(t *testing.T) {
	prodA := newCluster("a-prod", map[string]string{"env": "prod"})
	prodB := newCluster("b-prod", map[string]string{"env": "prod"})
	prodC := newCluster("c-prod", map[string]string{"env": "prod"})
	staging := newCluster("s-staging", map[string]string{"env": "staging"})

	cases := []struct {
		name    string
		labels  map[string]string
		fleet   []*cluster.Cluster
		shards  int
		want    []string
		wantErr bool
	}{
		{
			name:   "absent affinity selector places nothing",
			labels: map[string]string{"my-fleet/tier": "high"},
			fleet:  []*cluster.Cluster{prodA, prodB},
			shards: 3,
			want:   []string{"", "", ""},
		},
		{
			name:   "one candidate takes every channel shard",
			labels: map[string]string{placement.LabelAffinitySelector: "env=prod"},
			fleet:  []*cluster.Cluster{prodA, staging},
			shards: 3,
			want:   []string{"a-prod", "a-prod", "a-prod"},
		},
		{
			name: "shard-size caps the spread, round-robin distributes",
			labels: map[string]string{
				placement.LabelAffinitySelector: "env=prod",
				placement.LabelShardSize:        "2",
			},
			fleet:  []*cluster.Cluster{prodA, prodB, prodC},
			shards: 5,
			// Uneven split: the earlier cluster takes the remainder (003.7 OQ1).
			want: []string{"a-prod", "b-prod", "a-prod", "b-prod", "a-prod"},
		},
		{
			name: "shard-size above the candidate count is capped, not padded",
			labels: map[string]string{
				placement.LabelAffinitySelector: "env=prod",
				placement.LabelShardSize:        "10",
			},
			fleet:  []*cluster.Cluster{prodA, prodB},
			shards: 3,
			want:   []string{"a-prod", "b-prod", "a-prod"},
		},
		{
			name: "weight beats name, name breaks the tie",
			labels: map[string]string{
				placement.LabelAffinitySelector: "env=prod",
				placement.LabelShardSize:        "3",
			},
			fleet: []*cluster.Cluster{
				newCluster("a-prod", map[string]string{"env": "prod"}),
				newCluster("z-prod", map[string]string{
					"env": "prod", placement.LabelWeight: "10",
				}),
				newCluster("b-prod", map[string]string{"env": "prod"}),
			},
			shards: 3,
			want:   []string{"z-prod", "a-prod", "b-prod"},
		},
		{
			name: "anti-affinity drops a matching candidate",
			labels: map[string]string{
				placement.LabelAffinitySelector:     "env=prod",
				placement.LabelAntiAffinitySelector: "tier=shared",
				placement.LabelShardSize:            "5",
			},
			fleet: []*cluster.Cluster{
				prodA,
				newCluster("b-prod", map[string]string{"env": "prod", "tier": "shared"}),
			},
			shards: 2,
			want:   []string{"a-prod", "a-prod"},
		},
		{
			name: "a drain taint is never placed on, tolerated or not",
			labels: map[string]string{
				placement.LabelAffinitySelector: "env=prod",
				placement.LabelToleration:       "decommission:drain",
				placement.LabelShardSize:        "5",
			},
			fleet: []*cluster.Cluster{
				newCluster("a-prod", map[string]string{
					"env": "prod", placement.LabelTaint: "decommission:drain",
				}),
				prodB,
			},
			shards: 2,
			want:   []string{"b-prod", "b-prod"},
		},
		{
			name: "an untolerated no-creation taint is dropped",
			labels: map[string]string{
				placement.LabelAffinitySelector: "env=prod",
				placement.LabelShardSize:        "5",
			},
			fleet: []*cluster.Cluster{
				newCluster("a-prod", map[string]string{
					"env": "prod", placement.LabelTaint: "dedicated-billing:no-creation",
				}),
				prodB,
			},
			shards: 2,
			want:   []string{"b-prod", "b-prod"},
		},
		{
			name: "a tolerated no-creation taint stays a candidate",
			labels: map[string]string{
				placement.LabelAffinitySelector: "env=prod",
				placement.LabelToleration:       "maintenance:no-creation, dedicated-billing:no-creation",
				placement.LabelShardSize:        "5",
			},
			fleet: []*cluster.Cluster{
				newCluster("a-prod", map[string]string{
					"env": "prod", placement.LabelTaint: "dedicated-billing:no-creation",
				}),
				prodB,
			},
			shards: 2,
			want:   []string{"a-prod", "b-prod"},
		},
		{
			name:   "a non-ACTIVE cluster is never a candidate",
			labels: map[string]string{placement.LabelAffinitySelector: "env=prod"},
			fleet: []*cluster.Cluster{
				{Name: "a-prod", Labels: map[string]string{"env": "prod"}, State: cluster.StatePaused},
			},
			shards: 2,
			want:   []string{"", ""},
		},
		{
			name:    "a malformed shard-size is INVALID_ARGUMENT",
			labels:  map[string]string{placement.LabelShardSize: "many"},
			fleet:   []*cluster.Cluster{prodA},
			shards:  1,
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := placement.Select(tc.labels, tc.fleet, tc.shards)
			if tc.wantErr {
				if errs.KindOf(err) != errs.InvalidArgument {
					t.Fatalf("err = %v, want INVALID_ARGUMENT", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Select: %v", err)
			}
			if got := names(plan, tc.shards); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("assignment = %v, want %v", got, tc.want)
			}
		})
	}
}

// Two runs over the same inputs — and over a shuffled cluster slice — must
// produce identical assignments (003.7 invariant "placement is deterministic").
func TestSelectIsDeterministic(t *testing.T) {
	labels := map[string]string{
		placement.LabelAffinitySelector: "env=prod",
		placement.LabelShardSize:        "3",
	}
	fleet := []*cluster.Cluster{
		newCluster("alpha", map[string]string{"env": "prod"}),
		newCluster("bravo", map[string]string{"env": "prod", placement.LabelWeight: "5"}),
		newCluster("charlie", map[string]string{"env": "prod", placement.LabelWeight: "5"}),
		newCluster("delta", map[string]string{"env": "prod"}),
	}
	reversed := []*cluster.Cluster{fleet[3], fleet[2], fleet[1], fleet[0]}

	first, err := placement.Select(labels, fleet, 7)
	if err != nil {
		t.Fatal(err)
	}
	second, err := placement.Select(labels, reversed, 7)
	if err != nil {
		t.Fatal(err)
	}
	got, want := names(first, 7), names(second, 7)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("input order changed the assignment: %v vs %v", got, want)
	}
	// bravo and charlie tie on weight 5, so name breaks the tie; delta is capped
	// out by shard-size 3.
	expected := []string{"bravo", "charlie", "alpha", "bravo", "charlie", "alpha", "bravo"}
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("assignment = %v, want %v", got, expected)
	}
}

func TestCanHostIgnoresNoCreationButNotDrain(t *testing.T) {
	rules, err := placement.ParseChannelRules(map[string]string{
		placement.LabelAffinitySelector: "env=prod",
	})
	if err != nil {
		t.Fatal(err)
	}

	noCreation := newCluster("a-prod", map[string]string{
		"env": "prod", placement.LabelTaint: "maintenance:no-creation",
	})
	if ok, reason := rules.CanHost(noCreation); !ok {
		t.Errorf("a no-creation taint must not unseat an existing shard: %s", reason)
	}
	if ok, _ := rules.CanPlace(noCreation); ok {
		t.Error("a no-creation taint must block a new async-channel shard")
	}

	draining := newCluster("b-prod", map[string]string{
		"env": "prod", placement.LabelTaint: "decommission:drain",
	})
	if ok, _ := rules.CanHost(draining); ok {
		t.Error("a drain taint must unseat an existing shard")
	}
	if ok, _ := rules.CanHost(nil); ok {
		t.Error("a deleted cluster must unseat an existing shard")
	}
	paused := &cluster.Cluster{
		Name: "c-prod", Labels: map[string]string{"env": "prod"}, State: cluster.StatePaused,
	}
	if ok, _ := rules.CanHost(paused); ok {
		t.Error("a PAUSED cluster must unseat an existing shard")
	}
}

func TestParseChannelRulesRejectsMalformedLabels(t *testing.T) {
	cases := map[string]map[string]string{
		"bad selector":          {placement.LabelAffinitySelector: "env=="},
		"bad anti-selector":     {placement.LabelAntiAffinitySelector: "env IN"},
		"empty anti-selector":   {placement.LabelAntiAffinitySelector: ""},
		"shard-size zero":       {placement.LabelShardSize: "0"},
		"shard-size negative":   {placement.LabelShardSize: "-2"},
		"shard-size not an int": {placement.LabelShardSize: "2.5"},
		"toleration no effect":  {placement.LabelToleration: "maintenance"},
		"toleration bad effect": {placement.LabelToleration: "maintenance:evict"},
		"toleration empty item": {placement.LabelToleration: "a:drain,,b:drain"},
		"toleration bad name":   {placement.LabelToleration: "-bad-:drain"},
	}
	for name, labels := range cases {
		t.Run(name, func(t *testing.T) {
			if err := placement.ValidateChannelLabels(labels); errs.KindOf(err) != errs.InvalidArgument {
				t.Fatalf("err = %v, want INVALID_ARGUMENT", err)
			}
		})
	}
}

func TestParseChannelRulesDefaults(t *testing.T) {
	rules, err := placement.ParseChannelRules(nil)
	if err != nil {
		t.Fatal(err)
	}
	if rules.ShardSize() != placement.DefaultShardSize {
		t.Errorf("shard size = %d, want %d", rules.ShardSize(), placement.DefaultShardSize)
	}
	if rules.OptedIn() {
		t.Error("a channel with no labels must not be opted in to placement")
	}
	if rules.Tolerates(placement.Taint{Name: "x", Effect: placement.EffectNoCreation}) {
		t.Error("no tolerations declared, yet one matched")
	}
}

func TestParseClusterRules(t *testing.T) {
	rules, err := placement.ParseClusterRules(map[string]string{
		placement.LabelTaint:  "unstable:no-creation",
		placement.LabelWeight: "-3",
	})
	if err != nil {
		t.Fatal(err)
	}
	if rules.Weight != -3 {
		t.Errorf("weight = %d, want -3 (a negative weight is legal, it just sorts last)", rules.Weight)
	}
	if rules.Taint == nil || rules.Taint.String() != "unstable:no-creation" {
		t.Fatalf("taint = %v", rules.Taint)
	}

	for name, labels := range map[string]map[string]string{
		"bad effect": {placement.LabelTaint: "unstable:cordon"},
		"no effect":  {placement.LabelTaint: "unstable"},
		"bad weight": {placement.LabelWeight: "heavy"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := placement.ValidateClusterLabels(labels); errs.KindOf(err) != errs.InvalidArgument {
				t.Fatalf("err = %v, want INVALID_ARGUMENT", err)
			}
		})
	}
}

func TestRulesChangedIgnoresFreeFormLabels(t *testing.T) {
	before := map[string]string{
		placement.LabelAffinitySelector: "env=prod",
		"my-fleet/owner":                "billing",
	}
	after := map[string]string{
		placement.LabelAffinitySelector: "env=prod",
		"my-fleet/owner":                "payments",
	}
	if placement.RulesChanged(before, after) {
		t.Error("a free-form label edit must not trigger a placement pass")
	}
	after[placement.LabelShardSize] = "2"
	if !placement.RulesChanged(before, after) {
		t.Error("a reserved label edit must trigger a placement pass")
	}
}
