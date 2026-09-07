package scope_test

import (
	"testing"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/cluster"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/scope"
)

func TestSelectorMatches(t *testing.T) {
	cases := []struct {
		name          string
		agentLabels   map[string]string
		clusterLabels map[string]string
		want          bool
	}{
		{
			name:          "empty selector matches nothing",
			agentLabels:   map[string]string{},
			clusterLabels: map[string]string{"franz.placement/env": "prod"},
			want:          false,
		},
		{
			name:          "only non-reserved agent labels is still an empty selector",
			agentLabels:   map[string]string{"franz.role": "gregor-samsa", "team": "platform"},
			clusterLabels: map[string]string{"franz.placement/env": "prod"},
			want:          false,
		},
		{
			name:          "single pair matches",
			agentLabels:   map[string]string{"franz.placement-selector/env": "prod"},
			clusterLabels: map[string]string{"franz.placement/env": "prod"},
			want:          true,
		},
		{
			name:          "every pair must match",
			agentLabels:   map[string]string{"franz.placement-selector/env": "prod", "franz.placement-selector/org": "payments"},
			clusterLabels: map[string]string{"franz.placement/env": "prod", "franz.placement/org": "payments"},
			want:          true,
		},
		{
			name:          "one pair missing on the cluster fails the whole match",
			agentLabels:   map[string]string{"franz.placement-selector/env": "prod", "franz.placement-selector/org": "payments"},
			clusterLabels: map[string]string{"franz.placement/env": "prod"},
			want:          false,
		},
		{
			name:          "value mismatch fails",
			agentLabels:   map[string]string{"franz.placement-selector/env": "prod"},
			clusterLabels: map[string]string{"franz.placement/env": "staging"},
			want:          false,
		},
		{
			name:          "extra franz.placement keys on the cluster are ignored",
			agentLabels:   map[string]string{"franz.placement-selector/env": "prod"},
			clusterLabels: map[string]string{"franz.placement/env": "prod", "franz.placement/region": "us-east-1"},
			want:          true,
		},
		{
			name:          "a bare cluster label is not a franz.placement label",
			agentLabels:   map[string]string{"franz.placement-selector/env": "prod"},
			clusterLabels: map[string]string{"env": "prod"},
			want:          false,
		},
		{
			name:          "the selector grammar is not interpreted — a value is an exact string",
			agentLabels:   map[string]string{"franz.placement-selector/env": "prod,staging"},
			clusterLabels: map[string]string{"franz.placement/env": "prod"},
			want:          false,
		},
		{
			name:          "a cluster with no labels at all is out of scope",
			agentLabels:   map[string]string{"franz.placement-selector/env": "prod"},
			clusterLabels: nil,
			want:          false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := scope.SelectorFromLabels(tc.agentLabels).Matches(tc.clusterLabels)
			if got != tc.want {
				t.Fatalf("Matches() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestResolve(t *testing.T) {
	prodPayments := &cluster.Cluster{
		Name:   "east-1",
		State:  cluster.StateActive,
		Labels: map[string]string{"franz.placement/env": "prod", "franz.placement/org": "payments"},
	}
	prodAudit := &cluster.Cluster{
		Name:   "east-2",
		State:  cluster.StateActive,
		Labels: map[string]string{"franz.placement/env": "prod", "franz.placement/org": "audit"},
	}
	staging := &cluster.Cluster{
		Name:   "west-1",
		State:  cluster.StateActive,
		Labels: map[string]string{"franz.placement/env": "staging", "franz.placement/org": "payments"},
	}
	deletedProd := &cluster.Cluster{
		Name:   "east-9",
		State:  cluster.StateDeleted,
		Labels: map[string]string{"franz.placement/env": "prod", "franz.placement/org": "payments"},
	}
	all := []*cluster.Cluster{prodPayments, prodAudit, staging, deletedProd}

	cases := []struct {
		name        string
		agentLabels map[string]string
		want        []string
	}{
		{
			name:        "empty selector resolves to nothing",
			agentLabels: nil,
			want:        nil,
		},
		{
			name:        "one key selects every cluster carrying it",
			agentLabels: map[string]string{"franz.placement-selector/env": "prod"},
			want:        []string{"east-1", "east-2"},
		},
		{
			name: "two keys narrow to the conjunction",
			agentLabels: map[string]string{
				"franz.placement-selector/env": "prod",
				"franz.placement-selector/org": "payments",
			},
			want: []string{"east-1"},
		},
		{
			name:        "no cluster matches",
			agentLabels: map[string]string{"franz.placement-selector/env": "qa"},
			want:        nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := scope.Resolve(tc.agentLabels, all)
			names := make([]string, 0, len(got))
			for _, c := range got {
				names = append(names, c.Name)
			}
			if len(names) != len(tc.want) {
				t.Fatalf("Resolve() = %v, want %v", names, tc.want)
			}
			for i := range names {
				if names[i] != tc.want[i] {
					t.Fatalf("Resolve() = %v, want %v", names, tc.want)
				}
			}
		})
	}
}

func TestResolveExcludesDeletedClusters(t *testing.T) {
	deleted := &cluster.Cluster{
		Name:   "gone",
		State:  cluster.StateDeleted,
		Labels: map[string]string{"franz.placement/env": "prod"},
	}
	got := scope.Resolve(map[string]string{"franz.placement-selector/env": "prod"},
		[]*cluster.Cluster{deleted})
	if len(got) != 0 {
		t.Fatalf("a soft-deleted cluster must never be in scope, got %d", len(got))
	}
}

func TestValidateAgentLabels(t *testing.T) {
	cases := []struct {
		name    string
		labels  map[string]string
		wantErr bool
	}{
		{name: "no reserved labels", labels: map[string]string{"team": "platform"}},
		{name: "valid pair", labels: map[string]string{"franz.placement-selector/env": "prod"}},
		{name: "dotted name", labels: map[string]string{"franz.placement-selector/kafka.tier": "gold"}},
		{
			name:    "empty value would match on key presence alone",
			labels:  map[string]string{"franz.placement-selector/env": ""},
			wantErr: true,
		},
		{
			name:    "empty name",
			labels:  map[string]string{"franz.placement-selector/": "prod"},
			wantErr: true,
		},
		{
			name:    "leading punctuation",
			labels:  map[string]string{"franz.placement-selector/-env": "prod"},
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := scope.ValidateAgentLabels(tc.labels)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ValidateAgentLabels() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestValidateClusterLabels(t *testing.T) {
	if err := scope.ValidateClusterLabels(map[string]string{"franz.placement/env": "prod"}); err != nil {
		t.Fatalf("valid placement label rejected: %v", err)
	}
	if err := scope.ValidateClusterLabels(map[string]string{"franz.placement/env": ""}); err == nil {
		t.Fatal("empty placement value should be rejected")
	}
}

// The two reserved prefixes must not shadow one another: `franz.placement/x` is
// not a `franz.placement-selector/*` label and vice versa.
func TestPrefixesDoNotOverlap(t *testing.T) {
	labels := map[string]string{
		"franz.placement/env":          "prod",
		"franz.placement-selector/env": "staging",
	}
	if sel := scope.SelectorFromLabels(labels); len(sel) != 1 || sel["env"] != "staging" {
		t.Fatalf("SelectorFromLabels() = %v", sel)
	}
	if pl := scope.PlacementFromLabels(labels); len(pl) != 1 || pl["env"] != "prod" {
		t.Fatalf("PlacementFromLabels() = %v", pl)
	}
}
