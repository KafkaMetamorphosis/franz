package fieldmask

import (
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	franzv1 "github.com/KafkaMetamorphosis/franz/pkg/gen/go/franz/v1"
)

func mask(paths ...string) *fieldmaskpb.FieldMask { return &fieldmaskpb.FieldMask{Paths: paths} }

func TestValidateRejectsEmpty(t *testing.T) {
	msg := &franzv1.KafkaCluster{}
	for _, m := range []*fieldmaskpb.FieldMask{nil, mask(), {}} {
		err := Validate(m, msg)
		if e, ok := errs.As(err); !ok || e.Violations[0].Field != "update_mask" {
			t.Errorf("Validate(%v) = %v, want update_mask violation", m, err)
		}
	}
}

func TestValidateRejectsImmutableAndUnknown(t *testing.T) {
	msg := &franzv1.KafkaCluster{}
	for _, path := range []string{"name", "frn", "created_at", "updated_at", "bogus"} {
		if err := Validate(mask(path), msg); err == nil {
			t.Errorf("Validate(%q) = nil, want error", path)
		}
	}
}

func TestValidateAcceptsRealFields(t *testing.T) {
	msg := &franzv1.KafkaCluster{}
	if err := Validate(mask("labels", "cluster_configuration", "state"), msg); err != nil {
		t.Errorf("Validate = %v, want nil", err)
	}
	// JSON name form
	if err := Validate(mask("clusterConfiguration"), msg); err != nil {
		t.Errorf("Validate(jsonName) = %v, want nil", err)
	}
}

func TestApplyCopiesOnlyMaskedFields(t *testing.T) {
	dst := franzv1.KafkaCluster_builder{
		Name:                 proto.String("east-1"),
		Labels:               map[string]string{"env": "prod", "team": "billing"},
		ClusterConfiguration: map[string]string{"default.replication.factor": "3"},
	}.Build()
	src := franzv1.KafkaCluster_builder{
		Name:   proto.String("SHOULD-NOT-APPLY"),
		Labels: map[string]string{"env": "staging"},
	}.Build()

	if err := Apply(mask("labels"), src, dst); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if dst.GetName() != "east-1" {
		t.Errorf("name was changed to %q", dst.GetName())
	}
	// map replaced wholesale, not merged
	if len(dst.GetLabels()) != 1 || dst.GetLabels()["env"] != "staging" {
		t.Errorf("labels = %v, want {env:staging}", dst.GetLabels())
	}
	// untouched field survives
	if dst.GetClusterConfiguration()["default.replication.factor"] != "3" {
		t.Errorf("cluster_configuration was disturbed: %v", dst.GetClusterConfiguration())
	}
}

func TestApplyClearsMaskedFieldUnsetOnSrc(t *testing.T) {
	dst := franzv1.KafkaCluster_builder{
		Name:   proto.String("east-1"),
		Labels: map[string]string{"env": "prod"},
	}.Build()
	src := franzv1.KafkaCluster_builder{Name: proto.String("east-1")}.Build()

	if err := Apply(mask("labels"), src, dst); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(dst.GetLabels()) != 0 {
		t.Errorf("labels should have been cleared, got %v", dst.GetLabels())
	}
}

func TestApplyRejectsBadMask(t *testing.T) {
	dst := &franzv1.KafkaCluster{}
	src := &franzv1.KafkaCluster{}
	if err := Apply(mask("name"), src, dst); err == nil {
		t.Error("Apply with immutable field should fail")
	}
	if err := Apply(nil, src, dst); err == nil {
		t.Error("Apply with nil mask should fail")
	}
}
