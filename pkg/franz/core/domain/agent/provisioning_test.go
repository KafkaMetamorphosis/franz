package agent

import (
	"testing"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
)

func TestValidateProvisioningLabels(t *testing.T) {
	cases := []struct {
		name  string
		specs []ProvisioningLabelSpec
		ok    bool
	}{
		{"empty is fine", nil, true},
		{
			"well-formed",
			[]ProvisioningLabelSpec{
				{Key: "franz.provisioning/deployment-type", AllowedValues: []string{"local-docker"}, DefaultValue: "local-docker", Required: true},
				{Key: "franz.provisioning/kafka-version", DefaultValue: "3.7.0"},
			},
			true,
		},
		{"empty key", []ProvisioningLabelSpec{{Key: ""}}, false},
		{"key not franz-prefixed", []ProvisioningLabelSpec{{Key: "provisioning/x"}}, false},
		{
			"duplicate key",
			[]ProvisioningLabelSpec{{Key: "franz.provisioning/x"}, {Key: "franz.provisioning/x"}},
			false,
		},
		{
			"default not in allowed values",
			[]ProvisioningLabelSpec{{Key: "franz.provisioning/x", AllowedValues: []string{"a", "b"}, DefaultValue: "c"}},
			false,
		},
		{
			"default free when allowed values empty",
			[]ProvisioningLabelSpec{{Key: "franz.provisioning/x", DefaultValue: "anything"}},
			true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateProvisioningLabels(tc.specs)
			if tc.ok && err != nil {
				t.Fatalf("want ok, got %v", err)
			}
			if !tc.ok {
				if errs.KindOf(err) != errs.InvalidArgument {
					t.Fatalf("want InvalidArgument, got %v", err)
				}
			}
		})
	}
}

func TestSetProvisioningLabels(t *testing.T) {
	a, _ := New(testRealm(), "a", TypeClusterProvider, nil, nil, "h")
	if err := a.SetProvisioningLabels([]ProvisioningLabelSpec{{Key: "franz.provisioning/kafka-image"}}); err != nil {
		t.Fatalf("SetProvisioningLabels: %v", err)
	}
	if len(a.ProvisioningLabels) != 1 {
		t.Fatalf("not stored: %+v", a.ProvisioningLabels)
	}
	if err := a.SetProvisioningLabels([]ProvisioningLabelSpec{{Key: "bad"}}); err == nil {
		t.Fatal("malformed schema should be rejected")
	}
}
