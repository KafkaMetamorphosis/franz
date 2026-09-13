package client_test

import (
	"testing"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/client"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
)

func testRealm() realm.Realm { return realm.Realm{Slug: "default"} }

func TestNewClient(t *testing.T) {
	c, err := client.New(testRealm(), "billing", map[string]string{"org.com/owner": "payments-team"})
	if err != nil {
		t.Fatal(err)
	}
	if c.Name != "billing" || c.FRN.String() != "frn:default:client:billing" {
		t.Fatalf("Name/FRN = %q/%q", c.Name, c.FRN.String())
	}
	if c.Labels["org.com/owner"] != "payments-team" {
		t.Fatalf("Labels = %+v", c.Labels)
	}
}

func TestNewClientNilLabelsBecomesEmptyMap(t *testing.T) {
	c, err := client.New(testRealm(), "billing", nil)
	if err != nil {
		t.Fatal(err)
	}
	if c.Labels == nil || len(c.Labels) != 0 {
		t.Fatalf("Labels = %#v, want a non-nil empty map", c.Labels)
	}
}

func TestNewClientRejectsInvalidName(t *testing.T) {
	if _, err := client.New(testRealm(), "Not Valid!", nil); errs.KindOf(err) != errs.InvalidArgument {
		t.Fatalf("kind = %v", errs.KindOf(err))
	}
}

func TestSetLabelsReplacesWholesale(t *testing.T) {
	c, err := client.New(testRealm(), "billing", map[string]string{"team": "infra"})
	if err != nil {
		t.Fatal(err)
	}
	c.SetLabels(map[string]string{"org.com/owner": "payments-team"})
	if c.Labels["team"] != "" || c.Labels["org.com/owner"] != "payments-team" {
		t.Fatalf("Labels = %+v, want a wholesale replacement", c.Labels)
	}
}

func TestSetLabelsNilBecomesEmptyMap(t *testing.T) {
	c, err := client.New(testRealm(), "billing", map[string]string{"team": "infra"})
	if err != nil {
		t.Fatal(err)
	}
	c.SetLabels(nil)
	if c.Labels == nil || len(c.Labels) != 0 {
		t.Fatalf("Labels = %#v, want a non-nil empty map", c.Labels)
	}
}
