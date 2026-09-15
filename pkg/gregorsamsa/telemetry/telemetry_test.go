package telemetry

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/KafkaMetamorphosis/franz/pkg/gregorsamsa/kafkaadmin"
)

// fakeMigrationAdmin implements only what observeMigrationSignals calls;
// everything else panics if reached, so a test that hits an unexpected method
// fails loudly instead of silently returning zero values.
type fakeMigrationAdmin struct {
	kafkaadmin.Admin
	offsets      []kafkaadmin.PartitionOffsets
	offsetsErr   error
	groupOffsets map[string][]int32 // group -> partitions it has committed to
	groupErr     error
}

func (f *fakeMigrationAdmin) ListOffsets(context.Context, string) ([]kafkaadmin.PartitionOffsets, error) {
	return f.offsets, f.offsetsErr
}

func (f *fakeMigrationAdmin) ListConsumerGroupOffsets(_ context.Context, group, _ string) ([]int32, error) {
	if f.groupErr != nil {
		return nil, f.groupErr
	}
	return f.groupOffsets[group], nil
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestObserveMigrationSignalsDrainedWhenNoDataAnywhere(t *testing.T) {
	admin := &fakeMigrationAdmin{
		offsets: []kafkaadmin.PartitionOffsets{{Partition: 0, Earliest: 10, Latest: 10}},
	}
	s := &Sweeper{log: discardLogger()}
	drained, connected := s.observeMigrationSignals(context.Background(), admin, "orders-0", nil)
	if !drained {
		t.Error("want drained (no partition has data)")
	}
	if connected {
		t.Error("want not connected (no groups)")
	}
}

func TestObserveMigrationSignalsNotDrainedWhenAnyPartitionHasData(t *testing.T) {
	admin := &fakeMigrationAdmin{
		offsets: []kafkaadmin.PartitionOffsets{
			{Partition: 0, Earliest: 10, Latest: 10},
			{Partition: 1, Earliest: 5, Latest: 42}, // still has data
		},
	}
	s := &Sweeper{log: discardLogger()}
	drained, _ := s.observeMigrationSignals(context.Background(), admin, "orders-0", nil)
	if drained {
		t.Error("want not drained (partition 1 still has data)")
	}
}

func TestObserveMigrationSignalsConnectedWhenAGroupHasCommitted(t *testing.T) {
	admin := &fakeMigrationAdmin{
		offsets:      []kafkaadmin.PartitionOffsets{{Partition: 0, Earliest: 0, Latest: 0}},
		groupOffsets: map[string][]int32{"billing.orders-0": {0, 1}},
	}
	s := &Sweeper{log: discardLogger()}
	_, connected := s.observeMigrationSignals(context.Background(), admin, "orders-0",
		[]string{"billing.orders-0", "unrelated-group"})
	if !connected {
		t.Error("want connected (billing.orders-0 has committed offsets)")
	}
}

func TestObserveMigrationSignalsNotConnectedWhenNoGroupCommitted(t *testing.T) {
	admin := &fakeMigrationAdmin{
		offsets:      []kafkaadmin.PartitionOffsets{{Partition: 0, Earliest: 0, Latest: 0}},
		groupOffsets: map[string][]int32{}, // every group returns empty for this topic
	}
	s := &Sweeper{log: discardLogger()}
	_, connected := s.observeMigrationSignals(context.Background(), admin, "orders-0",
		[]string{"some-other-groups-topic-consumer"})
	if connected {
		t.Error("want not connected (no group has committed to this topic)")
	}
}

func TestObserveMigrationSignalsUnknownOnListOffsetsError(t *testing.T) {
	admin := &fakeMigrationAdmin{offsetsErr: context.DeadlineExceeded}
	s := &Sweeper{log: discardLogger()}
	drained, connected := s.observeMigrationSignals(context.Background(), admin, "orders-0", nil)
	if drained || connected {
		t.Errorf("drained=%v connected=%v, want both false on error (conservative)", drained, connected)
	}
}
