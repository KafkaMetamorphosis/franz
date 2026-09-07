package out

import (
	"context"

	"github.com/google/uuid"
)

// ShardPlacer materialises and re-evaluates an Async Channel's async-channel
// shards (003.7 placement). It is implemented by the placement use case; the
// entity services depend on this interface only, so they never learn the
// selection algorithm or how a shard row is written.
//
// Both methods are best-effort and synchronous: they run after the caller's
// transaction has committed, and a failure is logged rather than propagated —
// channel create always succeeds (003.7) and the retry sweep re-runs the pass
// within one interval.
type ShardPlacer interface {
	// PlaceChannel runs one placement pass for a single channel. Called when the
	// channel is created and when its `franz.*` labels change.
	PlaceChannel(ctx context.Context, realmID uuid.UUID, channelName string)

	// PlaceRealm runs a pass for every ACTIVE channel in the realm. Called when a
	// cluster's labels or state change, since any channel may have gained or lost
	// a candidate.
	PlaceRealm(ctx context.Context, realmID uuid.UUID)
}
