package out

import (
	"context"

	"github.com/google/uuid"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/realm"
)

// RealmRepository loads realms. Provisioning is out of scope (003.1), so there is
// no Create/Update/Delete.
type RealmRepository interface {
	// GetBySlug returns the realm with the given slug, or errs.NotFound.
	GetBySlug(ctx context.Context, slug string) (realm.Realm, error)
	// GetByID returns the realm with the given id, or errs.NotFound.
	GetByID(ctx context.Context, id uuid.UUID) (realm.Realm, error)
}
