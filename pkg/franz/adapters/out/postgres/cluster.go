package postgres

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/cluster"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/errs"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/domain/frn"
	"github.com/KafkaMetamorphosis/franz/pkg/franz/core/ports/out"
)

// ClusterRepo implements out.ClusterRepository with hand-written pgx (003.12).
type ClusterRepo struct {
	db *DB
}

// NewClusterRepo wires the repository to the pool.
func NewClusterRepo(db *DB) *ClusterRepo { return &ClusterRepo{db: db} }

var _ out.ClusterRepository = (*ClusterRepo)(nil)

const clusterColumns = `id, realm_id, name, frn, connection_strings, labels,
	cluster_configuration, cluster_provider_agent, state, created_at, updated_at`

type rowScanner interface {
	Scan(dest ...any) error
}

type connStringJSON struct {
	BootstrapURLs []string `json:"bootstrap_urls"`
	Type          string   `json:"type"`
}

func toConnJSON(conns []cluster.ConnectionString) []connStringJSON {
	out := make([]connStringJSON, len(conns))
	for i, c := range conns {
		out[i] = connStringJSON{BootstrapURLs: c.BootstrapURLs, Type: string(c.Type)}
	}
	return out
}

func fromConnJSON(in []connStringJSON) []cluster.ConnectionString {
	out := make([]cluster.ConnectionString, len(in))
	for i, c := range in {
		out[i] = cluster.ConnectionString{
			BootstrapURLs: c.BootstrapURLs,
			Type:          cluster.ConnectionType(c.Type),
		}
	}
	return out
}

func scanCluster(sc rowScanner) (*cluster.Cluster, error) {
	var (
		c                           cluster.Cluster
		frnPath, state              string
		connsRaw, labelsRaw, cfgRaw []byte
	)
	err := sc.Scan(&c.ID, &c.RealmID, &c.Name, &frnPath, &connsRaw, &labelsRaw,
		&cfgRaw, &c.ProviderAgent, &state, &c.CreatedAt, &c.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errs.NotFoundf("kafka cluster not found")
		}
		return nil, errs.Internalf("scan kafka cluster").Wrap(err)
	}

	f, err := frn.ParsePath(frnPath)
	if err != nil {
		return nil, errs.Internalf("stored frn %q is malformed", frnPath).Wrap(err)
	}
	c.FRN = f
	c.State = cluster.State(state)

	var cj []connStringJSON
	if err := json.Unmarshal(connsRaw, &cj); err != nil {
		return nil, errs.Internalf("decode connection_strings").Wrap(err)
	}
	c.ConnectionStrings = fromConnJSON(cj)

	c.Labels = map[string]string{}
	c.Configuration = map[string]string{}
	if err := json.Unmarshal(labelsRaw, &c.Labels); err != nil {
		return nil, errs.Internalf("decode labels").Wrap(err)
	}
	if err := json.Unmarshal(cfgRaw, &c.Configuration); err != nil {
		return nil, errs.Internalf("decode cluster_configuration").Wrap(err)
	}
	return &c, nil
}

// Create inserts a new cluster row.
func (r *ClusterRepo) Create(ctx context.Context, c *cluster.Cluster) error {
	if c.ID == uuid.Nil {
		id, err := uuid.NewV7()
		if err != nil {
			return errs.Internalf("generate uuid").Wrap(err)
		}
		c.ID = id
	}
	conns, _ := json.Marshal(toConnJSON(c.ConnectionStrings))
	labels, _ := json.Marshal(nonNilMap(c.Labels))
	cfg, _ := json.Marshal(nonNilMap(c.Configuration))

	row := r.db.Pool().QueryRow(ctx, `
		INSERT INTO kafka_cluster
			(id, realm_id, name, frn, connection_strings, labels,
			 cluster_configuration, cluster_provider_agent, state)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		RETURNING `+clusterColumns,
		c.ID, c.RealmID, c.Name, c.FRN.Path(), conns, labels, cfg,
		c.ProviderAgent, string(c.State))

	stored, err := scanCluster(row)
	if err != nil {
		if isUniqueViolation(err) {
			return errs.Existsf("kafka cluster %q already exists", c.Name)
		}
		return err
	}
	*c = *stored
	return nil
}

// Get returns the cluster by (realm, name), soft-deleted rows included.
func (r *ClusterRepo) Get(ctx context.Context, realmID uuid.UUID, name string) (*cluster.Cluster, error) {
	row := r.db.Pool().QueryRow(ctx,
		`SELECT `+clusterColumns+` FROM kafka_cluster WHERE realm_id=$1 AND name=$2`,
		realmID, name)
	return scanCluster(row)
}

// List returns one page ordered by name, with the 003.1 selector applied in Go
// (003.12 OQ2: Go-side for now).
func (r *ClusterRepo) List(ctx context.Context, q out.ClusterQuery) (out.ClusterPage, error) {
	limit := q.Limit
	if limit <= 0 {
		limit = 50
	}

	sql := `SELECT ` + clusterColumns + ` FROM kafka_cluster WHERE realm_id=$1 AND name > $2`
	if !q.IncludeDeleted {
		sql += ` AND state <> 'DELETED'`
	}
	sql += ` ORDER BY name ASC LIMIT 5000`

	rows, err := r.db.Pool().Query(ctx, sql, q.RealmID, q.AfterName)
	if err != nil {
		return out.ClusterPage{}, errs.Internalf("list kafka clusters").Wrap(err)
	}
	defer rows.Close()

	var page out.ClusterPage
	for rows.Next() {
		c, err := scanCluster(rows)
		if err != nil {
			return out.ClusterPage{}, err
		}
		if !q.Selector.Match(c.Labels) {
			continue
		}
		page.Clusters = append(page.Clusters, c)
		if len(page.Clusters) > limit {
			page.Clusters = page.Clusters[:limit]
			page.LastName = page.Clusters[limit-1].Name
			break
		}
	}
	if err := rows.Err(); err != nil {
		return out.ClusterPage{}, errs.Internalf("iterate kafka clusters").Wrap(err)
	}
	return page, nil
}

// ListByProviderAgent returns every cluster in the realm owned by agentName,
// DELETED rows included, ordered by name.
func (r *ClusterRepo) ListByProviderAgent(
	ctx context.Context, realmID uuid.UUID, agentName string,
) ([]*cluster.Cluster, error) {
	rows, err := r.db.Pool().Query(ctx,
		`SELECT `+clusterColumns+` FROM kafka_cluster
		 WHERE realm_id=$1 AND cluster_provider_agent=$2 ORDER BY name ASC`,
		realmID, agentName)
	if err != nil {
		return nil, errs.Internalf("list clusters by provider agent").Wrap(err)
	}
	defer rows.Close()

	var out []*cluster.Cluster
	for rows.Next() {
		c, err := scanCluster(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, errs.Internalf("iterate clusters by provider agent").Wrap(err)
	}
	return out, nil
}

// Mutate loads the row FOR UPDATE, runs mutate, and persists the result in one
// transaction.
func (r *ClusterRepo) Mutate(
	ctx context.Context, realmID uuid.UUID, name string,
	mutate func(*cluster.Cluster) error,
) (*cluster.Cluster, error) {
	var result *cluster.Cluster
	err := r.db.WithTx(ctx, func(tx pgx.Tx) error {
		row := tx.QueryRow(ctx,
			`SELECT `+clusterColumns+` FROM kafka_cluster
			 WHERE realm_id=$1 AND name=$2 FOR UPDATE`, realmID, name)
		c, err := scanCluster(row)
		if err != nil {
			return err
		}
		if err := mutate(c); err != nil {
			return err
		}
		conns, _ := json.Marshal(toConnJSON(c.ConnectionStrings))
		labels, _ := json.Marshal(nonNilMap(c.Labels))
		cfg, _ := json.Marshal(nonNilMap(c.Configuration))

		updated, err := scanCluster(tx.QueryRow(ctx, `
			UPDATE kafka_cluster SET
				connection_strings=$1, labels=$2, cluster_configuration=$3,
				cluster_provider_agent=$4, state=$5, updated_at=now()
			WHERE id=$6
			RETURNING `+clusterColumns,
			conns, labels, cfg, c.ProviderAgent, string(c.State), c.ID))
		if err != nil {
			return err
		}
		result = updated
		return nil
	})
	return result, err
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func nonNilMap(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}
