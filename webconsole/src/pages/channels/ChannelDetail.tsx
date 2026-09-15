import { useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { Breadcrumbs, Empty, ErrorBanner, Loading, PageHeading, Panel, StatusPill } from "../../components/ui";
import { MigrationPhaseBadge } from "../../components/MigrationPhaseBadge";
import {
  useChannel,
  useChannelLifecycle,
  useChannelTopics,
  useClusters,
  useMigrateKafkaTopic,
  useShardMigrations,
} from "../../api/hooks";
import { channelTypeLabel } from "../../api/enums";

// Ported from 001-ux/demo/async-channel-detail.html. The Access policy and
// Clients-with-access panels are intentionally absent — the policy engine and
// client views ship with deliverable 17.
export function ChannelDetail() {
  const { name = "" } = useParams();
  const navigate = useNavigate();
  const { data, isLoading, error } = useChannel(name);
  const { pause, resume, remove } = useChannelLifecycle(name);

  const channel = data?.asyncChannel;
  const deleted = channel?.state === "CHANNEL_STATE_DELETED";
  const declaredPartitions = channel?.channelPartitions ?? 0;

  // Poll while shards are still converging so the table reflects the agent.
  const { data: topicsData } = useChannelTopics(name, { pollMs: deleted ? undefined : 5000 });
  const shards = topicsData?.kafkaTopics ?? [];

  // Migration (18/22): every migration involving one of this channel's
  // shards, polled at the same 5s cadence as the shard table above (simpler
  // than gating the interval on whether anything is still in flight, and
  // consistent with how ClusterDetail's provider-status poll never stops
  // either).
  const { data: migrationsData } = useShardMigrations(name, { pollMs: deleted ? undefined : 5000 });
  const migrations = migrationsData?.migrations ?? [];
  const { data: clustersData } = useClusters();
  const clusters = clustersData?.kafkaClusters ?? [];
  const migrateTopic = useMigrateKafkaTopic();
  const [migratingShard, setMigratingShard] = useState<string | null>(null);
  const [targetCluster, setTargetCluster] = useState("");

  return (
    <>
      <Breadcrumbs
        items={[
          { label: "Franz Console", to: "/" },
          { label: "Async Channels", to: "/async-channels" },
          { label: name },
        ]}
      />
      <PageHeading
        title={name}
        lead={channel?.frn}
        actions={
          channel && !deleted ? (
            <>
              <Link className="button" to={`/async-channels/${name}/edit`}>
                Edit
              </Link>
              {channel.state === "CHANNEL_STATE_PAUSED" ? (
                <button className="button" onClick={() => resume.mutate()} disabled={resume.isPending}>
                  Resume
                </button>
              ) : (
                <button className="button" onClick={() => pause.mutate()} disabled={pause.isPending}>
                  Pause
                </button>
              )}
              <button
                className="button danger"
                onClick={() => {
                  if (confirm(`Delete Async Channel ${name}?`)) {
                    remove.mutate(undefined, { onSuccess: () => navigate("/async-channels") });
                  }
                }}
              >
                Delete
              </button>
            </>
          ) : null
        }
      />
      <ErrorBanner error={error ?? pause.error ?? resume.error ?? remove.error} />

      {isLoading ? (
        <Loading what="channel" />
      ) : !channel ? (
        <Empty>
          Async Channel not found. <Link to="/async-channels">Back to Async Channels</Link>
        </Empty>
      ) : (
        <>
          <Panel
            title="Intent"
            note="What the operator declared. The channel declares the intent; Franz creates and maintains its generated Kafka Topics."
          >
            <dl className="detail-grid">
              <dt>State</dt>
              <dd data-testid="channel-state">
                <StatusPill value={channel.state} />
              </dd>
              <dt>Channel type</dt>
              <dd>
                <code>{channelTypeLabel(channel.type)}</code>
              </dd>
              <dt>Channel partitions</dt>
              <dd>
                {declaredPartitions}{" "}
                <span className="panel-note">
                  generated Kafka Topics — distinct from Kafka topic partitions
                </span>
              </dd>
              <dt>Labels</dt>
              <dd>
                {Object.entries(channel.labels ?? {}).map(([key, value]) => (
                  <span className="tag" key={key}>
                    {key}={value}
                  </span>
                ))}
                {Object.keys(channel.labels ?? {}).length === 0 ? (
                  <span className="panel-note">none</span>
                ) : null}
              </dd>
              <dt>FRN</dt>
              <dd>
                <code>{channel.frn}</code>
              </dd>
              <dt>Created</dt>
              <dd>{channel.createdAt ? new Date(channel.createdAt).toLocaleString() : "—"}</dd>
              <dt>Updated</dt>
              <dd>{channel.updatedAt ? new Date(channel.updatedAt).toLocaleString() : "—"}</dd>
            </dl>
          </Panel>

          <Panel
            title="Generated Kafka Topics"
            note="Each channel partition becomes a Kafka Topic once placement assigns it a cluster (ADR-API-009)."
          >
            <p className="panel-note" data-testid="shard-placement">
              Shards: {shards.length} of {declaredPartitions} placed
              {shards.length < declaredPartitions
                ? " — add a franz.affinity/selector that matches a registered cluster"
                : "."}
            </p>
            {shards.length > 0 ? (
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Topic</th>
                      <th>Cluster</th>
                      <th>Partitions</th>
                      <th>RF</th>
                      <th>State</th>
                      <th>Migration</th>
                    </tr>
                  </thead>
                  <tbody>
                    {shards.map((s) => {
                      // A shard may appear as either source or target across
                      // its history; the one row shown is whichever is not
                      // yet DONE/FAILED, or the most recent otherwise.
                      const shardMigrations = migrations.filter(
                        (m) => m.sourceKafkaTopic === s.name || m.targetKafkaTopic === s.name,
                      );
                      const activeMigration =
                        shardMigrations.find((m) => m.phase !== "MIGRATION_PHASE_DONE" && m.phase !== "MIGRATION_PHASE_FAILED") ??
                        shardMigrations[shardMigrations.length - 1];
                      const otherClusters = clusters.filter((c) => c.name !== s.kafkaCluster);

                      return (
                        <tr key={s.name}>
                          <td>
                            <code>{s.name}</code>
                          </td>
                          <td>
                            {s.kafkaCluster ? (
                              <Link to={`/kafka/clusters/${s.kafkaCluster}`}>{s.kafkaCluster}</Link>
                            ) : (
                              <span className="panel-note">unplaced</span>
                            )}
                          </td>
                          <td>{s.partitions ?? "—"}</td>
                          <td>{s.replicationFactor ?? "—"}</td>
                          <td>
                            <StatusPill value={s.state} />
                            {s.misplaced ? (
                              <span className="status paused" title={s.misplacedReason}>
                                Misplaced
                              </span>
                            ) : null}
                          </td>
                          <td>
                            {activeMigration ? (
                              <MigrationPhaseBadge
                                phase={activeMigration.phase}
                                failureReason={activeMigration.failureReason}
                              />
                            ) : migratingShard === s.name ? (
                              <span className="action-row-fields">
                                <select
                                  aria-label={`Target cluster for ${s.name}`}
                                  value={targetCluster}
                                  onChange={(e) => setTargetCluster(e.target.value)}
                                >
                                  <option value="" disabled>
                                    target cluster…
                                  </option>
                                  {otherClusters.map((c) => (
                                    <option key={c.name} value={c.name}>
                                      {c.name}
                                    </option>
                                  ))}
                                </select>
                                <button
                                  type="button"
                                  className="button"
                                  disabled={!targetCluster || migrateTopic.isPending}
                                  onClick={() =>
                                    migrateTopic.mutate(
                                      { kafkaTopic: s.name!, targetCluster },
                                      { onSuccess: () => setMigratingShard(null) },
                                    )
                                  }
                                >
                                  Go
                                </button>
                                <button type="button" className="button" onClick={() => setMigratingShard(null)}>
                                  Cancel
                                </button>
                              </span>
                            ) : s.kafkaCluster ? (
                              <button
                                type="button"
                                className="button"
                                onClick={() => {
                                  setTargetCluster("");
                                  setMigratingShard(s.name ?? null);
                                }}
                              >
                                Migrate to…
                              </button>
                            ) : (
                              <span className="panel-note">—</span>
                            )}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            ) : null}
            <ErrorBanner error={migrateTopic.error} />
          </Panel>

          <Panel title="Access policy" note="Which clients may use this channel.">
            <p className="empty-note">
              Access policy — managed in a later release. This channel has no policy editor yet, so it
              stays closed: a client with no matching Allow has no access.
            </p>
          </Panel>
        </>
      )}
    </>
  );
}
