import { Link, useNavigate, useParams } from "react-router-dom";
import { Breadcrumbs, Empty, ErrorBanner, Loading, PageHeading, Panel, StatusPill } from "../../components/ui";
import { useChannel, useChannelLifecycle, useChannelTopics } from "../../api/hooks";
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
                    </tr>
                  </thead>
                  <tbody>
                    {shards.map((s) => (
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
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : null}
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
