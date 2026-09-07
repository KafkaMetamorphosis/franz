import { Link, useNavigate, useParams } from "react-router-dom";
import { Breadcrumbs, Empty, ErrorBanner, Loading, PageHeading, Panel, StatusPill } from "../../components/ui";
import { useChannel, useChannelLifecycle } from "../../api/hooks";
import { channelTypeLabel } from "../../api/enums";

// Ported from 001-ux/demo/async-channel-detail.html. The Access policy, Clients
// with access and Generated Kafka Topics panels are intentionally absent: the
// policy engine and the client views ship with deliverable 17, and shard rows
// are materialised by placement in deliverable 13 (ADR-API-009).
export function ChannelDetail() {
  const { name = "" } = useParams();
  const navigate = useNavigate();
  const { data, isLoading, error } = useChannel(name);
  const { pause, resume, remove } = useChannelLifecycle(name);

  const channel = data?.asyncChannel;
  const deleted = channel?.state === "CHANNEL_STATE_DELETED";
  const declaredPartitions = channel?.channelPartitions ?? 0;

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
            note="Each channel partition becomes a Kafka Topic once placement assigns it a cluster."
          >
            <p className="empty-note" data-testid="shard-placement">
              Shards: 0 of {declaredPartitions} placed — placement not yet enabled. Shard rows are
              materialised by placement (deliverable 13), not at channel create.
            </p>
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
