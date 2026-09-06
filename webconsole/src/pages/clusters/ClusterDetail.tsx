import { Link, useNavigate, useParams } from "react-router-dom";
import {
  Breadcrumbs,
  Empty,
  ErrorBanner,
  Loading,
  PageHeading,
  Panel,
  StatusPill,
} from "../../components/ui";
import {
  useCluster,
  useClusterLifecycle,
  useClusterProviderEvents,
} from "../../api/hooks";
import { providerPhaseLabel } from "../../api/enums";

// The detail page polls GetKafkaCluster + the event history every 4s (06.6) so
// provider status trends toward READY without a manual refresh.
const POLL_MS = 4000;

export function ClusterDetail() {
  const { name = "" } = useParams();
  const navigate = useNavigate();
  const { data, isLoading, error } = useCluster(name, { pollMs: POLL_MS });
  const eventsQuery = useClusterProviderEvents(name, { pollMs: POLL_MS });
  const { pause, resume, remove } = useClusterLifecycle(name);

  const cluster = data?.kafkaCluster;
  const status = cluster?.providerStatus;
  const events = eventsQuery.data?.events ?? [];
  const deleted = cluster?.state === "KAFKA_CLUSTER_STATE_DELETED";

  return (
    <>
      <Breadcrumbs
        items={[
          { label: "Franz Console", to: "/" },
          { label: "Kafka Clusters", to: "/kafka/clusters" },
          { label: name },
        ]}
      />
      <PageHeading
        title={name}
        lead={cluster?.frn}
        actions={
          cluster && !deleted ? (
            <>
              {cluster.state === "KAFKA_CLUSTER_STATE_PAUSED" ? (
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
                  if (confirm(`Delete cluster ${name}?`)) {
                    remove.mutate(undefined, { onSuccess: () => navigate("/kafka/clusters") });
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
        <Loading what="cluster" />
      ) : !cluster ? (
        <Empty>
          Cluster not found. <Link to="/kafka/clusters">Back to Kafka Clusters</Link>
        </Empty>
      ) : (
        <>
          <Panel title="Intent" note="What the operator declared. Agents never change this.">
            <dl className="detail-grid">
              <dt>State</dt>
              <dd>
                <StatusPill value={cluster.state} />
              </dd>
              <dt>Bootstrap</dt>
              <dd>
                {(cluster.connectionStrings?.[0]?.bootstrapUrls ?? []).map((u) => (
                  <code key={u} style={{ marginRight: 8 }}>
                    {u}
                  </code>
                ))}
              </dd>
              <dt>Cluster Provider</dt>
              <dd>
                {cluster.clusterProviderAgent ? (
                  <Link to={`/agents/${cluster.clusterProviderAgent}`}>{cluster.clusterProviderAgent}</Link>
                ) : (
                  <span className="panel-note">Not linked</span>
                )}
              </dd>
              <dt>Labels</dt>
              <dd>
                {Object.entries(cluster.labels ?? {}).map(([k, v]) => (
                  <span className="tag" key={k}>
                    {k}={v}
                  </span>
                ))}
              </dd>
              <dt>cluster_configuration</dt>
              <dd>
                {Object.entries(cluster.clusterConfiguration ?? {}).map(([k, v]) => (
                  <span className="tag" key={k}>
                    {k}={v}
                  </span>
                ))}
                {Object.keys(cluster.clusterConfiguration ?? {}).length === 0 ? (
                  <span className="panel-note">none</span>
                ) : null}
              </dd>
            </dl>
          </Panel>

          <Panel
            title="Provider status"
            note="What the Cluster Provider agent last reported — refreshes automatically."
          >
            {status?.phase ? (
              <dl className="detail-grid">
                <dt>Phase</dt>
                <dd data-testid="provider-phase">
                  <StatusPill value={status.phase} />
                </dd>
                <dt>Reachable</dt>
                <dd>{status.reachable ? "Yes" : "No"}</dd>
                <dt>Recipe</dt>
                <dd>
                  <code>{status.recipeRef || "—"}</code>
                </dd>
                <dt>Reported by</dt>
                <dd>{status.reportingAgent || "—"}</dd>
                <dt>Reported at</dt>
                <dd>{status.reportedAt ? new Date(status.reportedAt).toLocaleString() : "—"}</dd>
                {status.message ? (
                  <>
                    <dt>Message</dt>
                    <dd>{status.message}</dd>
                  </>
                ) : null}
              </dl>
            ) : (
              <p className="empty-note" data-testid="provider-phase">
                {providerPhaseLabel(undefined)} — no Cluster Provider agent has reported yet.
              </p>
            )}
          </Panel>

          <Panel title="Provider event timeline" note="Append-only history of agent status reports (last 30 days).">
            <ErrorBanner error={eventsQuery.error} />
            {events.length === 0 ? (
              <Empty>No provider events yet.</Empty>
            ) : (
              <ul className="timeline">
                {events.map((ev, i) => (
                  <li key={i}>
                    <time>{ev.occurredAt ? new Date(ev.occurredAt).toLocaleString() : "—"}</time>
                    <div>
                      <StatusPill value={ev.phase} /> {ev.reachable ? "reachable" : "unreachable"}
                      {ev.recipeRef ? (
                        <>
                          {" "}
                          · <code>{ev.recipeRef}</code>
                        </>
                      ) : null}
                      {ev.message ? <div className="panel-note">{ev.message}</div> : null}
                    </div>
                  </li>
                ))}
              </ul>
            )}
          </Panel>
        </>
      )}
    </>
  );
}
