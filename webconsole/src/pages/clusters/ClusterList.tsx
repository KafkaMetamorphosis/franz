import { Link } from "react-router-dom";
import { Breadcrumbs, Empty, ErrorBanner, Loading, PageHeading, StatusPill } from "../../components/ui";
import { useClusters } from "../../api/hooks";
import { providerPhaseLabel } from "../../api/enums";

export function ClusterList() {
  const { data, isLoading, error } = useClusters();
  const clusters = data?.kafkaClusters ?? [];

  return (
    <>
      <Breadcrumbs items={[{ label: "Franz Console", to: "/" }, { label: "Kafka Clusters" }]} />
      <PageHeading
        title="Kafka Clusters"
        lead="Registered Kafka clusters and their declared fleet context. Registration is intent only — agents do the work."
        actions={
          <Link className="button primary" to="/kafka/clusters/register">
            Register Kafka Cluster
          </Link>
        }
      />
      <ErrorBanner error={error} />
      <section className="panel">
        <div className="toolbar">
          <span className="panel-note">
            {clusters.length} registered cluster{clusters.length === 1 ? "" : "s"}
          </span>
        </div>
        {isLoading ? (
          <Loading what="clusters" />
        ) : clusters.length === 0 ? (
          <Empty>
            No clusters yet. <Link to="/kafka/clusters/register">Register one</Link>.
          </Empty>
        ) : (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Cluster name</th>
                  <th>Bootstrap</th>
                  <th>Labels</th>
                  <th>Provider</th>
                  <th>State</th>
                  <th>Provider status</th>
                </tr>
              </thead>
              <tbody>
                {clusters.map((c) => (
                  <tr key={c.name}>
                    <td>
                      <Link to={`/kafka/clusters/${c.name}`}>{c.name}</Link>
                      <small className="resource-id">{c.frn}</small>
                    </td>
                    <td>
                      <code>{c.connectionStrings?.[0]?.bootstrapUrls?.[0] ?? "—"}</code>
                    </td>
                    <td>
                      {Object.entries(c.labels ?? {}).map(([k, v]) => (
                        <span className="tag" key={k}>
                          {k}={v}
                        </span>
                      ))}
                    </td>
                    <td>
                      {c.clusterProviderAgent ? (
                        <Link to={`/agents/${c.clusterProviderAgent}`}>{c.clusterProviderAgent}</Link>
                      ) : (
                        <span className="panel-note">Not linked</span>
                      )}
                    </td>
                    <td>
                      <StatusPill value={c.state} />
                    </td>
                    <td>
                      {c.providerStatus?.phase ? (
                        <StatusPill value={c.providerStatus.phase} />
                      ) : (
                        <span className="panel-note">{providerPhaseLabel(undefined)}</span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </>
  );
}
