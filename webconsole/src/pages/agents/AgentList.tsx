import { Link } from "react-router-dom";
import { Breadcrumbs, Empty, ErrorBanner, Loading, PageHeading, StatusPill } from "../../components/ui";
import { useAgents } from "../../api/hooks";
import { agentTypeLabel } from "../../api/enums";

export function AgentList() {
  const { data, isLoading, error } = useAgents();
  const agents = data?.agents ?? [];

  return (
    <>
      <Breadcrumbs items={[{ label: "Franz Console", to: "/" }, { label: "Agents" }]} />
      <PageHeading
        title="Agents"
        lead="Programs registered with Franz that connect to the fleet API over gRPC and act on the values it returns."
        actions={
          <Link className="button primary" to="/agents/register">
            Register Agent
          </Link>
        }
      />
      <ErrorBanner error={error} />
      <section className="panel">
        <div className="toolbar">
          <span className="panel-note">
            {agents.length} registered agent{agents.length === 1 ? "" : "s"}
          </span>
        </div>
        {isLoading ? (
          <Loading what="agents" />
        ) : agents.length === 0 ? (
          <Empty>
            No agents yet. <Link to="/agents/register">Register one</Link> to get started.
          </Empty>
        ) : (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Agent name</th>
                  <th>Type</th>
                  <th>Labels</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {agents.map((a) => (
                  <tr key={a.name}>
                    <td>
                      <Link to={`/agents/${a.name}`}>{a.name}</Link>
                      <small className="resource-id">{a.frn}</small>
                    </td>
                    <td>{agentTypeLabel(a.type)}</td>
                    <td>
                      {Object.entries(a.labels ?? {}).map(([k, v]) => (
                        <span className="tag" key={k}>
                          {k}={v}
                        </span>
                      ))}
                    </td>
                    <td>
                      <StatusPill value={a.status} />
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
