import { Link } from "react-router-dom";
import { Breadcrumbs, PageHeading } from "../components/ui";
import { useAgents, useClusters } from "../api/hooks";

export function Home() {
  const agents = useAgents();
  const clusters = useClusters();

  return (
    <>
      <Breadcrumbs items={[{ label: "Franz Console" }]} />
      <PageHeading
        title="Console Home"
        lead="Feature 1 — register a Cluster Provider agent and stand a Kafka cluster up in Docker from the browser."
      />
      <section className="stats" aria-label="Fleet summary">
        <div className="stat">
          <div className="stat-label">Kafka Clusters</div>
          <div className="stat-value">{clusters.data?.kafkaClusters?.length ?? "—"}</div>
          <div className="stat-foot">
            <Link to="/kafka/clusters">View clusters</Link>
          </div>
        </div>
        <div className="stat">
          <div className="stat-label">Agents</div>
          <div className="stat-value">{agents.data?.agents?.length ?? "—"}</div>
          <div className="stat-foot">
            <Link to="/agents">View agents</Link>
          </div>
        </div>
      </section>
      <section className="panel">
        <div className="panel-header">
          <div>
            <h2>Services</h2>
            <p className="panel-note">Choose a service to manage its declared resources.</p>
          </div>
        </div>
        <div className="panel-body">
          <div className="service-grid">
            <Link className="service-card" to="/kafka/clusters">
              <div className="service-icon">▦</div>
              <h3>Kafka Clusters</h3>
              <p>Register Kafka clusters in the control plane and describe their context with fleet labels.</p>
              <span className="service-action">Open service →</span>
            </Link>
            <Link className="service-card" to="/agents">
              <div className="service-icon">⛁</div>
              <h3>Agents</h3>
              <p>Register the programs that connect to the fleet API — cluster providers, resource providers, telemetry agents.</p>
              <span className="service-action">Open service →</span>
            </Link>
          </div>
        </div>
      </section>
    </>
  );
}
