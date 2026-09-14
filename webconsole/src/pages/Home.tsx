import { Link } from "react-router-dom";
import { Breadcrumbs, PageHeading } from "../components/ui";
import { useAgents, useChannels, useClients, useClusters, useIndicators, usePolicies } from "../api/hooks";

export function Home() {
  const agents = useAgents();
  const clusters = useClusters();
  const channels = useChannels();
  const indicators = useIndicators();
  const policies = usePolicies();
  const clients = useClients();

  return (
    <>
      <Breadcrumbs items={[{ label: "Franz Console" }]} />
      <PageHeading
        title="Console Home"
        lead="Feature 1 — register a Cluster Provider agent and stand a Kafka cluster up in Docker from the browser."
      />
      <section className="stats" aria-label="Fleet summary">
        <div className="stat">
          <div className="stat-label">Async Channels</div>
          <div className="stat-value">{channels.data?.asyncChannels?.length ?? "—"}</div>
          <div className="stat-foot">
            <Link to="/async-channels">View channels</Link>
          </div>
        </div>
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
        <div className="stat">
          <div className="stat-label">Indicators</div>
          <div className="stat-value">{indicators.data?.indicators?.length ?? "—"}</div>
          <div className="stat-foot">
            <Link to="/governance/indicators">View indicators</Link>
          </div>
        </div>
        <div className="stat">
          <div className="stat-label">Policies</div>
          <div className="stat-value">{policies.data?.policies?.length ?? "—"}</div>
          <div className="stat-foot">
            <Link to="/governance/policies">View policies</Link>
          </div>
        </div>
        <div className="stat">
          <div className="stat-label">Clients</div>
          <div className="stat-value">{clients.data?.clients?.length ?? "—"}</div>
          <div className="stat-foot">
            <Link to="/clients">View clients</Link>
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
            <Link className="service-card" to="/async-channels">
              <div className="service-icon">⇄</div>
              <h3>Async Channels</h3>
              <p>Declare the customer-facing async boundaries Franz manages, and the labels that place them.</p>
              <span className="service-action">Open service →</span>
            </Link>
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
            <Link className="service-card" to="/governance/indicators">
              <div className="service-icon">◎</div>
              <h3>Governance</h3>
              <p>Register indicators Telemetry Agents publish samples for, and author policies that react when a limit is crossed.</p>
              <span className="service-action">Open service →</span>
            </Link>
            <Link className="service-card" to="/clients">
              <div className="service-icon">◇</div>
              <h3>Clients</h3>
              <p>The fleet-wide SDK identity — see which channels a client may use and the consumer groups it's been observed running.</p>
              <span className="service-action">Open service →</span>
            </Link>
          </div>
        </div>
      </section>
    </>
  );
}
