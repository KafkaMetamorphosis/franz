import { useEffect, useMemo, useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { Breadcrumbs, ErrorBanner, PageHeading, Panel } from "../../components/ui";
import { LabelEditor } from "../../components/LabelEditor";
import { useAgents, useCreateCluster } from "../../api/hooks";
import { formatKeyValues, parseKeyValues } from "../../keyvalues";
import { KAFKA_VERSION_KEY, defaultsFromAgent } from "../../clusterConfig";

export function ClusterRegister() {
  const navigate = useNavigate();
  const createCluster = useCreateCluster();
  const agentsQuery = useAgents("AGENT_TYPE_CLUSTER_PROVIDER");

  const [name, setName] = useState("");
  const [bootstrap, setBootstrap] = useState("");
  const [providerAgent, setProviderAgent] = useState("");
  const [labels, setLabels] = useState<Record<string, string>>({});
  const [config, setConfig] = useState("");
  const [version, setVersion] = useState("");
  const [brokers, setBrokers] = useState("");
  const [diskSize, setDiskSize] = useState("");
  const [localError, setLocalError] = useState<string | null>(null);

  const providerAgents = agentsQuery.data?.agents ?? [];
  const selectedAgent = providerAgents.find((a) => a.name === providerAgent);
  const defaults = useMemo(() => defaultsFromAgent(selectedAgent), [selectedAgent]);

  // Pre-fill the config section whenever the selected provider agent changes.
  useEffect(() => {
    setConfig(formatKeyValues(defaults.config));
    setVersion(defaults.defaultVersion);
  }, [defaults]);

  return (
    <>
      <Breadcrumbs
        items={[
          { label: "Franz Console", to: "/" },
          { label: "Kafka Clusters", to: "/kafka/clusters" },
          { label: "Register Kafka Cluster" },
        ]}
      />
      <PageHeading
        title="Register Kafka Cluster"
        lead="Record a Kafka Cluster and its configuration. A linked Cluster Provider agent stands the substrate up."
      />
      <ErrorBanner error={localError ?? createCluster.error} />
      <Panel title="Cluster details" note="Registration records declared intent. Franz never connects to the cluster.">
        <form
          className="form-layout"
          onSubmit={(e) => {
            e.preventDefault();
            setLocalError(null);
            const bootstrapUrls = bootstrap.split(",").map((s) => s.trim()).filter(Boolean);
            if (bootstrapUrls.length === 0) {
              setLocalError("At least one bootstrap URL is required.");
              return;
            }
            const clusterConfiguration = parseKeyValues(config);
            if (version) clusterConfiguration[KAFKA_VERSION_KEY] = version;
            createCluster.mutate(
              {
                name: name.trim(),
                connectionStrings: [
                  { bootstrapUrls, type: "CONNECTION_TYPE_PLAINTEXT" as never },
                ],
                labels,
                clusterConfiguration,
                clusterProviderAgent: providerAgent || undefined,
                brokers: brokers ? Number(brokers) : undefined,
                diskSize: diskSize || undefined,
              },
              { onSuccess: (res) => navigate(`/kafka/clusters/${res.kafkaCluster?.name ?? name.trim()}`) },
            );
          }}
        >
          <div className="form-section">
            <h3>Identity and endpoint</h3>
            <div className="field">
              <label htmlFor="cluster-name">
                Cluster name <small>The control-plane identifier. Immutable.</small>
              </label>
              <div>
                <input
                  id="cluster-name"
                  required
                  placeholder="local-1"
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                />
              </div>
            </div>
            <div className="field">
              <label htmlFor="bootstrap">
                Bootstrap URL(s) <small>Comma-separated. Recorded, not validated.</small>
              </label>
              <div>
                <input
                  id="bootstrap"
                  required
                  placeholder="localhost:9092"
                  value={bootstrap}
                  onChange={(e) => setBootstrap(e.target.value)}
                />
              </div>
            </div>
          </div>

          <div className="form-section">
            <h3>Cluster Provider</h3>
            <p className="form-help">The Cluster Provider agent that deploys and maintains this cluster&rsquo;s substrate.</p>
            <div className="field">
              <label htmlFor="provider">
                Cluster Provider <small>Optional. Link one later from the detail page.</small>
              </label>
              <div>
                <select id="provider" value={providerAgent} onChange={(e) => setProviderAgent(e.target.value)}>
                  <option value="">No provider — managed outside Franz</option>
                  {providerAgents.map((a) => (
                    <option key={a.name} value={a.name}>
                      {a.name}
                    </option>
                  ))}
                </select>
                {providerAgents.length === 0 ? (
                  <p className="field-note">
                    No Cluster Provider agents registered. <Link to="/agents/register">Register one</Link>.
                  </p>
                ) : null}
              </div>
            </div>
          </div>

          <div className="form-section">
            <h3>Cluster configuration</h3>
            <p className="form-help">
              {selectedAgent
                ? `Pre-filled from ${selectedAgent.name}'s advertised defaults. Advisory — Franz enforces nothing.`
                : "The single home for this cluster's Kafka config."}
            </p>
            <div className="field">
              <label htmlFor="kafka-version">Kafka version</label>
              <div>
                <select id="kafka-version" value={version} onChange={(e) => setVersion(e.target.value)}>
                  {(defaults.versions.includes(version) ? defaults.versions : [version, ...defaults.versions])
                    .filter(Boolean)
                    .map((v) => (
                      <option key={v} value={v}>
                        {v}
                      </option>
                    ))}
                </select>
              </div>
            </div>
            <div className="field">
              <label htmlFor="brokers">
                Brokers <small>Desired broker count. Forwarded to the agent.</small>
              </label>
              <div>
                <input
                  id="brokers"
                  type="number"
                  min={1}
                  placeholder="1"
                  value={brokers}
                  onChange={(e) => setBrokers(e.target.value)}
                />
              </div>
            </div>
            <div className="field">
              <label htmlFor="disk-size">
                Disk size <small>Size hint, e.g. 50Gi. Forwarded to the agent.</small>
              </label>
              <div>
                <input
                  id="disk-size"
                  placeholder="50Gi"
                  value={diskSize}
                  onChange={(e) => setDiskSize(e.target.value)}
                />
              </div>
            </div>
            <div className="field">
              <label htmlFor="config">
                cluster_configuration <small>One <code>key = value</code> per line.</small>
              </label>
              <div>
                <textarea
                  id="config"
                  placeholder={"partitions = 3\nreplication-factor = 1"}
                  value={config}
                  onChange={(e) => setConfig(e.target.value)}
                />
              </div>
            </div>
          </div>

          <div className="form-section">
            <h3>Labels</h3>
            <p className="form-help">Fleet-context metadata and reserved <code>franz.placement/*</code> coordinates.</p>
            <div className="field">
              <label>Labels</label>
              <div>
                <LabelEditor value={labels} onChange={setLabels} />
              </div>
            </div>
          </div>

          <div className="form-actions">
            <Link className="button" to="/kafka/clusters">
              Cancel
            </Link>
            <button className="button primary" type="submit" disabled={createCluster.isPending}>
              {createCluster.isPending ? "Registering…" : "Register Kafka Cluster"}
            </button>
          </div>
        </form>
      </Panel>
    </>
  );
}
