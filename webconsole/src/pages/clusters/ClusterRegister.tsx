import { useMemo, useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { Breadcrumbs, ErrorBanner, PageHeading, Panel } from "../../components/ui";
import { LabelEditor } from "../../components/LabelEditor";
import { ProvisioningFields } from "../../components/ProvisioningFields";
import { useAgents, useCreateCluster } from "../../api/hooks";
import { parseKeyValues } from "../../keyvalues";
import { FALLBACK_PROVISIONING_LABELS, missingRequired, prefilled } from "../../provisioning";

export function ClusterRegister() {
  const navigate = useNavigate();
  const createCluster = useCreateCluster();
  const agentsQuery = useAgents("AGENT_TYPE_CLUSTER_PROVIDER");

  const [name, setName] = useState("");
  const [bootstrap, setBootstrap] = useState("");
  const [providerAgent, setProviderAgent] = useState("");
  const [labels, setLabels] = useState<Record<string, string>>({});
  const [provisioning, setProvisioning] = useState<Record<string, string>>({});
  const [config, setConfig] = useState("");
  const [localError, setLocalError] = useState<string | null>(null);

  const providerAgents = agentsQuery.data?.agents ?? [];
  const selectedAgent = providerAgents.find((a) => a.name === providerAgent);
  const specs =
    (selectedAgent?.provisioningLabels?.length ?? 0) > 0
      ? selectedAgent!.provisioningLabels!
      : FALLBACK_PROVISIONING_LABELS;

  // Pre-fill the provisioning values with the schema defaults whenever the
  // selected agent (and therefore the spec list) changes.
  const provisioningValues = useMemo(() => prefilled(provisioning, specs), [provisioning, specs]);

  const parsedConfig = useMemo(() => parseKeyValues(config), [config]);

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
        lead="Record a Kafka Cluster and its provisioning intent. A linked Cluster Provider agent stands the substrate up."
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
            const missing = missingRequired(provisioningValues, specs);
            if (missing.length > 0) {
              setLocalError(`Required provisioning label(s) not set: ${missing.join(", ")}.`);
              return;
            }
            createCluster.mutate(
              {
                name: name.trim(),
                connectionStrings: [
                  { bootstrapUrls, type: "CONNECTION_TYPE_PLAINTEXT" as never },
                ],
                labels: { ...labels, ...provisioningValues },
                clusterConfiguration: parsedConfig,
                clusterProviderAgent: providerAgent || undefined,
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
            <h3>Provisioning intent</h3>
            <p className="form-help">
              {selectedAgent?.provisioningLabels?.length
                ? `Fields declared by ${selectedAgent.name}.`
                : "franz.provisioning/* labels the agent's recipe reads."}
            </p>
            <ProvisioningFields specs={specs} value={provisioningValues} onChange={setProvisioning} />
          </div>

          <div className="form-section">
            <h3>Context labels</h3>
            <div className="field">
              <label>Labels</label>
              <div>
                <LabelEditor value={labels} onChange={setLabels} />
              </div>
            </div>
          </div>

          <div className="form-section">
            <h3>Cluster configuration</h3>
            <p className="form-help">Default Kafka topic config, one <code>key=value</code> per line.</p>
            <div className="field">
              <label htmlFor="config">cluster_configuration</label>
              <div>
                <textarea
                  id="config"
                  placeholder={"default.replication.factor=1\nnum.partitions=3"}
                  value={config}
                  onChange={(e) => setConfig(e.target.value)}
                />
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
