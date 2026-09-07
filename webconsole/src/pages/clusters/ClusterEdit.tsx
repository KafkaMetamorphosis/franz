import { useMemo, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { Breadcrumbs, ErrorBanner, Loading, PageHeading, Panel } from "../../components/ui";
import { LabelEditor } from "../../components/LabelEditor";
import { ApiError } from "../../api/client";
import { useAgents, useCluster, useUpdateKafkaCluster, updateMask } from "../../api/hooks";
import { formatKeyValues, parseKeyValues } from "../../keyvalues";
import { KAFKA_VERSION_KEY } from "../../clusterConfig";

export function ClusterEdit() {
  const { name = "" } = useParams();
  const navigate = useNavigate();
  const { data, isLoading, error } = useCluster(name);
  const update = useUpdateKafkaCluster(name);
  const agentsQuery = useAgents("AGENT_TYPE_CLUSTER_PROVIDER");
  const cluster = data?.kafkaCluster;
  const providerAgents = agentsQuery.data?.agents ?? [];

  const base = useMemo(() => {
    if (!cluster) return null;
    const cfg = { ...(cluster.clusterConfiguration ?? {}) };
    const version = cfg[KAFKA_VERSION_KEY] ?? "";
    delete cfg[KAFKA_VERSION_KEY];
    return {
      bootstrap: (cluster.connectionStrings?.[0]?.bootstrapUrls ?? []).join(", "),
      labels: { ...(cluster.labels ?? {}) },
      config: formatKeyValues(cfg),
      version,
      brokers: cluster.brokers ? String(cluster.brokers) : "",
      diskSize: cluster.diskSize ?? "",
      providerAgent: cluster.clusterProviderAgent ?? "",
    };
  }, [cluster]);

  const [draft, setDraft] = useState<null | NonNullable<typeof base>>(null);
  const [confirmedReassign, setConfirmedReassign] = useState(false);
  const [localError, setLocalError] = useState<string | null>(null);
  const form = draft ?? base;

  if (isLoading) return <Loading what="cluster" />;
  if (error || !cluster || !form || !base) {
    return (
      <>
        <ErrorBanner error={error} />
        <p className="empty-note">
          Cluster not found. <Link to="/kafka/clusters">Back to Kafka Clusters</Link>
        </p>
      </>
    );
  }

  const deleted = cluster.state === "KAFKA_CLUSTER_STATE_DELETED";
  const set = (patch: Partial<NonNullable<typeof draft>>) => setDraft({ ...form, ...patch });

  const providerChanged = form.providerAgent !== base.providerAgent;
  const needsReassignConfirm = providerChanged && base.providerAgent !== "";

  const bootstrapUrls = form.bootstrap.split(",").map((s) => s.trim()).filter(Boolean);
  const configChanged = form.config !== base.config || form.version !== base.version;
  const changed: string[] = [];
  if (form.bootstrap !== base.bootstrap) changed.push("connectionStrings");
  if (JSON.stringify(form.labels) !== JSON.stringify(base.labels)) changed.push("labels");
  if (configChanged) changed.push("clusterConfiguration");
  if (providerChanged) changed.push("clusterProviderAgent");
  if (form.brokers !== base.brokers) changed.push("brokers");
  if (form.diskSize !== base.diskSize) changed.push("diskSize");
  const noChange = changed.length === 0;

  const save = () => {
    setLocalError(null);
    if (changed.includes("connectionStrings") && bootstrapUrls.length === 0) {
      setLocalError("At least one bootstrap URL is required.");
      return;
    }
    if (needsReassignConfirm && !confirmedReassign) {
      setLocalError("Confirm the provider re-assignment before saving.");
      return;
    }

    const body: Record<string, unknown> = { updateMask: updateMask(changed) };
    if (changed.includes("connectionStrings"))
      body.connectionStrings = [
        { bootstrapUrls, type: cluster.connectionStrings?.[0]?.type ?? "CONNECTION_TYPE_PLAINTEXT" },
      ];
    if (changed.includes("labels")) body.labels = form.labels;
    if (changed.includes("clusterConfiguration")) {
      const cfg = parseKeyValues(form.config);
      if (form.version) cfg[KAFKA_VERSION_KEY] = form.version;
      body.clusterConfiguration = cfg;
    }
    if (changed.includes("clusterProviderAgent")) body.clusterProviderAgent = form.providerAgent;
    if (changed.includes("brokers")) body.brokers = form.brokers ? Number(form.brokers) : 0;
    if (changed.includes("diskSize")) body.diskSize = form.diskSize;

    update.mutate(body, { onSuccess: () => navigate(`/kafka/clusters/${name}`) });
  };

  const conflict = update.error instanceof ApiError && update.error.status === 409;

  return (
    <>
      <Breadcrumbs
        items={[
          { label: "Franz Console", to: "/" },
          { label: "Kafka Clusters", to: "/kafka/clusters" },
          { label: name, to: `/kafka/clusters/${name}` },
          { label: "Edit" },
        ]}
      />
      <PageHeading title={`Edit ${name}`} lead={cluster.frn} />
      <ErrorBanner error={localError ?? update.error} />
      {conflict ? (
        <div className="app-error" role="alert">
          The cluster changed since you opened this form.{" "}
          <button
            type="button"
            className="button"
            onClick={() => {
              setDraft(null);
              setConfirmedReassign(false);
              update.reset();
            }}
          >
            Reload and re-apply
          </button>
        </div>
      ) : null}
      {deleted ? <p className="empty-note">This cluster is deleted and cannot be edited.</p> : null}

      <Panel title="Editable fields" note="name, FRN, state and provider status are immutable here. Use the detail page for pause / resume / delete.">
        <form
          className="form-layout"
          onSubmit={(e) => {
            e.preventDefault();
            save();
          }}
        >
          <div className="form-section">
            <h3>Endpoint</h3>
            <div className="field">
              <label htmlFor="bootstrap">
                Bootstrap URL(s) <small>Comma-separated. Recorded, not validated.</small>
              </label>
              <div>
                <input
                  id="bootstrap"
                  value={form.bootstrap}
                  disabled={deleted}
                  onChange={(e) => set({ bootstrap: e.target.value })}
                />
              </div>
            </div>
          </div>

          <div className="form-section">
            <h3>Cluster Provider</h3>
            <div className="field">
              <label htmlFor="provider">Cluster Provider agent</label>
              <div>
                <select
                  id="provider"
                  value={form.providerAgent}
                  disabled={deleted}
                  onChange={(e) => {
                    setConfirmedReassign(false);
                    set({ providerAgent: e.target.value });
                  }}
                >
                  <option value="">No provider — managed outside Franz</option>
                  {providerAgents.map((a) => (
                    <option key={a.name} value={a.name}>
                      {a.name}
                    </option>
                  ))}
                  {form.providerAgent && !providerAgents.some((a) => a.name === form.providerAgent) ? (
                    <option value={form.providerAgent}>{form.providerAgent} (not found)</option>
                  ) : null}
                </select>
              </div>
            </div>
            {needsReassignConfirm ? (
              <label className="checkbox-field reassign-warning">
                <input
                  type="checkbox"
                  checked={confirmedReassign}
                  onChange={(e) => setConfirmedReassign(e.target.checked)}
                />
                I understand: <strong>{base.providerAgent}</strong> will tear its substrate down and{" "}
                {form.providerAgent ? (
                  <>
                    <strong>{form.providerAgent}</strong> will re-provision this cluster.
                  </>
                ) : (
                  <>this cluster will no longer be provisioned by Franz.</>
                )}
              </label>
            ) : null}
          </div>

          <div className="form-section">
            <h3>Cluster configuration</h3>
            <div className="field">
              <label htmlFor="kafka-version">Kafka version</label>
              <div>
                <input
                  id="kafka-version"
                  value={form.version}
                  disabled={deleted}
                  onChange={(e) => set({ version: e.target.value })}
                />
              </div>
            </div>
            <div className="field">
              <label htmlFor="brokers">Brokers</label>
              <div>
                <input
                  id="brokers"
                  type="number"
                  min={1}
                  value={form.brokers}
                  disabled={deleted}
                  onChange={(e) => set({ brokers: e.target.value })}
                />
              </div>
            </div>
            <div className="field">
              <label htmlFor="disk-size">Disk size</label>
              <div>
                <input
                  id="disk-size"
                  value={form.diskSize}
                  disabled={deleted}
                  onChange={(e) => set({ diskSize: e.target.value })}
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
                  value={form.config}
                  disabled={deleted}
                  onChange={(e) => set({ config: e.target.value })}
                />
              </div>
            </div>
          </div>

          <div className="form-section">
            <h3>Labels</h3>
            <div className="field">
              <label>Labels</label>
              <div>
                <LabelEditor value={form.labels} onChange={(labels) => set({ labels })} />
              </div>
            </div>
          </div>

          <div className="form-actions">
            <Link className="button" to={`/kafka/clusters/${name}`}>
              Cancel
            </Link>
            <button
              className="button primary"
              type="submit"
              disabled={deleted || noChange || update.isPending}
            >
              {update.isPending ? "Saving…" : "Save changes"}
            </button>
          </div>
        </form>
      </Panel>
    </>
  );
}
