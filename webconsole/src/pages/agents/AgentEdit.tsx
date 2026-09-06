import { useMemo, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { Breadcrumbs, ErrorBanner, Loading, PageHeading, Panel } from "../../components/ui";
import { LabelEditor } from "../../components/LabelEditor";
import { ProvisioningLabelEditor } from "../../components/ProvisioningLabelEditor";
import { validateSchema } from "../../provisioning";
import { ApiError } from "../../api/client";
import { useAgent, useUpdateAgent, updateMask, type ProvisioningLabelSpec } from "../../api/hooks";
import { AGENT_TYPES } from "../../api/enums";

export function AgentEdit() {
  const { name = "" } = useParams();
  const navigate = useNavigate();
  const { data, isLoading, error } = useAgent(name);
  const update = useUpdateAgent(name);
  const agent = data?.agent;

  // Snapshot the server state once loaded; the form edits a copy.
  const [draft, setDraft] = useState<null | {
    type: string;
    labels: Record<string, string>;
    provisioningLabels: ProvisioningLabelSpec[];
  }>(null);
  const [localError, setLocalError] = useState<string | null>(null);

  const base = useMemo(() => {
    if (!agent) return null;
    return {
      type: agent.type ?? AGENT_TYPES[0].value,
      labels: { ...(agent.labels ?? {}) },
      provisioningLabels: (agent.provisioningLabels ?? []).map((s) => ({ ...s })),
    };
  }, [agent]);

  const form = draft ?? base;

  if (isLoading) return <Loading what="agent" />;
  if (error || !agent || !form) {
    return (
      <>
        <ErrorBanner error={error} />
        <p className="empty-note">
          Agent not found. <Link to="/agents">Back to Agents</Link>
        </p>
      </>
    );
  }

  const deleted = agent.status === "AGENT_STATUS_DELETED";
  const set = (patch: Partial<NonNullable<typeof draft>>) => setDraft({ ...form, ...patch });

  const changed: string[] = [];
  if (base) {
    if (form.type !== base.type) changed.push("type");
    if (JSON.stringify(form.labels) !== JSON.stringify(base.labels)) changed.push("labels");
    if (JSON.stringify(form.provisioningLabels) !== JSON.stringify(base.provisioningLabels))
      changed.push("provisioningLabels");
  }
  const noChange = changed.length === 0;

  const save = () => {
    setLocalError(null);
    if (changed.includes("provisioningLabels")) {
      const problem = validateSchema(form.provisioningLabels);
      if (problem) {
        setLocalError(problem);
        return;
      }
    }
    const body: Record<string, unknown> = { updateMask: updateMask(changed) };
    if (changed.includes("type")) body.type = form.type;
    if (changed.includes("labels")) body.labels = form.labels;
    if (changed.includes("provisioningLabels")) body.provisioningLabels = form.provisioningLabels;

    update.mutate(body, {
      onSuccess: () => navigate(`/agents/${name}`),
    });
  };

  const conflict = update.error instanceof ApiError && update.error.status === 409;

  return (
    <>
      <Breadcrumbs
        items={[
          { label: "Franz Console", to: "/" },
          { label: "Agents", to: "/agents" },
          { label: name, to: `/agents/${name}` },
          { label: "Edit" },
        ]}
      />
      <PageHeading title={`Edit ${name}`} lead={agent.frn} />
      <ErrorBanner error={localError ?? update.error} />
      {conflict ? (
        <div className="app-error" role="alert">
          The agent changed since you opened this form.{" "}
          <button type="button" className="button" onClick={() => { setDraft(null); update.reset(); }}>
            Reload and re-apply
          </button>
        </div>
      ) : null}
      {deleted ? <p className="empty-note">This agent is deleted and cannot be edited.</p> : null}

      <Panel title="Editable fields" note="name, FRN and status are immutable. Use the detail page for pause / resume / delete / rotate.">
        <form
          className="form-layout"
          onSubmit={(e) => {
            e.preventDefault();
            save();
          }}
        >
          <div className="form-section">
            <h3>Type</h3>
            <div className="field">
              <label htmlFor="agent-type">Agent type</label>
              <div>
                <select
                  id="agent-type"
                  value={form.type}
                  disabled={deleted}
                  onChange={(e) => set({ type: e.target.value })}
                >
                  {AGENT_TYPES.map((t) => (
                    <option key={t.value} value={t.value}>
                      {t.label}
                    </option>
                  ))}
                </select>
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

          <div className="form-section">
            <h3>Provisioning-label schema</h3>
            <p className="form-help">
              Advisory. The console pre-fills and constrains these fields on a resource form that
              targets this agent. Franz does not enforce them.
            </p>
            <ProvisioningLabelEditor
              value={form.provisioningLabels}
              onChange={(provisioningLabels) => set({ provisioningLabels })}
            />
          </div>

          <div className="form-actions">
            <Link className="button" to={`/agents/${name}`}>
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
