import { useState } from "react";
import { Link } from "react-router-dom";
import { Breadcrumbs, CopyButton, ErrorBanner, PageHeading, Panel } from "../../components/ui";
import { LabelEditor } from "../../components/LabelEditor";
import { useCreateAgent } from "../../api/hooks";
import { AGENT_TYPES } from "../../api/enums";

export function AgentRegister() {
  const createAgent = useCreateAgent();

  const [name, setName] = useState("");
  const [type, setType] = useState<string>(AGENT_TYPES[0].value);
  const [labels, setLabels] = useState<Record<string, string>>({});
  const [token, setToken] = useState<string | null>(null);
  const [createdName, setCreatedName] = useState<string | null>(null);

  if (token && createdName) {
    return (
      <>
        <Breadcrumbs
          items={[
            { label: "Franz Console", to: "/" },
            { label: "Agents", to: "/agents" },
            { label: "Register Agent" },
          ]}
        />
        <PageHeading title={`Agent ${createdName} registered`} />
        <Panel title="Bearer token" note="Shown once. Franz stores only its hash — copy it now.">
          <div className="token-reveal">
            <code data-testid="agent-token">{token}</code>
            <CopyButton text={token} label="Copy token" />
            <p className="field-note">
              The agent presents this as <code>authorization: Bearer &lt;token&gt;</code>. Rotate it
              from the agent detail page if it leaks.
            </p>
          </div>
          <div className="form-actions">
            <Link className="button" to="/agents">
              Back to Agents
            </Link>
            <Link className="button primary" to={`/agents/${createdName}`}>
              Open agent
            </Link>
          </div>
        </Panel>
      </>
    );
  }

  return (
    <>
      <Breadcrumbs
        items={[
          { label: "Franz Console", to: "/" },
          { label: "Agents", to: "/agents" },
          { label: "Register Agent" },
        ]}
      />
      <PageHeading
        title="Register Agent"
        lead="Record an agent that will connect to the Franz fleet API. Franz never connects back."
      />
      <ErrorBanner error={createAgent.error} />
      <Panel title="Agent details" note="Registration records the agent in the control plane — nothing is provisioned here.">
        <form
          className="form-layout"
          onSubmit={(e) => {
            e.preventDefault();
            createAgent.mutate(
              { name: name.trim(), type: type as never, labels },
              {
                onSuccess: (res) => {
                  setToken(res.token ?? "");
                  setCreatedName(res.agent?.name ?? name.trim());
                },
              },
            );
          }}
        >
          <div className="form-section">
            <h3>Identity and type</h3>
            <div className="field">
              <label htmlFor="agent-name">
                Agent name <small>The control-plane identifier. Immutable.</small>
              </label>
              <div>
                <input
                  id="agent-name"
                  required
                  placeholder="local-docker"
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                />
              </div>
            </div>
            <div className="field">
              <label htmlFor="agent-type">Agent type</label>
              <div>
                <select id="agent-type" value={type} onChange={(e) => setType(e.target.value)}>
                  {AGENT_TYPES.map((t) => (
                    <option key={t.value} value={t.value}>
                      {t.label}
                    </option>
                  ))}
                </select>
                <p className="field-note">An organisational filter only. It does not change how the agent connects.</p>
              </div>
            </div>
          </div>
          <div className="form-section">
            <h3>Labels</h3>
            <p className="form-help">Optional free-form metadata — owner, team, environment.</p>
            <div className="field">
              <label>Labels</label>
              <div>
                <LabelEditor value={labels} onChange={setLabels} />
              </div>
            </div>
          </div>
          <div className="form-actions">
            <Link className="button" to="/agents">
              Cancel
            </Link>
            <button className="button primary" type="submit" disabled={createAgent.isPending}>
              {createAgent.isPending ? "Registering…" : "Register Agent"}
            </button>
          </div>
        </form>
      </Panel>
    </>
  );
}
