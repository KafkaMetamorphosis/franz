import { useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { Breadcrumbs, CopyButton, ErrorBanner, Loading, PageHeading, Panel, StatusPill } from "../../components/ui";
import { useAgent, useAgentLifecycle, useRotateAgentToken } from "../../api/hooks";
import { agentTypeLabel } from "../../api/enums";

export function AgentDetail() {
  const { name = "" } = useParams();
  const navigate = useNavigate();
  const { data, isLoading, error } = useAgent(name);
  const rotate = useRotateAgentToken(name);
  const { pause, resume, remove } = useAgentLifecycle(name);
  const [rotatedToken, setRotatedToken] = useState<string | null>(null);

  const agent = data?.agent;
  const deleted = agent?.status === "AGENT_STATUS_DELETED";

  return (
    <>
      <Breadcrumbs
        items={[
          { label: "Franz Console", to: "/" },
          { label: "Agents", to: "/agents" },
          { label: name },
        ]}
      />
      <PageHeading
        title={name}
        lead={agent ? `${agentTypeLabel(agent.type)} agent` : undefined}
        actions={
          agent && !deleted ? (
            <>
              {agent.status === "AGENT_STATUS_PAUSED" ? (
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
                  if (confirm(`Delete agent ${name}? This cannot be undone.`)) {
                    remove.mutate(undefined, { onSuccess: () => navigate("/agents") });
                  }
                }}
              >
                Delete
              </button>
            </>
          ) : null
        }
      />
      <ErrorBanner error={error ?? rotate.error ?? pause.error ?? resume.error ?? remove.error} />

      {isLoading ? (
        <Loading what="agent" />
      ) : agent ? (
        <>
          <Panel title="Overview">
            <dl className="detail-grid">
              <dt>FRN</dt>
              <dd>
                <code>{agent.frn}</code>
              </dd>
              <dt>Type</dt>
              <dd>{agentTypeLabel(agent.type)}</dd>
              <dt>Status</dt>
              <dd>
                <StatusPill value={agent.status} />
              </dd>
              <dt>Labels</dt>
              <dd>
                {Object.entries(agent.labels ?? {}).map(([k, v]) => (
                  <span className="tag" key={k}>
                    {k}={v}
                  </span>
                ))}
                {Object.keys(agent.labels ?? {}).length === 0 ? <span className="panel-note">none</span> : null}
              </dd>
              <dt>Registered</dt>
              <dd>{agent.createdAt ? new Date(agent.createdAt).toLocaleString() : "—"}</dd>
            </dl>
          </Panel>

          <Panel
            title="Bearer token"
            note="Rotating issues a new token and invalidates the current one immediately."
          >
            {rotatedToken ? (
              <div className="token-reveal">
                <code data-testid="rotated-token">{rotatedToken}</code>
                <CopyButton text={rotatedToken} label="Copy token" />
                <p className="field-note">Shown once. Update the agent&rsquo;s configuration with this value.</p>
              </div>
            ) : (
              <p className="panel-note">
                Franz stores only the hash of the token minted at registration. The plaintext is not
                recoverable.
              </p>
            )}
            <div className="form-actions">
              <button
                className="button"
                disabled={deleted || rotate.isPending}
                onClick={() =>
                  rotate.mutate(undefined, { onSuccess: (res) => setRotatedToken(res.token ?? "") })
                }
              >
                {rotate.isPending ? "Rotating…" : "Rotate token"}
              </button>
            </div>
          </Panel>
        </>
      ) : (
        <p className="empty-note">
          Agent not found. <Link to="/agents">Back to Agents</Link>
        </p>
      )}
    </>
  );
}
