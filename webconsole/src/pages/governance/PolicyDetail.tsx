import { Link, useNavigate, useParams } from "react-router-dom";
import { Breadcrumbs, Empty, ErrorBanner, Loading, PageHeading, Panel } from "../../components/ui";
import { describeAction, entityLabel, operatorLabel } from "../../api/enums";
import {
  updateMask,
  useDeletePolicy,
  useDryRunPolicy,
  usePolicy,
  usePolicyActions,
  useUpdatePolicy,
} from "../../api/hooks";

// PolicyDetail's Dry-run panel calls DryRunPolicy with the policy's own saved
// definition — the RPC has no separate "hypothetical value" input; it
// evaluates against the latest real sample per matched resource and mutates
// nothing (003.8).
export function PolicyDetail() {
  const { name = "" } = useParams();
  const navigate = useNavigate();
  const { data, isLoading, error } = usePolicy(name);
  const { data: actionsData } = usePolicyActions(name);
  const update = useUpdatePolicy(name);
  const deletePolicy = useDeletePolicy();
  const dryRun = useDryRunPolicy();

  const policy = data?.policy;
  const recentActions = actionsData?.actions ?? [];

  return (
    <>
      <Breadcrumbs
        items={[
          { label: "Franz Console", to: "/" },
          { label: "Policies", to: "/governance/policies" },
          { label: name },
        ]}
      />
      <PageHeading
        title={name}
        lead={policy ? `Watches ${policy.indicator}` : undefined}
        actions={
          policy ? (
            <>
              <Link className="button" to={`/governance/policies/${name}/edit`}>
                Edit
              </Link>
              <button
                className="button"
                onClick={() =>
                  update.mutate({ updateMask: updateMask(["enabled"]), enabled: !policy.enabled })
                }
                disabled={update.isPending}
              >
                {policy.enabled ? "Disable" : "Enable"}
              </button>
              <button
                className="button danger"
                onClick={() => {
                  if (confirm(`Delete Policy ${name}?`)) {
                    deletePolicy.mutate(name, { onSuccess: () => navigate("/governance/policies") });
                  }
                }}
              >
                Delete
              </button>
            </>
          ) : null
        }
      />
      <ErrorBanner error={error ?? update.error ?? deletePolicy.error} />

      {isLoading ? (
        <Loading what="policy" />
      ) : !policy ? (
        <Empty>
          Policy not found. <Link to="/governance/policies">Back to Policies</Link>
        </Empty>
      ) : (
        <>
          <Panel title="Definition">
            <dl className="detail-grid">
              <dt>Indicator</dt>
              <dd>
                <Link to={`/governance/indicators/${policy.indicator}`}>{policy.indicator}</Link>
              </dd>
              <dt>Matcher</dt>
              <dd>
                {entityLabel(policy.matcher?.entity)}
                {policy.matcher?.selector ? (
                  <>
                    {" "}
                    <code>{policy.matcher.selector}</code>
                  </>
                ) : (
                  <span className="panel-note"> (every resource)</span>
                )}
              </dd>
              <dt>Limit</dt>
              <dd>
                {operatorLabel(policy.limit?.operator)} {policy.limit?.value}
              </dd>
              <dt>Actions</dt>
              <dd>
                <ul>
                  {(policy.actions ?? []).map((action, i) => (
                    <li key={i}>{describeAction(action)}</li>
                  ))}
                </ul>
              </dd>
              <dt>Weight</dt>
              <dd>{policy.weight ?? 0}</dd>
              <dt>Enabled</dt>
              <dd data-testid="policy-enabled">{policy.enabled ? "Yes" : "No"}</dd>
              <dt>Last fired</dt>
              <dd>{policy.lastFiredAt ? new Date(policy.lastFiredAt).toLocaleString() : "never"}</dd>
              <dt>FRN</dt>
              <dd>
                <code>{policy.frn}</code>
              </dd>
            </dl>
          </Panel>

          <Panel
            title="Dry run"
            note="Evaluates this policy's saved definition against the latest real sample per matched resource. Nothing is applied."
          >
            <button
              className="button"
              onClick={() =>
                dryRun.mutate({
                  indicator: policy.indicator,
                  matcher: policy.matcher,
                  limit: policy.limit,
                  actions: policy.actions,
                })
              }
              disabled={dryRun.isPending}
            >
              {dryRun.isPending ? "Running…" : "Dry run"}
            </button>
            <ErrorBanner error={dryRun.error} />
            {dryRun.data ? (
              <>
                <p className="panel-note" data-testid="dry-run-not-applied">
                  Not applied — dry run only.
                </p>
                {(dryRun.data.matches ?? []).length === 0 ? (
                  <Empty>No resources currently match this policy's matcher.</Empty>
                ) : (
                  <div className="table-wrap">
                    <table>
                      <thead>
                        <tr>
                          <th>Resource</th>
                          <th>Indicator value</th>
                          <th>Would trigger</th>
                        </tr>
                      </thead>
                      <tbody>
                        {(dryRun.data.matches ?? []).map((match, i) => (
                          <tr key={i}>
                            <td>
                              <code>{match.resourceFrn}</code>
                            </td>
                            <td>{match.indicatorValue}</td>
                            <td>{match.wouldTrigger ? "Yes" : "No"}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </>
            ) : null}
          </Panel>

          <Panel title="Recent policy actions" note="Audit trail — one row per automated change this policy made.">
            {recentActions.length === 0 ? (
              <Empty>No actions fired yet.</Empty>
            ) : (
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>When</th>
                      <th>Resource</th>
                      <th>Indicator value</th>
                      <th>Action</th>
                      <th>Result</th>
                    </tr>
                  </thead>
                  <tbody>
                    {recentActions.map((entry, i) => (
                      <tr key={i}>
                        <td>{entry.occurredAt ? new Date(entry.occurredAt).toLocaleString() : "—"}</td>
                        <td>
                          <code>{entry.resourceFrn}</code>
                        </td>
                        <td>{entry.indicatorValue}</td>
                        <td>{describeAction(entry.action)}</td>
                        <td>{entry.result}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </Panel>
        </>
      )}
    </>
  );
}
