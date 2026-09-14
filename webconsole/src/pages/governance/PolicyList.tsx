import { Link } from "react-router-dom";
import { Breadcrumbs, Empty, ErrorBanner, Loading, PageHeading } from "../../components/ui";
import { usePolicies } from "../../api/hooks";
import { entityLabel, operatorLabel } from "../../api/enums";

export function PolicyList() {
  const { data, isLoading, error } = usePolicies();
  const policies = data?.policies ?? [];

  return (
    <>
      <Breadcrumbs items={[{ label: "Franz Console", to: "/" }, { label: "Policies" }]} />
      <PageHeading
        title="Policies"
        lead="Reactive rules: when an Indicator crosses a limit, a Policy's whitelisted actions run on every matched resource."
        actions={
          <Link className="button primary" to="/governance/policies/register">
            Register Policy
          </Link>
        }
      />
      <ErrorBanner error={error} />
      <section className="panel">
        <div className="toolbar">
          <span className="panel-note">
            {policies.length} polic{policies.length === 1 ? "y" : "ies"}
          </span>
        </div>
        {isLoading ? (
          <Loading what="policies" />
        ) : policies.length === 0 ? (
          <Empty>
            No Policies yet. <Link to="/governance/policies/register">Register one</Link>.
          </Empty>
        ) : (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Policy</th>
                  <th>Indicator</th>
                  <th>Matcher</th>
                  <th>Limit</th>
                  <th>Actions</th>
                  <th>Weight</th>
                  <th>Enabled</th>
                  <th>Last fired</th>
                </tr>
              </thead>
              <tbody>
                {policies.map((policy) => (
                  <tr key={policy.name}>
                    <td>
                      <Link to={`/governance/policies/${policy.name}`}>{policy.name}</Link>
                    </td>
                    <td>
                      <code>{policy.indicator}</code>
                    </td>
                    <td>
                      {entityLabel(policy.matcher?.entity)}
                      {policy.matcher?.selector ? (
                        <>
                          {" "}
                          <code>{policy.matcher.selector}</code>
                        </>
                      ) : (
                        <span className="panel-note"> (every resource)</span>
                      )}
                    </td>
                    <td>
                      {operatorLabel(policy.limit?.operator)} {policy.limit?.value}
                    </td>
                    <td>{policy.actions?.length ?? 0}</td>
                    <td>{policy.weight ?? 0}</td>
                    <td>{policy.enabled ? "Yes" : "No"}</td>
                    <td>{policy.lastFiredAt ? new Date(policy.lastFiredAt).toLocaleString() : "—"}</td>
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
