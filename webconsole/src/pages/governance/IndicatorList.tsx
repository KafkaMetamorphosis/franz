import { Link } from "react-router-dom";
import { Breadcrumbs, Empty, ErrorBanner, Loading, PageHeading, StatusPill } from "../../components/ui";
import { useIndicators } from "../../api/hooks";
import { entityLabel } from "../../api/enums";

export function IndicatorList() {
  const { data, isLoading, error } = useIndicators();
  const indicators = data?.indicators ?? [];

  return (
    <>
      <Breadcrumbs items={[{ label: "Franz Console", to: "/" }, { label: "Indicators" }]} />
      <PageHeading
        title="Indicators"
        lead="Metrics Telemetry Agents publish samples for. A Policy watches one indicator and acts when a limit is crossed."
        actions={
          <Link className="button primary" to="/governance/indicators/register">
            Register Indicator
          </Link>
        }
      />
      <ErrorBanner error={error} />
      <section className="panel">
        <div className="toolbar">
          <span className="panel-note">
            {indicators.length} indicator{indicators.length === 1 ? "" : "s"}
          </span>
        </div>
        {isLoading ? (
          <Loading what="indicators" />
        ) : indicators.length === 0 ? (
          <Empty>
            No Indicators yet. <Link to="/governance/indicators/register">Register one</Link>.
          </Empty>
        ) : (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Indicator</th>
                  <th>Unit</th>
                  <th>Applies to</th>
                  <th>Health</th>
                  <th>Last sample</th>
                </tr>
              </thead>
              <tbody>
                {indicators.map((indicator) => (
                  <tr key={indicator.name}>
                    <td>
                      <Link to={`/governance/indicators/${indicator.name}`}>{indicator.name}</Link>
                    </td>
                    <td>
                      <code>{indicator.unit}</code>
                    </td>
                    <td>{entityLabel(indicator.appliesTo)}</td>
                    <td>
                      <StatusPill value={indicator.health} />
                    </td>
                    <td>
                      {indicator.lastSampleAt ? new Date(indicator.lastSampleAt).toLocaleString() : "—"}
                    </td>
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
