import { useMemo, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { Breadcrumbs, Empty, ErrorBanner, Loading, PageHeading, Panel, StatusPill } from "../../components/ui";
import { useDeleteIndicator, useIndicator, useIndicatorSamples } from "../../api/hooks";
import { entityLabel, familyLabel } from "../../api/enums";
import { ApiError } from "../../api/client";
import { IndicatorVisualisation, RangeSelector } from "../../components/indicatorViz";
import { fromISOFor, pageSizeFor, type RangeKey } from "../../indicatorSamples";

export function IndicatorDetail() {
  const { name = "" } = useParams();
  const navigate = useNavigate();
  const [range, setRange] = useState<RangeKey>("1h");
  const { data, isLoading, error } = useIndicator(name);

  // `from` is pinned per range rather than recomputed every render, so the query
  // key is stable and the chart does not refetch on unrelated re-renders.
  const from = useMemo(() => fromISOFor(range), [range]);
  const { data: samplesData, isLoading: samplesLoading } = useIndicatorSamples(name, {
    from,
    pageSize: pageSizeFor(range),
  });
  const deleteIndicator = useDeleteIndicator();

  const indicator = data?.indicator;
  const samples = samplesData?.samples ?? [];

  // 003.8: DeleteIndicator's only failure modes are NotFound (404, the
  // indicator is already gone) and this FAILED_PRECONDITION (rendered as HTTP
  // 400 — grpc-gateway deliberately does not use 412 for it) when a Policy
  // still references it. A specific message, not a generic error, is the whole
  // point of surfacing this.
  const referencedByPolicy = deleteIndicator.error instanceof ApiError && deleteIndicator.error.status === 400;

  return (
    <>
      <Breadcrumbs
        items={[
          { label: "Franz Console", to: "/" },
          { label: "Indicators", to: "/governance/indicators" },
          { label: name },
        ]}
      />
      <PageHeading
        title={name}
        lead={indicator ? `Applies to ${entityLabel(indicator.appliesTo)}` : undefined}
        actions={
          indicator ? (
            <>
              <Link className="button" to={`/governance/indicators/${name}/edit`}>
                Edit
              </Link>
              <button
                className="button danger"
                onClick={() => {
                  if (confirm(`Delete Indicator ${name}?`)) {
                    deleteIndicator.mutate(name, { onSuccess: () => navigate("/governance/indicators") });
                  }
                }}
              >
                Delete
              </button>
            </>
          ) : null
        }
      />
      <ErrorBanner error={error} />
      {referencedByPolicy ? (
        <div className="app-error" role="alert">
          This indicator is still referenced by at least one Policy — delete or repoint those Policies
          first.
        </div>
      ) : (
        <ErrorBanner error={deleteIndicator.error} />
      )}

      {isLoading ? (
        <Loading what="indicator" />
      ) : !indicator ? (
        <Empty>
          Indicator not found. <Link to="/governance/indicators">Back to Indicators</Link>
        </Empty>
      ) : (
        <>
          <Panel title="Definition">
            <dl className="detail-grid">
              <dt>Unit</dt>
              <dd>
                <code>{indicator.unit}</code>
                {indicator.family ? (
                  <span className="panel-note"> — compared as {familyLabel(indicator.family)}</span>
                ) : null}
              </dd>
              <dt>Applies to</dt>
              <dd>{entityLabel(indicator.appliesTo)}</dd>
              <dt>Health</dt>
              <dd data-testid="indicator-health">
                <StatusPill value={indicator.health} />
              </dd>
              <dt>Last sample</dt>
              <dd>{indicator.lastSampleAt ? new Date(indicator.lastSampleAt).toLocaleString() : "—"}</dd>
              <dt>Staleness threshold</dt>
              <dd>{indicator.stalenessThreshold || "—"}</dd>
              <dt>Source agents</dt>
              <dd>
                {(indicator.sourceAgents ?? []).length === 0 ? (
                  <span className="panel-note">any agent</span>
                ) : (
                  indicator.sourceAgents!.map((agent) => (
                    <span className="tag" key={agent}>
                      {agent}
                    </span>
                  ))
                )}
              </dd>
            </dl>
          </Panel>

          <Panel
            title="History"
            note="One series per resource this indicator applies to, over the selected window."
            actions={<RangeSelector value={range} onChange={setRange} />}
          >
            {samplesLoading ? (
              <Loading what="samples" />
            ) : samples.length === 0 ? (
              <Empty>No samples in this window.</Empty>
            ) : (
              <IndicatorVisualisation
                samples={samples}
                family={indicator.family}
                range={range}
              />
            )}
          </Panel>

          <Panel
            title="Recent samples"
            note="Most recent samples across every resource this indicator applies to."
          >
            {samples.length === 0 ? (
              <Empty>No samples reported yet.</Empty>
            ) : (
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Resource</th>
                      <th>Value</th>
                      <th>Sampled at</th>
                    </tr>
                  </thead>
                  <tbody>
                    {samples.map((sample, i) => (
                      <tr key={`${sample.resourceFrn}-${sample.sampleAt}-${i}`}>
                        <td>
                          <code>{sample.resourceFrn}</code>
                        </td>
                        <td>{sample.value}</td>
                        <td>{sample.sampleAt ? new Date(sample.sampleAt).toLocaleString() : "—"}</td>
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
