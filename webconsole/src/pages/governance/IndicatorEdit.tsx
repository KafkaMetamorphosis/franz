import { useMemo, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { Breadcrumbs, ErrorBanner, Loading, PageHeading, Panel } from "../../components/ui";
import { ApiError } from "../../api/client";
import { updateMask, useIndicator, useUpdateIndicator } from "../../api/hooks";
import { entityLabel } from "../../api/enums";

// Only unit / staleness_threshold / source_agents are maskable — applies_to is
// immutable (003.14) and never appears in the mask.
export function IndicatorEdit() {
  const { name = "" } = useParams();
  const navigate = useNavigate();
  const { data, isLoading, error } = useIndicator(name);
  const update = useUpdateIndicator(name);
  const indicator = data?.indicator;

  const baseUnit = indicator?.unit ?? "";
  const baseStaleness = indicator?.stalenessThreshold ?? "";
  const baseAgents = useMemo(() => (indicator?.sourceAgents ?? []).join(", "), [indicator]);

  const [unit, setUnit] = useState<string | null>(null);
  const [staleness, setStaleness] = useState<string | null>(null);
  const [agentsText, setAgentsText] = useState<string | null>(null);

  if (isLoading) return <Loading what="indicator" />;
  if (error || !indicator) {
    return (
      <>
        <ErrorBanner error={error} />
        <p className="empty-note">
          Indicator not found. <Link to="/governance/indicators">Back to Indicators</Link>
        </p>
      </>
    );
  }

  const currentUnit = unit ?? baseUnit;
  const currentStaleness = staleness ?? baseStaleness;
  const currentAgentsText = agentsText ?? baseAgents;
  const changed =
    currentUnit !== baseUnit || currentStaleness !== baseStaleness || currentAgentsText !== baseAgents;
  const conflict = update.error instanceof ApiError && update.error.status === 409;

  return (
    <>
      <Breadcrumbs
        items={[
          { label: "Franz Console", to: "/" },
          { label: "Indicators", to: "/governance/indicators" },
          { label: name, to: `/governance/indicators/${name}` },
          { label: "Edit" },
        ]}
      />
      <PageHeading title={`Edit ${name}`} lead={`Applies to ${entityLabel(indicator.appliesTo)}`} />
      <ErrorBanner error={update.error} />
      {conflict ? (
        <div className="app-error" role="alert">
          The indicator changed since you opened this form.{" "}
          <button
            type="button"
            className="button"
            onClick={() => {
              setUnit(null);
              setStaleness(null);
              setAgentsText(null);
              update.reset();
            }}
          >
            Reload and re-apply
          </button>
        </div>
      ) : null}

      <Panel title="Editable fields" note="Unit, staleness threshold, and source agents. Applies-to is immutable.">
        <form
          className="form-layout"
          onSubmit={(event) => {
            event.preventDefault();
            const paths: string[] = [];
            if (currentUnit !== baseUnit) paths.push("unit");
            if (currentStaleness !== baseStaleness) paths.push("staleness_threshold");
            const sourceAgents = currentAgentsText
              .split(",")
              .map((a) => a.trim())
              .filter(Boolean);
            if (currentAgentsText !== baseAgents) paths.push("source_agents");
            update.mutate(
              {
                updateMask: updateMask(paths),
                unit: currentUnit,
                stalenessThreshold: currentStaleness,
                sourceAgents,
              },
              { onSuccess: () => navigate(`/governance/indicators/${name}`) },
            );
          }}
        >
          <div className="form-section">
            <h3>Immutable</h3>
            <dl className="detail-grid">
              <dt>Applies to</dt>
              <dd>{entityLabel(indicator.appliesTo)}</dd>
            </dl>
          </div>

          <div className="form-section">
            <h3>Definition</h3>
            <div className="field">
              <label htmlFor="indicator-unit">Unit</label>
              <div>
                <input
                  id="indicator-unit"
                  required
                  value={currentUnit}
                  onChange={(event) => setUnit(event.target.value)}
                />
              </div>
            </div>
            <div className="field">
              <label htmlFor="indicator-staleness">Staleness threshold</label>
              <div>
                <input
                  id="indicator-staleness"
                  required
                  value={currentStaleness}
                  onChange={(event) => setStaleness(event.target.value)}
                />
              </div>
            </div>
            <div className="field">
              <label htmlFor="indicator-agents">
                Source agents <small>Comma-separated. Empty means any agent.</small>
              </label>
              <div>
                <input
                  id="indicator-agents"
                  value={currentAgentsText}
                  onChange={(event) => setAgentsText(event.target.value)}
                />
              </div>
            </div>
          </div>

          <div className="form-actions">
            <Link className="button" to={`/governance/indicators/${name}`}>
              Cancel
            </Link>
            <button className="button primary" type="submit" disabled={!changed || update.isPending}>
              {update.isPending ? "Saving…" : "Save changes"}
            </button>
          </div>
        </form>
      </Panel>
    </>
  );
}
